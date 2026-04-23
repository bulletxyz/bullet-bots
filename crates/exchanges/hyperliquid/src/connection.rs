//! `HyperliquidConnection` — sets up authenticated REST + streaming clients,
//! subscribes to the required WS feeds, and demultiplexes `Message`s into
//! typed per-event channels.
//!
//! Canonical sources:
//!   - `Message::UserFills` → `Trade` (one per execution, authoritative source
//!     of position changes).
//!   - `Message::OrderUpdates` → `OrderLifecycle` (status transitions, used
//!     for reconcile and client_id → oid resolution).
//!   - `Message::L2Book` → `BookUpdate`.
//!   - `Message::ActiveAssetCtx` → `MarkPriceUpdate` (carries both mark_px
//!     and funding — fixes the longstanding "funding is always zero" gap).
//!   - `Message::AllMids` → `MarkPriceUpdate` (fallback with funding_rate=0
//!     until the per-coin `ActiveAssetCtx` arrives).

use std::str::FromStr;

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, MarkPriceUpdate, OrderLifecycle, Trade};
use bb_core::harness::{EventFeed, EventTx, FeedContext};
use bb_core::types::{Order, OrderStatus, OrderType, Side};
use ethers::signers::{LocalWallet, Signer};
use hyperliquid_rust_sdk::{
    AssetCtx, BaseUrl, ExchangeClient, InfoClient, Message, Subscription,
};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::broker::HyperliquidBroker;
use crate::config::HyperliquidConfig;
use crate::convert;

const EXCHANGE: &str = "hyperliquid";

pub struct HyperliquidFeeds {
    pub trade: HyperliquidTradeFeed,
    pub book: HyperliquidBookFeed,
    pub lifecycle: HyperliquidOrderLifecycleFeed,
    pub mark_price: HyperliquidMarkPriceFeed,
}

/// Connect to Hyperliquid and return the REST broker plus typed feeds for
/// the harness to wire up. `symbol` is in bb format (e.g. `"BTC-USD"`).
pub async fn connect(
    config: &HyperliquidConfig,
    symbol: &str,
) -> Result<(HyperliquidBroker, HyperliquidFeeds), BotError> {
    let key_hex =
        config.private_key_hex.strip_prefix("0x").unwrap_or(&config.private_key_hex);
    let wallet: LocalWallet = key_hex
        .parse()
        .map_err(|e| BotError::config(format!("Invalid HL private key: {e}")))?;
    let address = wallet.address();
    let base_url = match config.network.as_str() {
        "mainnet" => BaseUrl::Mainnet,
        _ => BaseUrl::Testnet,
    };

    let exchange_client =
        ExchangeClient::new(None, wallet.clone(), Some(base_url), None, None)
            .await
            .map_err(|e| BotError::exchange(e, true))?;
    let info = InfoClient::new(None, Some(base_url))
        .await
        .map_err(|e| BotError::exchange(e, true))?;

    // Separate InfoClient for WS (needs `with_reconnect` and stays alive in the
    // muxer task). The REST `info` above is kept on the broker for queries.
    let mut ws_info = InfoClient::with_reconnect(None, Some(base_url))
        .await
        .map_err(|e| BotError::exchange(e, true))?;

    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<Message>();
    let coin = convert::to_hl_coin(symbol);

    for (label, sub) in [
        ("L2Book", Subscription::L2Book { coin: coin.clone() }),
        ("OrderUpdates", Subscription::OrderUpdates { user: address }),
        ("UserFills", Subscription::UserFills { user: address }),
        ("AllMids", Subscription::AllMids),
        ("ActiveAssetCtx", Subscription::ActiveAssetCtx { coin: coin.clone() }),
    ] {
        ws_info
            .subscribe(sub, ws_tx.clone())
            .await
            .map_err(|e| BotError::exchange(format!("HL subscribe {label}: {e}"), false))?;
    }
    tracing::info!(symbol, coin = %coin, address = %format!("{address:?}"), "Hyperliquid: subscribed");

    let (trade_tx, trade_rx) = mpsc::unbounded_channel::<Trade>();
    let (book_tx, book_rx) = mpsc::unbounded_channel::<BookUpdate>();
    let (life_tx, life_rx) = mpsc::unbounded_channel::<OrderLifecycle>();
    let (mark_tx, mark_rx) = mpsc::unbounded_channel::<MarkPriceUpdate>();

    let target_coin = coin.clone();
    tokio::spawn(async move {
        let _ws_info = ws_info; // keep WS alive
        while let Some(msg) = ws_rx.recv().await {
            match msg {
                Message::L2Book(b) if b.data.coin == target_coin => {
                    let _ = book_tx.send(l2_book_to_event(&b.data));
                }
                Message::OrderUpdates(u) => {
                    for update in u.data.iter().filter(|u| u.order.coin == target_coin) {
                        let _ = life_tx.send(order_update_to_lifecycle(update));
                    }
                }
                Message::UserFills(f) => {
                    for fill in f.data.fills.iter().filter(|f| f.coin == target_coin) {
                        let _ = trade_tx.send(fill_to_trade(fill));
                    }
                }
                Message::AllMids(m) => {
                    if let Some(mid_str) = m.data.mids.get(&target_coin) {
                        let _ = mark_tx.send(MarkPriceUpdate {
                            exchange: EXCHANGE.into(),
                            symbol: convert::to_bb_symbol(&target_coin),
                            mark_price: mid_str.parse().unwrap_or(Decimal::ZERO),
                            funding_rate: Decimal::ZERO,
                        });
                    }
                }
                Message::ActiveAssetCtx(ctx) if ctx.data.coin == target_coin => {
                    if let Some(event) = active_asset_ctx_to_mark(&ctx.data) {
                        let _ = mark_tx.send(event);
                    }
                }
                _ => {}
            }
        }
        tracing::warn!("Hyperliquid: WS muxer ended");
    });

    let broker = HyperliquidBroker::new(exchange_client, info, wallet, address);
    let feeds = HyperliquidFeeds {
        trade: HyperliquidTradeFeed { rx: trade_rx },
        book: HyperliquidBookFeed { rx: book_rx },
        lifecycle: HyperliquidOrderLifecycleFeed { rx: life_rx },
        mark_price: HyperliquidMarkPriceFeed { rx: mark_rx },
    };
    Ok((broker, feeds))
}

// -- Typed feeds ------------------------------------------------------------

macro_rules! feed_impl {
    ($ty:ident, $event:ty) => {
        pub struct $ty {
            rx: mpsc::UnboundedReceiver<$event>,
        }

        #[async_trait]
        impl EventFeed<$event> for $ty {
            async fn run(
                self: Box<Self>,
                tx: EventTx<$event>,
                cx: FeedContext,
            ) -> Result<(), BotError> {
                let mut this = *self;
                loop {
                    tokio::select! {
                        biased;
                        _ = cx.cancelled() => return Ok(()),
                        maybe = this.rx.recv() => match maybe {
                            Some(event) => { let _ = tx.send(event); }
                            None => return Ok(()),
                        }
                    }
                }
            }
        }
    };
}

feed_impl!(HyperliquidTradeFeed, Trade);
feed_impl!(HyperliquidBookFeed, BookUpdate);
feed_impl!(HyperliquidOrderLifecycleFeed, OrderLifecycle);
feed_impl!(HyperliquidMarkPriceFeed, MarkPriceUpdate);

// -- Message → event conversions -------------------------------------------

fn l2_book_to_event(data: &hyperliquid_rust_sdk::L2BookData) -> BookUpdate {
    use std::collections::BTreeMap;
    let bids: BTreeMap<Decimal, Decimal> = data
        .levels
        .first()
        .map(|levels| levels.iter().map(|l| (convert::parse_dec(&l.px), convert::parse_dec(&l.sz))).collect())
        .unwrap_or_default();
    let asks: BTreeMap<Decimal, Decimal> = data
        .levels
        .get(1)
        .map(|levels| levels.iter().map(|l| (convert::parse_dec(&l.px), convert::parse_dec(&l.sz))).collect())
        .unwrap_or_default();
    BookUpdate {
        exchange: EXCHANGE.into(),
        symbol: convert::to_bb_symbol(&data.coin),
        orderbook: bb_core::types::OrderBook { bids, asks, last_update_id: data.time },
    }
}

fn fill_to_trade(fill: &hyperliquid_rust_sdk::TradeInfo) -> Trade {
    let side = match fill.side.as_str() {
        "B" | "Buy" | "buy" => Side::Buy,
        _ => Side::Sell,
    };
    Trade {
        exchange: EXCHANGE.into(),
        symbol: convert::to_bb_symbol(&fill.coin),
        order_id: fill.oid.to_string(),
        client_id: fill.cloid.as_ref().map(|c| c.to_string()),
        side,
        price: convert::parse_dec(&fill.px),
        quantity: convert::parse_dec(&fill.sz),
    }
}

fn order_update_to_lifecycle(update: &hyperliquid_rust_sdk::OrderUpdate) -> OrderLifecycle {
    let order = &update.order;
    let side = match order.side.as_str() {
        "B" | "Buy" | "buy" => Side::Buy,
        _ => Side::Sell,
    };
    let status = match update.status.as_str() {
        "open" | "new" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "partiallyFilled" => OrderStatus::PartiallyFilled,
        _ => OrderStatus::Open,
    };
    let orig = convert::parse_dec(&order.orig_sz);
    let remaining = convert::parse_dec(&order.sz);
    let filled = (orig - remaining).max(Decimal::ZERO);

    OrderLifecycle {
        exchange: EXCHANGE.into(),
        order: Order {
            id: order.oid.to_string(),
            client_id: order.cloid.clone(),
            symbol: convert::to_bb_symbol(&order.coin),
            side,
            order_type: OrderType::Limit,
            price: convert::parse_dec(&order.limit_px),
            quantity: orig,
            filled_quantity: filled,
            status,
        },
    }
}

fn active_asset_ctx_to_mark(
    data: &hyperliquid_rust_sdk::ActiveAssetCtxData,
) -> Option<MarkPriceUpdate> {
    let (mark, funding) = match &data.ctx {
        AssetCtx::Perps(p) => (
            Decimal::from_str(&p.shared.mark_px).ok()?,
            Decimal::from_str(&p.funding).unwrap_or(Decimal::ZERO),
        ),
        AssetCtx::Spot(s) => (Decimal::from_str(&s.shared.mark_px).ok()?, Decimal::ZERO),
    };
    Some(MarkPriceUpdate {
        exchange: EXCHANGE.into(),
        symbol: convert::to_bb_symbol(&data.coin),
        mark_price: mark,
        funding_rate: funding,
    })
}
