//! `BulletConnection` — owns a single `ManagedWebsocket` and demultiplexes
//! `ServerMessage`s into typed per-event-kind channels.
//!
//! Each channel backs one `EventFeed` implementation: `BulletTradeFeed`,
//! `BulletBookFeed`, `BulletOrderLifecycleFeed`, `BulletMarkPriceFeed`. The
//! muxer task is spawned at connection time and runs until the `ManagedWebsocket`
//! returns a terminal `Disconnected`.
//!
//! Canonical source split — important for correctness:
//!   - `OrderUpdateData::TradeFill` emits BOTH a `Trade` (for inventory) and
//!     an `OrderLifecycle` (for reconcile). They are independent events so
//!     strategies that only handle `Trade` won't miss position updates, and
//!     strategies that only handle `OrderLifecycle` won't miss a transition.
//!   - `OrderUpdateData::PlaceOrder` / `Cancel` emit only `OrderLifecycle` —
//!     they carry no execution, so there's no `Trade` to emit.

use std::str::FromStr;
use std::sync::Arc;

use bb_core::error::BotError;
use bb_core::events::{BookUpdate, MarkPriceUpdate, OrderLifecycle, Trade};
use bb_core::types::{Order, OrderBook, OrderStatus, OrderType, Side};
use bullet_rust_sdk::types::{
    BookTickerMessage, DepthUpdate, MarkPriceMessage, OrderUpdateData, OrderUpdateMessage,
};
use bullet_rust_sdk::ws::models::ServerMessage;
use bullet_rust_sdk::{
    Client, Keypair, ManagedWebsocket, Network, OrderbookDepth, Topic, UserActionDiscriminants,
    WsEvent,
};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::broker::{BulletBroker, load_increments};
use crate::config::BulletConfig;
use crate::convert;

const EXCHANGE: &str = "bullet";

pub struct BulletFeeds {
    pub trade: BulletTradeFeed,
    pub book: BulletBookFeed,
    pub lifecycle: BulletOrderLifecycleFeed,
    pub mark_price: BulletMarkPriceFeed,
}

/// Connect to Bullet and set up the demux task. Returns the REST broker and a
/// bundle of typed feeds to wire into the harness.
pub async fn connect(
    config: &BulletConfig,
    symbol: &str,
) -> Result<(BulletBroker, BulletFeeds), BotError> {
    let keypair = if let Some(path) = config.key_file.as_deref() {
        Keypair::read_from_file(path).map_err(|e| {
            BotError::config(format!("Failed to load keystore {}: {e}", path.display()))
        })?
    } else {
        let hex = secrecy::ExposeSecret::expose_secret(&config.private_key_hex);
        if hex.is_empty() {
            return Err(BotError::config(
                "Bullet: no key material — set [exchanges.bullet].key_file, \
                 BB_BULLET_KEY_FILE, private_key_hex, or BB_BULLET_PRIVATE_KEY_HEX"
                    .to_string(),
            ));
        }
        Keypair::from_hex(hex).map_err(|e| BotError::config(format!("Invalid private key: {e}")))?
    };
    let network = match config.network.as_str() {
        "mainnet" => Network::Mainnet,
        "testnet" => Network::Testnet,
        other => Network::from(other),
    };

    let client = Client::builder()
        .network(network)
        .keypair(keypair)
        .user_actions(vec![
            UserActionDiscriminants::PlaceOrders,
            UserActionDiscriminants::CancelOrders,
            UserActionDiscriminants::CancelMarketOrders,
        ])
        .build()
        .await
        .map_err(|e| BotError::exchange(e, true))?;

    let address = client.address().map_err(|e| BotError::exchange(e, false))?;
    let increments = load_increments(&client).await?;
    let client = Arc::new(client);

    // Set up WS + subscriptions.
    let ws = client
        .connect_ws_managed()
        .call()
        .await
        .map_err(|e| BotError::exchange(e, true))?;
    ws.subscribe(
        [
            Topic::depth(symbol, OrderbookDepth::D20),
            Topic::book_ticker(symbol),
            Topic::mark_price(symbol),
        ],
        None,
    )
    .map_err(|e| BotError::exchange(e, false))?;
    // Bullet user-order stream: address-prefixed raw topic (no listenKey flow).
    let user_topic = format!("{address}@user.orders");
    ws.subscribe_raw([user_topic.clone()], None)
        .map_err(|e| BotError::exchange(e, false))?;

    tracing::info!(
        symbol,
        address = %address,
        user_topic,
        symbols_known = client.symbols().len(),
        "Bullet: connected + subscribed"
    );

    // Typed channels (one per feed).
    let (trade_tx, trade_rx) = mpsc::unbounded_channel::<Trade>();
    let (book_tx, book_rx) = mpsc::unbounded_channel::<BookUpdate>();
    let (life_tx, life_rx) = mpsc::unbounded_channel::<OrderLifecycle>();
    let (mark_tx, mark_rx) = mpsc::unbounded_channel::<MarkPriceUpdate>();

    // Muxer task — reads WS, classifies, forwards. Holds `ws` so the connection
    // stays alive for the lifetime of the task.
    tokio::spawn(muxer_loop(ws, trade_tx, book_tx, life_tx, mark_tx));

    let broker = BulletBroker::new(Arc::clone(&client), increments);
    let feeds = BulletFeeds {
        trade: BulletTradeFeed { rx: trade_rx },
        book: BulletBookFeed { rx: book_rx },
        lifecycle: BulletOrderLifecycleFeed { rx: life_rx },
        mark_price: BulletMarkPriceFeed { rx: mark_rx },
    };
    Ok((broker, feeds))
}

async fn muxer_loop(
    mut ws: ManagedWebsocket,
    trade_tx: mpsc::UnboundedSender<Trade>,
    book_tx: mpsc::UnboundedSender<BookUpdate>,
    life_tx: mpsc::UnboundedSender<OrderLifecycle>,
    mark_tx: mpsc::UnboundedSender<MarkPriceUpdate>,
) {
    loop {
        let Some(ws_event) = ws.recv().await else {
            tracing::warn!("Bullet: managed WS ended");
            break;
        };
        match ws_event {
            WsEvent::Message(msg) => match *msg {
                ServerMessage::DepthUpdate(ref depth) => {
                    let _ = book_tx.send(depth_to_event(depth));
                }
                ServerMessage::BookTicker(ref bt) => {
                    let _ = book_tx.send(book_ticker_to_event(bt));
                }
                ServerMessage::MarkPrice(ref mp) => {
                    let _ = mark_tx.send(mark_price_to_event(mp));
                }
                ServerMessage::OrderUpdate(ref update) => {
                    // Emit Trade and/or OrderLifecycle depending on variant.
                    if let Some(trade) = order_update_to_trade(update) {
                        let _ = trade_tx.send(trade);
                    }
                    let _ = life_tx.send(order_update_to_lifecycle(update));
                }
                // AggTrade intentionally not forwarded — the `OrderUpdate`
                // TradeFill variant is the authoritative fill source for our
                // account. Emitting from both would double-count.
                _ => continue,
            },
            WsEvent::Reconnecting => {
                tracing::info!("Bullet: WebSocket reconnecting (managed)");
            }
            WsEvent::Disconnected(reason) => {
                tracing::error!(%reason, "Bullet: permanently disconnected");
                break;
            }
        }
    }
}

// -- Typed feeds ------------------------------------------------------------

use async_trait::async_trait;
use bb_core::harness::{EventFeed, EventTx, FeedContext};

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

feed_impl!(BulletTradeFeed, Trade);
feed_impl!(BulletBookFeed, BookUpdate);
feed_impl!(BulletOrderLifecycleFeed, OrderLifecycle);
feed_impl!(BulletMarkPriceFeed, MarkPriceUpdate);

// -- Message → event conversions -------------------------------------------

fn depth_to_event(depth: &DepthUpdate) -> BookUpdate {
    let bids = depth.bids.iter().filter_map(convert::parse_level_tuple).collect();
    let asks = depth.asks.iter().filter_map(convert::parse_level_tuple).collect();
    BookUpdate {
        exchange: EXCHANGE.into(),
        symbol: depth.symbol.clone(),
        orderbook: OrderBook { bids, asks, last_update_id: depth.last_update_id },
    }
}

fn book_ticker_to_event(bt: &BookTickerMessage) -> BookUpdate {
    use std::collections::BTreeMap;
    let mut bids = BTreeMap::new();
    let mut asks = BTreeMap::new();
    if let (Ok(price), Ok(qty)) =
        (Decimal::from_str(&bt.best_bid_price), Decimal::from_str(&bt.best_bid_qty))
    {
        bids.insert(price, qty);
    }
    if let (Ok(price), Ok(qty)) =
        (Decimal::from_str(&bt.best_ask_price), Decimal::from_str(&bt.best_ask_qty))
    {
        asks.insert(price, qty);
    }
    BookUpdate {
        exchange: EXCHANGE.into(),
        symbol: bt.symbol.clone(),
        orderbook: OrderBook { bids, asks, last_update_id: bt.update_id },
    }
}

fn mark_price_to_event(mp: &MarkPriceMessage) -> MarkPriceUpdate {
    MarkPriceUpdate {
        exchange: EXCHANGE.into(),
        symbol: mp.symbol.clone(),
        mark_price: Decimal::from_str(&mp.mark_price).unwrap_or_default(),
        funding_rate: Decimal::from_str(&mp.funding_rate).unwrap_or_default(),
    }
}

/// Extract a `Trade` from `OrderUpdateData::TradeFill`. Returns `None` for
/// PlaceOrder / Cancel variants — they carry no execution.
fn order_update_to_trade(msg: &OrderUpdateMessage) -> Option<Trade> {
    let OrderUpdateData::TradeFill(data) = &msg.order else { return None };
    let side = match data.side.as_str() {
        "BUY" => Side::Buy,
        _ => Side::Sell,
    };
    Some(Trade {
        exchange: EXCHANGE.into(),
        symbol: data.common.symbol.clone(),
        order_id: data.common.order_id.to_string(),
        client_id: data.common.client_order_id.as_ref().map(|c| c.to_string()),
        side,
        price: data.last_filled_price.parse().unwrap_or_default(),
        quantity: data.last_filled_qty.parse().unwrap_or_default(),
    })
}

/// Extract an `OrderLifecycle` update from any `OrderUpdate` variant.
///
/// `price` / `quantity` on the resulting `Order` always carry the *original*
/// order's limit price and size — never per-fill execution values. For a
/// `TradeFill` the venue often omits those fields (they're `Option<String>`
/// in the SDK), in which case we fall back to zero and rely on
/// `filled_quantity` as the authoritative "how much got filled" number.
fn order_update_to_lifecycle(msg: &OrderUpdateMessage) -> OrderLifecycle {
    let (symbol, order_id, client_id, status_str, side_str, price, quantity, filled_quantity) =
        match &msg.order {
            OrderUpdateData::TradeFill(data) => (
                data.common.symbol.clone(),
                data.common.order_id.to_string(),
                data.common.client_order_id.as_ref().map(|c| c.to_string()),
                data.common.status.clone(),
                data.side.clone(),
                data.price.as_deref().and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO),
                data.quantity.as_deref().and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO),
                data.last_filled_qty.parse().unwrap_or_default(),
            ),
            OrderUpdateData::PlaceOrder(data) => (
                data.common.symbol.clone(),
                data.common.order_id.to_string(),
                data.common.client_order_id.as_ref().map(|c| c.to_string()),
                data.common.status.clone(),
                data.side.clone(),
                data.price.parse().unwrap_or_default(),
                data.quantity.parse().unwrap_or_default(),
                Decimal::ZERO,
            ),
            OrderUpdateData::Cancel(data) => (
                data.common.symbol.clone(),
                data.common.order_id.to_string(),
                data.common.client_order_id.as_ref().map(|c| c.to_string()),
                data.common.status.clone(),
                String::new(),
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            ),
        };
    let status = match status_str.as_str() {
        "NEW" => OrderStatus::Open,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "EXPIRED" | "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Open,
    };
    let side = match side_str.as_str() {
        "BUY" => Side::Buy,
        _ => Side::Sell,
    };
    OrderLifecycle {
        exchange: EXCHANGE.into(),
        order: Order {
            id: order_id,
            client_id,
            symbol,
            side,
            order_type: OrderType::Limit,
            price,
            quantity,
            filled_quantity,
            status,
        },
    }
}
