//! `HyperliquidConnection` — sets up authenticated REST + streaming clients,
//! subscribes to the required WS feeds, and demultiplexes `Message`s into
//! typed per-event channels.
//!
//! Canonical sources:
//!   - `Message::UserFills` → `Trade` (one per execution, authoritative source of position
//!     changes).
//!   - `Message::OrderUpdates` → `OrderLifecycle` (status transitions, used for reconcile and
//!     `client_id` → oid resolution).
//!   - `Message::L2Book` → `BookUpdate`.
//!   - `Message::ActiveAssetCtx` → `MarkPriceUpdate` (carries both `mark_px` and funding — fixes
//!     the longstanding "funding is always zero" gap).
//!   - `Message::AllMids` → `MarkPriceUpdate` (fallback with `funding_rate`=0 until the per-coin
//!     `ActiveAssetCtx` arrives).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bb_core::error::BotError;
use bb_core::events::{BookUpdate, MarkPriceUpdate, OrderLifecycle, Trade};
use bb_core::harness::MpscFeed;
use ethers::signers::{LocalWallet, Signer};
use hyperliquid_rust_sdk::{BaseUrl, ExchangeClient, InfoClient, Message, Subscription};
use tokio::sync::mpsc;

use crate::broker::{ConnectionHealth, HyperliquidBroker, new_client_id_map};
use crate::config::HyperliquidConfig;
use crate::convert;

/// `BookUpdate` / `MarkPriceUpdate` channels are bounded — the muxer uses
/// `try_send` and drops-newest on overflow. `Trade` / `OrderLifecycle` stay
/// unbounded: missing fills permanently corrupts position tracking.
const BOOK_CHANNEL_CAPACITY: usize = 4_096;
const MARK_CHANNEL_CAPACITY: usize = 256;

/// HL's WS sends data continuously (`AllMids` ~250ms, `ActiveAssetCtx`, depth);
/// a gap longer than this is treated as a transparent reconnect, triggering
/// a reconcile signal so strategies can resync against REST.
const HL_WS_QUIET_THRESHOLD: Duration = Duration::from_secs(10);

pub struct HyperliquidFeeds {
    pub trade: MpscFeed<Trade>,
    pub book: MpscFeed<BookUpdate>,
    pub lifecycle: MpscFeed<OrderLifecycle>,
    pub mark_price: MpscFeed<MarkPriceUpdate>,
}

/// Connect to Hyperliquid and return the REST broker plus typed feeds for
/// the harness to wire up. `symbol` is in bb format (e.g. `"BTC-USD"`).
#[allow(clippy::too_many_lines)]
pub async fn connect(
    config: &HyperliquidConfig,
    symbol: &str,
) -> Result<(HyperliquidBroker, HyperliquidFeeds), BotError> {
    let raw_key = secrecy::ExposeSecret::expose_secret(&config.private_key_hex);
    let key_hex = raw_key.strip_prefix("0x").unwrap_or(raw_key.as_str());
    let wallet: LocalWallet =
        key_hex.parse().map_err(|e| BotError::config(format!("Invalid HL private key: {e}")))?;
    let address = wallet.address();
    let base_url = match config.network.as_str() {
        "mainnet" => BaseUrl::Mainnet,
        _ => BaseUrl::Testnet,
    };

    let exchange_client = ExchangeClient::new(None, wallet.clone(), Some(base_url), None, None)
        .await
        .map_err(|e| BotError::exchange(e, true))?;
    let info =
        InfoClient::new(None, Some(base_url)).await.map_err(|e| BotError::exchange(e, true))?;

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
    let (book_tx, book_rx) = mpsc::channel::<BookUpdate>(BOOK_CHANNEL_CAPACITY);
    let (life_tx, life_rx) = mpsc::unbounded_channel::<OrderLifecycle>();
    let (mark_tx, mark_rx) = mpsc::channel::<MarkPriceUpdate>(MARK_CHANNEL_CAPACITY);

    // Connection health flags shared with broker. The HL SDK reconnects
    // transparently — there's no explicit `Reconnecting` event surfaced to
    // userspace — so we infer reconnects from message-stream gaps.
    let health = Arc::new(ConnectionHealth::default());
    let muxer_health = Arc::clone(&health);
    let client_ids = new_client_id_map();
    let muxer_client_ids = Arc::clone(&client_ids);

    let target_coin = coin.clone();
    tokio::spawn(async move {
        let _ws_info = ws_info; // keep WS alive
        // Track the highest OrderUpdate.status_timestamp we've seen. HL stamps
        // each frame with millisecond timestamps; a frame arriving below the
        // high-water mark indicates out-of-order delivery or a replay across
        // a reconnect — worth surfacing.
        let mut last_order_timestamp: u64 = 0;
        let mut last_msg_at = Instant::now();
        loop {
            let recv = tokio::time::timeout(HL_WS_QUIET_THRESHOLD, ws_rx.recv()).await;
            let msg = match recv {
                Err(_elapsed) => {
                    // No traffic for HL_WS_QUIET_THRESHOLD — proxy for a
                    // transparent reconnect. Flag for reconciliation; do
                    // not break.
                    tracing::warn!(
                        quiet_secs = HL_WS_QUIET_THRESHOLD.as_secs(),
                        "HL WS quiet — flagging reconcile (transparent reconnect proxy)"
                    );
                    muxer_health.reconcile_pending.store(true, Ordering::Release);
                    last_msg_at = Instant::now();
                    continue;
                }
                Ok(None) => {
                    tracing::error!("Hyperliquid: WS muxer ended — flagging disconnected");
                    muxer_health.disconnected.store(true, Ordering::Release);
                    break;
                }
                Ok(Some(msg)) => {
                    // Catch silent reconnects: if the SDK's transparent
                    // reconnect was fast enough that we got a message
                    // before our timeout fired but after a real gap.
                    let gap = last_msg_at.elapsed();
                    if gap > HL_WS_QUIET_THRESHOLD {
                        tracing::warn!(
                            gap_secs = gap.as_secs(),
                            "HL WS message after gap — flagging reconcile"
                        );
                        muxer_health.reconcile_pending.store(true, Ordering::Release);
                    }
                    last_msg_at = Instant::now();
                    msg
                }
            };
            match msg {
                Message::L2Book(b) if b.data.coin == target_coin => {
                    // drop-newest on overflow: next snapshot is incoming
                    let _ = book_tx.try_send(convert::l2_book_to_event(&b.data));
                }
                Message::OrderUpdates(u) => {
                    for update in u.data.iter().filter(|u| u.order.coin == target_coin) {
                        if update.status_timestamp < last_order_timestamp {
                            tracing::warn!(
                                previous = last_order_timestamp,
                                current = update.status_timestamp,
                                delta_ms = last_order_timestamp - update.status_timestamp,
                                oid = update.order.oid,
                                "HL OrderUpdate timestamp regressed — out-of-order or replay"
                            );
                        } else {
                            last_order_timestamp = update.status_timestamp;
                        }
                        let _ = life_tx
                            .send(convert::order_update_to_lifecycle(update, &muxer_client_ids));
                    }
                }
                Message::UserFills(f) => {
                    for fill in f.data.fills.iter().filter(|f| f.coin == target_coin) {
                        if let Some(trade) = convert::fill_to_trade(fill, &muxer_client_ids) {
                            let _ = trade_tx.send(trade);
                        }
                    }
                }
                Message::AllMids(m) => {
                    if let Some(mid_str) = m.data.mids.get(&target_coin)
                        && let Some(mark_price) =
                            bb_core::helpers::parse_decimal_or_warn(mid_str, "AllMids.mid")
                    {
                        let _ = mark_tx.try_send(MarkPriceUpdate {
                            exchange: "hyperliquid".into(),
                            symbol: convert::to_bb_symbol(&target_coin),
                            mark_price,
                            funding_rate: None, // AllMids carries no funding rate
                        });
                    }
                }
                Message::ActiveAssetCtx(ctx) if ctx.data.coin == target_coin => {
                    if let Some(event) = convert::active_asset_ctx_to_mark(&ctx.data) {
                        let _ = mark_tx.try_send(event);
                    }
                }
                _ => {}
            }
        }
        tracing::warn!("Hyperliquid: WS muxer ended");
    });

    let broker = HyperliquidBroker::new(exchange_client, info, address, health, client_ids);
    let feeds = HyperliquidFeeds {
        trade: MpscFeed::new(trade_rx),
        book: MpscFeed::bounded(book_rx),
        lifecycle: MpscFeed::new(life_rx),
        mark_price: MpscFeed::bounded(mark_rx),
    };
    Ok((broker, feeds))
}
