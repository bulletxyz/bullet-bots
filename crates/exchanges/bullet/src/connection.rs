//! `BulletConnection` — owns a single `ManagedWebsocket` and demultiplexes
//! `ServerMessage`s into typed per-event-kind channels.
//!
//! Each channel backs one `EventFeed` implementation via the generic
//! [`bb_core::harness::MpscFeed`]. The muxer task is spawned at connection
//! time and runs until the `ManagedWebsocket` returns a terminal `Disconnected`.
//!
//! Canonical source split — important for correctness:
//!   - `OrderUpdateData::TradeFill` emits BOTH a `Trade` (for inventory) and an `OrderLifecycle`
//!     (for reconcile). They are independent events so strategies that only handle `Trade` won't
//!     miss position updates, and strategies that only handle `OrderLifecycle` won't miss a
//!     transition.
//!   - `OrderUpdateData::PlaceOrder` / `Cancel` emit only `OrderLifecycle` — they carry no
//!     execution, so there's no `Trade` to emit.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bb_core::error::BotError;
use bb_core::events::{BookUpdate, MarkPriceUpdate, OrderLifecycle, Trade};
use bb_core::harness::MpscFeed;
use bullet_rust_sdk::ws::models::ServerMessage;
use bullet_rust_sdk::{
    Client, Keypair, ManagedWebsocket, Network, OrderbookDepth, Topic, UserActionDiscriminants,
    WsEvent,
};
use tokio::sync::mpsc;

use crate::broker::{BulletBroker, ConnectionHealth, load_increments};

/// BookUpdate / MarkPriceUpdate channels are bounded — the muxer uses
/// `try_send` and drops-newest on overflow, since the next tick supersedes
/// the previous one. Trade / OrderLifecycle stay unbounded: missing a fill
/// or state transition permanently corrupts position tracking.
const BOOK_CHANNEL_CAPACITY: usize = 4_096;
const MARK_CHANNEL_CAPACITY: usize = 256;
use crate::config::BulletConfig;
use crate::convert;

pub struct BulletFeeds {
    pub trade: MpscFeed<Trade>,
    pub book: MpscFeed<BookUpdate>,
    pub lifecycle: MpscFeed<OrderLifecycle>,
    pub mark_price: MpscFeed<MarkPriceUpdate>,
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
        other => {
            return Err(BotError::config(format!(
                "Unknown Bullet network '{other}' — use 'mainnet' or 'testnet'"
            )));
        }
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
    let ws = client.connect_ws_managed().call().await.map_err(|e| BotError::exchange(e, true))?;
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
    ws.subscribe_raw([user_topic.clone()], None).map_err(|e| BotError::exchange(e, false))?;

    tracing::info!(
        symbol,
        address = %address,
        user_topic,
        symbols_known = client.symbols().len(),
        "Bullet: connected + subscribed"
    );

    // Typed channels (one per feed).
    // Trade and OrderLifecycle are unbounded — missing fills corrupts position tracking.
    // BookUpdate and MarkPriceUpdate are bounded — newest tick supersedes previous.
    let (trade_tx, trade_rx) = mpsc::unbounded_channel::<Trade>();
    let (book_tx, book_rx) = mpsc::channel::<BookUpdate>(BOOK_CHANNEL_CAPACITY);
    let (life_tx, life_rx) = mpsc::unbounded_channel::<OrderLifecycle>();
    let (mark_tx, mark_rx) = mpsc::channel::<MarkPriceUpdate>(MARK_CHANNEL_CAPACITY);

    // Connection health flags shared with the broker. Muxer task flips them
    // on WS reconnect / permanent disconnect; strategies poll via the
    // `Broker::take_reconcile_signal` / `is_disconnected` trait methods.
    let health = Arc::new(ConnectionHealth::default());

    // Muxer task — reads WS, classifies, forwards. Holds `ws` so the connection
    // stays alive for the lifetime of the task.
    tokio::spawn(muxer_loop(ws, trade_tx, book_tx, life_tx, mark_tx, Arc::clone(&health)));

    let broker = BulletBroker::new(Arc::clone(&client), increments, health);
    let feeds = BulletFeeds {
        trade: MpscFeed::new(trade_rx),
        book: MpscFeed::bounded(book_rx),
        lifecycle: MpscFeed::new(life_rx),
        mark_price: MpscFeed::bounded(mark_rx),
    };
    Ok((broker, feeds))
}

async fn muxer_loop(
    mut ws: ManagedWebsocket,
    trade_tx: mpsc::UnboundedSender<Trade>,
    book_tx: mpsc::Sender<BookUpdate>,
    life_tx: mpsc::UnboundedSender<OrderLifecycle>,
    mark_tx: mpsc::Sender<MarkPriceUpdate>,
    health: Arc<ConnectionHealth>,
) {
    // Track the highest OrderUpdate event_time we've seen. Bullet stamps each
    // user-data frame with the rollup block's microsecond timestamp; a frame
    // arriving with a lower value than the high-water mark means out-of-order
    // delivery (or a replay across a reconnect), worth surfacing.
    let mut last_order_event_time: u64 = 0;
    loop {
        let Some(ws_event) = ws.recv().await else {
            tracing::error!("Bullet: managed WS ended — flagging disconnected");
            health.disconnected.store(true, Ordering::Release);
            break;
        };
        match ws_event {
            WsEvent::Message(msg) => match *msg {
                ServerMessage::DepthUpdate(ref depth) => {
                    // drop-newest on overflow: book consumers will get the next snapshot
                    let _ = book_tx.try_send(convert::depth_to_event(depth));
                }
                ServerMessage::BookTicker(ref bt) => {
                    let _ = book_tx.try_send(convert::book_ticker_to_event(bt));
                }
                ServerMessage::MarkPrice(ref mp) => {
                    if let Some(event) = convert::mark_price_to_event(mp) {
                        let _ = mark_tx.try_send(event);
                    }
                }
                ServerMessage::OrderUpdate(ref update) => {
                    if update.event_time < last_order_event_time {
                        tracing::warn!(
                            previous = last_order_event_time,
                            current = update.event_time,
                            delta_us = last_order_event_time - update.event_time,
                            "OrderUpdate event_time regressed — out-of-order or replay"
                        );
                    } else {
                        last_order_event_time = update.event_time;
                    }
                    // Emit Trade and/or OrderLifecycle depending on variant.
                    if let Some(trade) = convert::order_update_to_trade(update) {
                        let _ = trade_tx.send(trade);
                    }
                    let _ = life_tx.send(convert::order_update_to_lifecycle(update));
                }
                // AggTrade intentionally not forwarded — the `OrderUpdate`
                // TradeFill variant is the authoritative fill source for our
                // account. Emitting from both would double-count.
                _ => continue,
            },
            WsEvent::Reconnecting => {
                tracing::warn!("Bullet: WebSocket reconnecting — flagging reconcile");
                // Any state changes (fills, cancels) during the disconnect
                // window are lost. Strategies that observe this flag should
                // immediately query open orders to resync.
                health.reconcile_pending.store(true, Ordering::Release);
            }
            WsEvent::Disconnected(reason) => {
                tracing::error!(%reason, "Bullet: permanently disconnected — flagging");
                health.disconnected.store(true, Ordering::Release);
                break;
            }
        }
    }
}
