//! Event feeds — async producers that push events into the bus.
//!
//! A feed implements [`EventFeed<E>`] and is wired into the harness via
//! [`HarnessBuilder::wire_feed_named`]. The harness gives each feed a typed
//! [`EventTx`] (its bus sender) and a [`FeedContext`] with a cooperative
//! cancellation signal. When the shutdown trigger fires, feeds are expected
//! to observe `cx.cancelled()` and return promptly.


use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use super::event::Event;
use crate::error::BotError;

/// Typed sender handed to a feed. Wraps `broadcast::Sender<E>` so that
/// `send` surfaces a meaningful error and hides lagging-subscriber details
/// behind a simpler error type.
#[derive(Clone)]
pub struct EventTx<E: Event> {
    tx: broadcast::Sender<E>,
}

impl<E: Event> EventTx<E> {
    pub fn new(tx: broadcast::Sender<E>) -> Self {
        Self { tx }
    }

    /// Publish an event. Returns `Ok(n)` where `n` is the number of live
    /// subscribers the message was queued for, or `Err` if there are no
    /// subscribers. A no-subscriber error is informational — the feed can
    /// ignore and keep running.
    pub fn send(&self, event: E) -> Result<usize, NoSubscribers> {
        self.tx.send(event).map_err(|_| NoSubscribers)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("no subscribers for this event type")]
pub struct NoSubscribers;

/// Context passed to a feed's `run` method. Just the cooperative
/// cancellation token — feeds `select!` on `cancelled()` alongside their
/// read loop to exit promptly on shutdown.
pub struct FeedContext {
    cancel: CancellationToken,
}

impl FeedContext {
    pub fn new(cancel: CancellationToken) -> Self {
        Self { cancel }
    }

    /// Resolves once the harness requests shutdown.
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }
}

/// An async producer of events of type `E`. One feed per event type. Feeds
/// own their upstream (e.g., a WebSocket) and are responsible for reconnection
/// within their `run` body — the harness does not restart a feed that returns
/// (whether cleanly or with error).
#[async_trait]
pub trait EventFeed<E: Event>: Send + 'static {
    async fn run(self: Box<Self>, tx: EventTx<E>, cx: FeedContext) -> Result<(), BotError>;
}

/// Generic feed backed by an unbounded mpsc receiver. Exchange adapters use
/// this to bridge the muxer task (which writes to a `mpsc::Sender<E>`) and the
/// harness event bus (which the feed forwards into via `EventTx<E>`).
///
/// The muxer task is responsible for its own reconnect logic; this struct just
/// drains the channel into the bus until cancelled or the sender drops.
pub struct MpscFeed<E: Event> {
    rx: mpsc::UnboundedReceiver<E>,
}

impl<E: Event> MpscFeed<E> {
    pub fn new(rx: mpsc::UnboundedReceiver<E>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl<E: Event> EventFeed<E> for MpscFeed<E> {
    async fn run(self: Box<Self>, tx: EventTx<E>, cx: FeedContext) -> Result<(), BotError> {
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
