//! Event feeds — async producers that push events into the bus.
//!
//! A feed implements [`EventFeed<E>`] and is wired into the harness via
//! [`HarnessBuilder::wire_feed_named`]. The harness gives each feed a typed
//! [`EventTx`] (its bus sender) and a [`FeedContext`] with a cooperative
//! cancellation signal. When the shutdown trigger fires, feeds are expected
//! to observe `cx.cancelled()` and return promptly.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;
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

/// Context passed to a feed's `run` method. Owns the feed name and the
/// cooperative cancellation token for graceful shutdown.
pub struct FeedContext {
    name: Arc<str>,
    cancel: CancellationToken,
}

impl FeedContext {
    pub fn new(name: Arc<str>, cancel: CancellationToken) -> Self {
        Self { name, cancel }
    }

    pub fn feed_name(&self) -> &str {
        &self.name
    }

    /// Resolves once the harness requests shutdown. Feeds that block on I/O
    /// should select over this alongside their read loop.
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
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
