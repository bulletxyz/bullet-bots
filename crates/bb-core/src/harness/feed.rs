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

enum RxKind<E> {
    Unbounded(mpsc::UnboundedReceiver<E>),
    Bounded(mpsc::Receiver<E>),
}

impl<E> RxKind<E> {
    async fn recv(&mut self) -> Option<E> {
        match self {
            Self::Unbounded(r) => r.recv().await,
            Self::Bounded(r) => r.recv().await,
        }
    }
}

/// Generic feed bridging an internal mpsc receiver to the harness event bus.
///
/// Exchange adapters use this to connect their muxer task to the harness:
/// the muxer writes to the sender side, and `MpscFeed` drains the receiver
/// into the bus until cancelled or the sender drops.
///
/// Supports both unbounded and bounded mpsc channels:
/// - [`MpscFeed::new`] — unbounded; safe for critical streams (`Trade`, `OrderLifecycle`) where
///   dropping events is not acceptable.
/// - [`MpscFeed::bounded`] — bounded; use for `BookUpdate` / `MarkPriceUpdate` where the muxer uses
///   `try_send` and drops-newest on overflow. The muxer is responsible for overflow logging.
pub struct MpscFeed<E: Event> {
    rx: RxKind<E>,
}

impl<E: Event> MpscFeed<E> {
    /// Wrap an unbounded receiver. The muxer's `send()` never blocks or fails
    /// due to backpressure. Suitable for loss-sensitive streams.
    pub fn new(rx: mpsc::UnboundedReceiver<E>) -> Self {
        Self { rx: RxKind::Unbounded(rx) }
    }

    /// Wrap a bounded receiver. The muxer should use `try_send` and handle
    /// `TrySendError::Full` by logging and discarding — suitable for
    /// high-frequency events where the newest data supersedes the oldest.
    pub fn bounded(rx: mpsc::Receiver<E>) -> Self {
        Self { rx: RxKind::Bounded(rx) }
    }
}

#[async_trait]
impl<E: Event> EventFeed<E> for MpscFeed<E> {
    async fn run(self: Box<Self>, tx: EventTx<E>, cx: FeedContext) -> Result<(), BotError> {
        let mut this = *self;
        loop {
            tokio::select! {
                biased;
                () = cx.cancelled() => return Ok(()),
                maybe = this.rx.recv() => match maybe {
                    Some(event) => { let _ = tx.send(event); }
                    None => return Ok(()),
                }
            }
        }
    }
}
