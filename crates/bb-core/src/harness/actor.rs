//! Actors — typed event consumers with a lifecycle.
//!
//! An actor is any `Send + 'static` value that implements [`Actor`] and one or
//! more [`EventHandler<E>`] impls for the event types it cares about. The
//! harness calls `init` once before any handler, drives `on_event` on each
//! published value, and calls `wind_down` exactly once at shutdown (whether
//! clean, signal-driven, or failure-driven).
//!
//! Event handling is serialized per actor: the harness guards each actor
//! instance with a mutex so that `on_event` calls never overlap, and
//! `init`/`wind_down` run with exclusive access. This mirrors the actor-model
//! expectation that internal state mutation is single-threaded.

use std::sync::Arc;

use async_trait::async_trait;

use super::event::Event;
use crate::broker::{Broker, BrokerRegistry};
use crate::error::BotError;

/// Shared context available inside `init`, `on_event`, and `wind_down`. Holds
/// broker handles for order placement and a cancellation token for the actor
/// to request its own shutdown.
pub struct ActorContext {
    name: Arc<str>,
    brokers: Arc<BrokerRegistry>,
    shutdown: tokio_util::sync::CancellationToken,
}

impl ActorContext {
    pub fn new(
        name: Arc<str>,
        brokers: Arc<BrokerRegistry>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self { name, brokers, shutdown }
    }

    pub fn actor_name(&self) -> &str {
        &self.name
    }

    /// Look up a broker by name. Returns `Err(UnknownExchange)` if no broker
    /// is registered under that name — the typed error beats panicking on an
    /// `unwrap` and gives the actor a useful message for its strategy error.
    pub fn broker(&self, name: &str) -> Result<Arc<dyn Broker>, BotError> {
        self.brokers
            .get(name)
            .map(Arc::clone)
            .ok_or_else(|| BotError::UnknownExchange(name.to_string()))
    }

    /// Full registry — for the rare case (e.g. funding-arb) that needs to
    /// iterate all brokers. Prefer [`broker`] when you know the name.
    pub fn brokers(&self) -> &BrokerRegistry {
        &self.brokers
    }

    /// Request a graceful shutdown of the entire harness.
    pub fn request_shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// Why the actor is winding down. The harness hands this to `wind_down` so the
/// actor can log, page, or decide whether to cancel orders.
#[derive(Debug, Clone)]
pub enum WindDownReason {
    /// OS signal (typically Ctrl-C) or an external shutdown trigger.
    Signal,
    /// Every feed has finished producing and there's nothing more to handle.
    InputsClosed,
    /// An actor returned a fatal error from `init` or `on_event`. The failing
    /// actor's name is included.
    ActorFailed { actor: String, error: String },
    /// A feed returned a non-recoverable error.
    FeedFailed { feed: String, error: String },
}

/// Lifecycle trait. `Actor` is always implemented alongside zero or more
/// [`EventHandler<E>`] impls.
#[async_trait]
pub trait Actor: Send + 'static {
    /// Called exactly once, before any event is dispatched.
    async fn init(&mut self, _cx: &ActorContext) -> Result<(), BotError> {
        Ok(())
    }

    /// Called exactly once, after all subscriptions have been cancelled.
    /// Use this to cancel working orders, flatten positions, or log final stats.
    async fn wind_down(
        &mut self,
        _reason: &WindDownReason,
        _cx: &ActorContext,
    ) -> Result<(), BotError> {
        Ok(())
    }

    /// JSON status snapshot for the HTTP status API. Default is `null`.
    fn status(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

/// Per-event-type handler. Implement once per event type the actor consumes.
#[async_trait]
pub trait EventHandler<E: Event>: Actor {
    async fn on_event(&mut self, event: E, cx: &ActorContext) -> Result<(), BotError>;
}
