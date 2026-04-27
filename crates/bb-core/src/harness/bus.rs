//! Type-indexed broadcast bus.
//!
//! The bus stores one `broadcast::Sender<E>` per concrete event type, created
//! lazily on first access. Feeds obtain a sender via [`EventBus::sender`];
//! actors obtain a receiver via [`EventBus::subscribe`]. Both calls are
//! idempotent — subsequent calls for the same `E` hand out clones / new
//! subscriptions of the same underlying channel.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use super::event::Event;

/// Default broadcast channel capacity for event types that haven't been
/// individually tuned. 1024 is large enough for typical quoting cadence
/// without hoarding memory when events are small.
pub const DEFAULT_CAPACITY: usize = 1024;

struct Inner {
    channels: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    capacities: HashMap<TypeId, usize>,
}

/// Type-indexed broadcast bus with optional per-event-type capacity overrides.
///
/// Use [`EventBus::new`] for a default-capacity bus, or create a configured
/// one via [`EventBus::with_capacities`] (typically driven by
/// `HarnessBuilder::with_event_capacity`).
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<Mutex<Inner>>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub fn new() -> Self {
        Self::with_capacities(HashMap::new())
    }

    /// Build a bus with the given per-type capacity overrides. Types not in
    /// the map use [`DEFAULT_CAPACITY`].
    pub fn with_capacities(capacities: HashMap<TypeId, usize>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner { channels: HashMap::new(), capacities })),
        }
    }

    /// Obtain a sender for events of type `E`. The first call for a given `E`
    /// creates the channel at the configured capacity; later calls return
    /// clones of the same sender.
    pub fn sender<E: Event>(&self) -> broadcast::Sender<E> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let cap = guard
            .capacities
            .get(&TypeId::of::<E>())
            .copied()
            .unwrap_or(DEFAULT_CAPACITY);
        let entry = guard
            .channels
            .entry(TypeId::of::<E>())
            .or_insert_with(|| Box::new(broadcast::channel::<E>(cap).0));
        entry
            .downcast_ref::<broadcast::Sender<E>>()
            .unwrap_or_else(|| unreachable!("EventBus: TypeId collision — impossible with monomorphized E"))
            .clone()
    }

    /// Subscribe to events of type `E`. Panics cannot happen: the sender is
    /// created on demand via [`sender`] if it doesn't exist yet.
    pub fn subscribe<E: Event>(&self) -> broadcast::Receiver<E> {
        self.sender::<E>().subscribe()
    }
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.inner.lock().map(|g| g.channels.len()).unwrap_or(0);
        f.debug_struct("EventBus").field("channels", &len).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct A(u32);
    #[derive(Clone, Debug, PartialEq)]
    struct B(&'static str);

    #[tokio::test]
    async fn sends_and_receives_per_type() {
        let bus = EventBus::new();
        let mut rx_a = bus.subscribe::<A>();
        let mut rx_b = bus.subscribe::<B>();

        bus.sender::<A>().send(A(42)).unwrap();
        bus.sender::<B>().send(B("hi")).unwrap();

        assert_eq!(rx_a.recv().await.unwrap(), A(42));
        assert_eq!(rx_b.recv().await.unwrap(), B("hi"));
    }

    #[tokio::test]
    async fn multiple_subscribers_each_see_events() {
        let bus = EventBus::new();
        let mut rx1 = bus.subscribe::<A>();
        let mut rx2 = bus.subscribe::<A>();
        bus.sender::<A>().send(A(7)).unwrap();
        assert_eq!(rx1.recv().await.unwrap(), A(7));
        assert_eq!(rx2.recv().await.unwrap(), A(7));
    }

    #[tokio::test]
    async fn custom_capacity_is_used() {
        // With capacity 2, sending 3 messages before the subscriber drains
        // should produce a Lagged error on the first recv.
        let mut caps = HashMap::new();
        caps.insert(TypeId::of::<A>(), 2usize);
        let bus = EventBus::with_capacities(caps);
        let tx = bus.sender::<A>();
        let mut rx = bus.subscribe::<A>();

        let _ = tx.send(A(1));
        let _ = tx.send(A(2));
        let _ = tx.send(A(3)); // overflows capacity=2

        assert!(matches!(
            rx.recv().await,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
        ));
    }
}
