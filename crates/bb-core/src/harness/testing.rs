//! Test helpers for the harness.
//!
//! [`ScriptedFeed`] lets tests stand in as a feed that emits a preset list of
//! events and then exits cleanly. Combine with a stub `Broker` and you can
//! drive actors end-to-end under `cargo test`.
//!
//! [`MarketDataReplayFeed`] is a timestamped variant: each event is paired
//! with a `unix_ms` value and the feed advances a [`TestClock`] before each
//! send. Wire the same `TestClock` into the harness via
//! `HarnessBuilder::with_clock` to give strategies deterministic wall-clock
//! time during replay.
//!
//! [`MockBroker`] is a programmable broker for testing. It records all calls
//! (inspect via [`MockBroker::history`]) and optionally returns pre-queued
//! responses — useful for testing retry logic, rate-limit handling, etc.
//! If no response is queued, all methods default to success. Queue responses
//! with [`MockBroker::queue_place_response`] and friends.
//!
//! [`NullBroker`] is a type alias for [`MockBroker`] for backwards compat.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::event::Event;
use super::feed::{EventFeed, EventTx, FeedContext};
use crate::broker::Broker;
use crate::clock::TestClock;
use crate::error::BotError;
use crate::types::{
    Balance, CancelOrder, CancelResult, NewOrder, Order, OrderBook, OrderResult, Position,
};

/// Emits a fixed sequence of events, then returns. Tests wire this in where
/// a production feed would go and assert on actor behavior.
pub struct ScriptedFeed<E: Event> {
    events: Vec<E>,
}

impl<E: Event> ScriptedFeed<E> {
    pub fn new(events: Vec<E>) -> Self {
        Self { events }
    }
}

#[async_trait]
impl<E: Event> EventFeed<E> for ScriptedFeed<E> {
    async fn run(self: Box<Self>, tx: EventTx<E>, _cx: FeedContext) -> Result<(), BotError> {
        for event in self.events {
            let _ = tx.send(event);
            // Yield between events so actor handler tasks get scheduled before
            // the next send. Without this, tests that assert on intermediate
            // state are structurally flaky.
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

/// Timestamped replay feed. Like [`ScriptedFeed`] but advances a
/// [`TestClock`] to each event's `unix_ms` before sending, so strategies
/// that call `cx.clock().unix_ms()` see deterministic, event-driven time.
///
/// # Usage
/// ```ignore
/// let clock = TestClock::new(1_700_000_000_000);
/// let feed = MarketDataReplayFeed::new(
///     vec![(1_700_000_001_000, trade1), (1_700_000_002_000, trade2)],
///     clock.clone(),
/// );
/// let harness = HarnessBuilder::new()
///     .with_clock(Arc::new(clock))
///     .wire_feed_named("trades", feed)
///     .wire_actor(ActorSpec::new("strategy", my_actor).sub_critical::<Trade>())
///     .build()?;
/// ```
pub struct MarketDataReplayFeed<E: Event> {
    events: Vec<(u64, E)>,
    clock: TestClock,
}

impl<E: Event> MarketDataReplayFeed<E> {
    /// `events` is a `(unix_ms, event)` sequence. Events are sent in order;
    /// the clock is advanced to each timestamp before the send.
    pub fn new(events: Vec<(u64, E)>, clock: TestClock) -> Self {
        Self { events, clock }
    }
}

#[async_trait]
impl<E: Event> EventFeed<E> for MarketDataReplayFeed<E> {
    async fn run(self: Box<Self>, tx: EventTx<E>, _cx: FeedContext) -> Result<(), BotError> {
        for (ts, event) in self.events {
            self.clock.set_unix_ms(ts);
            let _ = tx.send(event);
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

/// Like [`ScriptedFeed`] but each event is preceded by an optional sleep,
/// letting tests control ordering across multiple feeds without real delays.
/// Pair with `tokio::time::pause()` for deterministic scheduling.
///
/// Each entry is `(delay_before_send, event)`. A zero `Duration` skips the sleep.
pub struct TimedFeed<E: Event> {
    events: Vec<(Duration, E)>,
}

impl<E: Event> TimedFeed<E> {
    pub fn new(events: Vec<(Duration, E)>) -> Self {
        Self { events }
    }
}

#[async_trait]
impl<E: Event> EventFeed<E> for TimedFeed<E> {
    async fn run(self: Box<Self>, tx: EventTx<E>, _cx: FeedContext) -> Result<(), BotError> {
        for (delay, event) in self.events {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let _ = tx.send(event);
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

/// A single recorded broker call. Read via [`MockBroker::history`].
#[derive(Debug, Clone, Default)]
pub struct RecordedCall {
    pub method: &'static str,
    pub symbol: Option<String>,
    pub orders: Vec<NewOrder>,
    pub cancels: Vec<CancelOrder>,
}

/// Programmable test broker. Records all calls and returns pre-queued
/// responses; defaults to success when the queue is empty.
///
/// # Usage
/// ```ignore
/// let broker = MockBroker::shared("venue");
/// broker.queue_place_response(Err(BotError::exchange("rate limited", true)));
/// broker.queue_place_response(Ok(())); // next call auto-generates order IDs
///
/// // After running the strategy:
/// assert_eq!(broker.placed_count().await, 2);
/// ```
pub struct MockBroker {
    name: String,
    history: Mutex<Vec<RecordedCall>>,
    next_order_id: AtomicU64,
    place_queue: Mutex<VecDeque<Result<(), BotError>>>,
    cancel_queue: Mutex<VecDeque<Result<(), BotError>>>,
    /// Per-order cancel results. Each entry is the full `Vec<CancelResult>` for
    /// one `cancel_orders` call. Checked before `cancel_queue`; when populated,
    /// the queued result is returned as-is, bypassing the default all-success path.
    cancel_results_queue: Mutex<VecDeque<Vec<CancelResult>>>,
    cancel_all_queue: Mutex<VecDeque<Result<(), BotError>>>,
    /// Positions returned by `get_positions` (default empty).
    positions: Mutex<Vec<Position>>,
}

/// Backwards-compat alias.
pub type NullBroker = MockBroker;

impl MockBroker {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            history: Mutex::new(Vec::new()),
            next_order_id: AtomicU64::new(1),
            place_queue: Mutex::new(VecDeque::new()),
            cancel_queue: Mutex::new(VecDeque::new()),
            cancel_results_queue: Mutex::new(VecDeque::new()),
            cancel_all_queue: Mutex::new(VecDeque::new()),
            positions: Mutex::new(Vec::new()),
        }
    }

    /// Set the positions returned by `get_positions`.
    pub async fn set_positions(&self, positions: Vec<Position>) {
        *self.positions.lock().await = positions;
    }

    pub fn shared(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self::new(name))
    }

    /// Queue a response for the next `place_orders` call.
    /// `Ok(())` → auto-generate `OrderResult` with monotonic IDs.
    /// `Err(e)` → return that transport error.
    pub async fn queue_place_response(&self, response: Result<(), BotError>) {
        self.place_queue.lock().await.push_back(response);
    }

    /// Queue a response for the next `cancel_orders` call.
    pub async fn queue_cancel_response(&self, response: Result<(), BotError>) {
        self.cancel_queue.lock().await.push_back(response);
    }

    /// Queue per-order `CancelResult`s for the next `cancel_orders` call.
    /// The slice must have one entry per order in the call. Takes precedence
    /// over `queue_cancel_response` — use this to simulate partial failures
    /// (e.g. one order rejected, another accepted).
    pub async fn queue_cancel_results(&self, results: Vec<CancelResult>) {
        self.cancel_results_queue.lock().await.push_back(results);
    }

    /// Queue a response for the next `cancel_all_orders` call.
    pub async fn queue_cancel_all_response(&self, response: Result<(), BotError>) {
        self.cancel_all_queue.lock().await.push_back(response);
    }

    /// Full call history in arrival order.
    pub async fn history(&self) -> Vec<RecordedCall> {
        self.history.lock().await.clone()
    }

    /// Number of `place_orders` calls recorded.
    pub async fn placed_count(&self) -> usize {
        self.history.lock().await.iter().filter(|c| c.method == "place_orders").count()
    }

    /// Orders from the most recent `place_orders` call, or empty if none.
    pub async fn last_placed_orders(&self) -> Vec<NewOrder> {
        self.history
            .lock()
            .await
            .iter()
            .rev()
            .find(|c| c.method == "place_orders")
            .map(|c| c.orders.clone())
            .unwrap_or_default()
    }

    /// Cancels from the most recent `cancel_orders` call, or empty if none.
    pub async fn last_cancels(&self) -> Vec<CancelOrder> {
        self.history
            .lock()
            .await
            .iter()
            .rev()
            .find(|c| c.method == "cancel_orders")
            .map(|c| c.cancels.clone())
            .unwrap_or_default()
    }

    async fn record(&self, call: RecordedCall) {
        self.history.lock().await.push(call);
    }

    fn next_id(&self) -> u64 {
        self.next_order_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[async_trait]
impl Broker for MockBroker {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get_orderbook(&self, _symbol: &str, _depth: usize) -> Result<OrderBook, BotError> {
        Ok(OrderBook::default())
    }

    async fn get_balances(&self) -> Result<Vec<Balance>, BotError> {
        Ok(vec![])
    }

    async fn get_positions(&self) -> Result<Vec<Position>, BotError> {
        Ok(self.positions.lock().await.clone())
    }

    async fn get_open_orders(&self, _symbol: &str) -> Result<Vec<Order>, BotError> {
        Ok(vec![])
    }

    async fn place_orders(&self, orders: &[NewOrder]) -> Result<Vec<OrderResult>, BotError> {
        self.record(RecordedCall {
            method: "place_orders",
            orders: orders.to_vec(),
            ..Default::default()
        })
        .await;
        if let Some(Err(e)) = self.place_queue.lock().await.pop_front() {
            return Err(e);
        }
        Ok(orders
            .iter()
            .map(|o| OrderResult {
                order_id: Some(format!("test-{}", self.next_id())),
                client_id: o.client_id.clone(),
                success: true,
                error: None,
            })
            .collect())
    }

    async fn cancel_orders(&self, cancels: &[CancelOrder]) -> Result<Vec<CancelResult>, BotError> {
        self.record(RecordedCall {
            method: "cancel_orders",
            cancels: cancels.to_vec(),
            ..Default::default()
        })
        .await;
        if let Some(results) = self.cancel_results_queue.lock().await.pop_front() {
            return Ok(results);
        }
        if let Some(Err(e)) = self.cancel_queue.lock().await.pop_front() {
            return Err(e);
        }
        Ok(cancels
            .iter()
            .map(|c| CancelResult { order_id: c.order_id.clone(), success: true, error: None })
            .collect())
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), BotError> {
        self.record(RecordedCall {
            method: "cancel_all_orders",
            symbol: Some(symbol.to_string()),
            ..Default::default()
        })
        .await;
        if let Some(Err(e)) = self.cancel_all_queue.lock().await.pop_front() {
            return Err(e);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::error::BotError;
    use crate::events::Tick;
    use crate::harness::{
        Actor, ActorContext, ActorSpec, EventHandler, HarnessBuilder, WindDownReason,
    };

    struct TimestampCollector {
        seen: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl Actor for TimestampCollector {}

    #[async_trait]
    impl EventHandler<Tick> for TimestampCollector {
        async fn on_event(
            &mut self,
            _event: Tick,
            cx: &ActorContext,
        ) -> Result<(), crate::error::BotError> {
            self.seen.lock().unwrap().push(cx.clock().unix_ms());
            Ok(())
        }
    }

    #[tokio::test]
    async fn replay_feed_advances_clock() {
        let clock = TestClock::new(0);
        let seen: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

        let ticks = vec![(1_000u64, Tick::now()), (2_000u64, Tick::now()), (3_000u64, Tick::now())];
        let feed = MarketDataReplayFeed::new(ticks, clock.clone());

        let actor = TimestampCollector { seen: Arc::clone(&seen) };
        let harness = HarnessBuilder::new()
            .with_clock(Arc::new(clock))
            .wire_feed_named("ticks", feed)
            .wire_actor(ActorSpec::new("collector", actor).sub::<Tick>())
            .build()
            .unwrap();

        harness.run().await.unwrap();

        let got = seen.lock().unwrap().clone();
        assert_eq!(got, vec![1_000, 2_000, 3_000]);
    }

    struct OneTickThenWait;

    #[async_trait]
    impl EventFeed<Tick> for OneTickThenWait {
        async fn run(self: Box<Self>, tx: EventTx<Tick>, cx: FeedContext) -> Result<(), BotError> {
            let _ = tx.send(Tick::now());
            cx.cancelled().await;
            Ok(())
        }
    }

    struct FailingActor;

    #[async_trait]
    impl Actor for FailingActor {}

    #[async_trait]
    impl EventHandler<Tick> for FailingActor {
        async fn on_event(&mut self, _event: Tick, _cx: &ActorContext) -> Result<(), BotError> {
            Err(BotError::strategy("boom"))
        }
    }

    #[tokio::test]
    async fn fatal_handler_error_reports_actor_failed() {
        let harness = HarnessBuilder::new()
            .wire_feed_named("tick", OneTickThenWait)
            .wire_actor(ActorSpec::new("failing", FailingActor).sub::<Tick>())
            .build()
            .unwrap();

        let reason = harness.run().await.unwrap();
        match reason {
            WindDownReason::ActorFailed { actor, error } => {
                assert_eq!(actor, "failing");
                assert!(error.contains("boom"));
            }
            other => panic!("expected ActorFailed, got {other:?}"),
        }
    }
}
