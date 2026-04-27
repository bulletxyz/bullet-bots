//! Test helpers for the harness.
//!
//! [`ScriptedFeed`] lets tests stand in as a feed that emits a preset list of
//! events and then exits cleanly. Combine with a stub `Broker` and you can
//! drive actors end-to-end under `cargo test`.
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

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::event::Event;
use super::feed::{EventFeed, EventTx, FeedContext};
use crate::broker::Broker;
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
    cancel_all_queue: Mutex<VecDeque<Result<(), BotError>>>,
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
            cancel_all_queue: Mutex::new(VecDeque::new()),
        }
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

    /// Number of `cancel_orders` calls recorded.
    pub async fn cancel_count(&self) -> usize {
        self.history.lock().await.iter().filter(|c| c.method == "cancel_orders").count()
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

    /// Panic with a message if the number of `place_orders` calls ≠ `n`.
    pub async fn assert_placed_count(&self, n: usize) {
        let got = self.placed_count().await;
        assert_eq!(got, n, "expected {n} place_orders calls, got {got}");
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
        Ok(vec![])
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
        match self.place_queue.lock().await.pop_front() {
            Some(Err(e)) => return Err(e),
            _ => {}
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
        match self.cancel_queue.lock().await.pop_front() {
            Some(Err(e)) => return Err(e),
            _ => {}
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
        match self.cancel_all_queue.lock().await.pop_front() {
            Some(Err(e)) => return Err(e),
            _ => {}
        }
        Ok(())
    }
}
