//! Test helpers for the harness.
//!
//! [`ScriptedFeed`] lets tests stand in as a feed that emits a preset list of
//! events and then exits cleanly. Combine with a stub `Broker` and you can
//! drive actors end-to-end under `cargo test`.
//!
//! [`NullBroker`] is a no-op broker that records calls — good enough for
//! strategies that place orders during init and want to avoid a real RPC.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::event::Event;
use super::feed::{EventFeed, EventTx, FeedContext};
use crate::broker::Broker;
use crate::error::BotError;
use crate::types::{
    Balance, CancelOrder, CancelResult, NewOrder, Order, OrderBook, OrderResult, Position,
};

/// Emits a fixed sequence of events, optionally spacing them by a delay, then
/// returns. Useful for unit-testing actor behavior against known inputs.
pub struct ScriptedFeed<E: Event> {
    events: Vec<E>,
    delay_between: Duration,
    name: &'static str,
}

impl<E: Event> ScriptedFeed<E> {
    pub fn new(events: Vec<E>) -> Self {
        Self { events, delay_between: Duration::from_millis(1), name: "scripted" }
    }

    #[must_use]
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay_between = delay;
        self
    }

    #[must_use]
    pub fn named(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }
}

#[async_trait]
impl<E: Event> EventFeed<E> for ScriptedFeed<E> {
    async fn run(self: Box<Self>, tx: EventTx<E>, cx: FeedContext) -> Result<(), BotError> {
        let this = *self;
        for event in this.events {
            if cx.is_cancelled() {
                return Ok(());
            }
            let _ = tx.send(event);
            tokio::time::sleep(this.delay_between).await;
        }
        Ok(())
    }
}

/// No-op broker that records the sequence of calls it saw. Every mutating
/// method succeeds, returning empty/success results. Read its `.history` from
/// tests to assert which orders were placed.
#[derive(Debug, Clone, Default)]
pub struct RecordedCall {
    pub method: &'static str,
    pub symbol: Option<String>,
    pub orders: Vec<NewOrder>,
    pub cancels: Vec<CancelOrder>,
}

pub struct NullBroker {
    name: String,
    history: Mutex<Vec<RecordedCall>>,
}

impl NullBroker {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), history: Mutex::new(Vec::new()) }
    }

    pub async fn history(&self) -> Vec<RecordedCall> {
        self.history.lock().await.clone()
    }

    pub fn shared(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self::new(name))
    }

    async fn record(&self, call: RecordedCall) {
        self.history.lock().await.push(call);
    }
}

#[async_trait]
impl Broker for NullBroker {
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
        Ok(orders
            .iter()
            .map(|o| OrderResult {
                order_id: String::new(),
                client_id: o.client_id.clone(),
                success: true,
                error: None,
            })
            .collect())
    }

    async fn cancel_orders(
        &self,
        cancels: &[CancelOrder],
    ) -> Result<Vec<CancelResult>, BotError> {
        self.record(RecordedCall {
            method: "cancel_orders",
            cancels: cancels.to_vec(),
            ..Default::default()
        })
        .await;
        Ok(cancels
            .iter()
            .map(|c| CancelResult {
                order_id: c.order_id.clone(),
                success: true,
                error: None,
            })
            .collect())
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), BotError> {
        self.record(RecordedCall {
            method: "cancel_all_orders",
            symbol: Some(symbol.to_string()),
            ..Default::default()
        })
        .await;
        Ok(())
    }
}
