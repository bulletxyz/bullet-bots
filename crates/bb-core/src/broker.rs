//! REST / request-response side of an exchange. Actors call into a `Broker`
//! from their event handlers to place orders, query state, etc.
//!
//! The streaming (WebSocket) side lives in typed [`EventFeed`](crate::harness::EventFeed)
//! implementations instead of being bundled here. That split is why the
//! framework can enforce "one canonical source per fact" at the type level
//! (e.g., `Trade` events are the only way position changes reach an actor).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::BotError;
use crate::types::{
    Balance, CancelOrder, CancelResult, NewOrder, Order, OrderBook, OrderResult, Position,
};

#[async_trait]
pub trait Broker: Send + Sync + 'static {
    fn name(&self) -> &str;

    async fn get_orderbook(&self, symbol: &str, depth: usize) -> Result<OrderBook, BotError>;
    async fn get_balances(&self) -> Result<Vec<Balance>, BotError>;
    async fn get_positions(&self) -> Result<Vec<Position>, BotError>;
    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>, BotError>;

    async fn place_orders(&self, orders: &[NewOrder]) -> Result<Vec<OrderResult>, BotError>;
    async fn cancel_orders(
        &self,
        cancels: &[CancelOrder],
    ) -> Result<Vec<CancelResult>, BotError>;
    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), BotError>;
}

/// Name → broker handle. Actors obtain this via `ActorContext::brokers()`.
#[derive(Default)]
pub struct BrokerRegistry {
    by_name: HashMap<Arc<str>, Arc<dyn Broker>>,
}

impl BrokerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        name: Arc<str>,
        broker: Arc<dyn Broker>,
    ) -> Result<(), BotError> {
        if self.by_name.contains_key(&name) {
            return Err(BotError::config(format!("duplicate broker name: {name}")));
        }
        self.by_name.insert(name, broker);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn Broker>> {
        self.by_name.get(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.by_name.keys().map(|k| k.as_ref())
    }

    /// Convenience: require a broker by name, returning a typed error if
    /// missing. Actors use this when they know the exchange name a priori.
    pub fn require(&self, name: &str) -> Result<&Arc<dyn Broker>, BotError> {
        self.get(name)
            .ok_or_else(|| BotError::UnknownExchange(name.to_string()))
    }
}

impl std::fmt::Debug for BrokerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerRegistry")
            .field("names", &self.by_name.keys().collect::<Vec<_>>())
            .finish()
    }
}
