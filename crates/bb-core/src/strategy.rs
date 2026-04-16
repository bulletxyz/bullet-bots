use std::collections::HashMap;

use async_trait::async_trait;

use crate::config::EngineConfig;
use crate::error::BotError;
use crate::exchange::Exchange;
use crate::types::*;

/// Cached state for a single exchange, maintained by the engine from events.
#[derive(Debug, Default)]
pub struct ExchangeState {
    pub orderbook: OrderBook,
    pub positions: Vec<Position>,
    pub balances: Vec<Balance>,
    pub open_orders: Vec<Order>,
}

/// Provides strategies with controlled access to exchanges and cached state.
pub struct StrategyContext {
    exchanges: HashMap<String, Box<dyn Exchange>>,
    state: HashMap<String, ExchangeState>,
    primary_exchange_name: String,
    pub config: EngineConfig,
}

impl StrategyContext {
    pub fn new(
        exchanges: HashMap<String, Box<dyn Exchange>>,
        primary_exchange_name: String,
        config: EngineConfig,
    ) -> Self {
        let state = exchanges.keys().map(|k| (k.clone(), ExchangeState::default())).collect();
        Self { exchanges, state, primary_exchange_name, config }
    }

    /// Get the name of the primary exchange.
    pub fn primary_exchange_name(&self) -> &str {
        &self.primary_exchange_name
    }

    /// Place orders on a named exchange.
    pub async fn place_orders(
        &self,
        exchange: &str,
        orders: &[NewOrder],
    ) -> Result<Vec<OrderResult>, BotError> {
        let ex = self
            .exchanges
            .get(exchange)
            .ok_or_else(|| BotError::UnknownExchange(exchange.to_string()))?;
        ex.place_orders(orders).await
    }

    /// Cancel orders on a named exchange.
    pub async fn cancel_orders(
        &self,
        exchange: &str,
        cancels: &[CancelOrder],
    ) -> Result<Vec<CancelResult>, BotError> {
        let ex = self
            .exchanges
            .get(exchange)
            .ok_or_else(|| BotError::UnknownExchange(exchange.to_string()))?;
        ex.cancel_orders(cancels).await
    }

    /// Cancel all orders for a symbol on a named exchange.
    pub async fn cancel_all_orders(&self, exchange: &str, symbol: &str) -> Result<(), BotError> {
        let ex = self
            .exchanges
            .get(exchange)
            .ok_or_else(|| BotError::UnknownExchange(exchange.to_string()))?;
        ex.cancel_all_orders(symbol).await
    }

    /// Fetch open orders from a named exchange (live query, not cached).
    pub async fn get_open_orders(
        &self,
        exchange: &str,
        symbol: &str,
    ) -> Result<Vec<Order>, BotError> {
        let ex = self
            .exchanges
            .get(exchange)
            .ok_or_else(|| BotError::UnknownExchange(exchange.to_string()))?;
        ex.get_open_orders(symbol).await
    }

    /// Fetch orderbook from a named exchange (live query).
    pub async fn get_orderbook(
        &self,
        exchange: &str,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBook, BotError> {
        let ex = self
            .exchanges
            .get(exchange)
            .ok_or_else(|| BotError::UnknownExchange(exchange.to_string()))?;
        ex.get_orderbook(symbol, depth).await
    }

    // -- Cached state reads (no network) --

    pub fn orderbook(&self, exchange: &str) -> Option<&OrderBook> {
        self.state.get(exchange).map(|s| &s.orderbook)
    }

    pub fn positions(&self, exchange: &str) -> Option<&[Position]> {
        self.state.get(exchange).map(|s| s.positions.as_slice())
    }

    pub fn balances(&self, exchange: &str) -> Option<&[Balance]> {
        self.state.get(exchange).map(|s| s.balances.as_slice())
    }

    pub fn open_orders_cached(&self, exchange: &str) -> Option<&[Order]> {
        self.state.get(exchange).map(|s| s.open_orders.as_slice())
    }

    /// Apply an exchange event to the cached state.
    pub fn apply_event(&mut self, event: &ExchangeEvent) {
        let exchange_name = event.exchange_name();
        let Some(state) = self.state.get_mut(exchange_name) else {
            return;
        };

        match event {
            ExchangeEvent::BookUpdate { orderbook, .. } => {
                state.orderbook = orderbook.clone();
            }
            ExchangeEvent::OrderUpdate { order, .. } => {
                // Update or insert the order in cached state
                if let Some(existing) = state.open_orders.iter_mut().find(|o| o.id == order.id) {
                    *existing = order.clone();
                } else {
                    state.open_orders.push(order.clone());
                }
                // Remove filled/cancelled orders from the open list
                state.open_orders.retain(|o| {
                    !matches!(
                        o.status,
                        OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
                    )
                });
            }
            _ => {}
        }
    }

    /// Re-fetch all state from all exchanges.
    pub async fn refresh_state(&mut self) -> Result<(), BotError> {
        let symbol = self.config.symbol.clone();
        for (name, exchange) in &self.exchanges {
            let state = self.state.get_mut(name).unwrap();
            state.orderbook = exchange.get_orderbook(&symbol, 20).await?;
            state.positions = exchange.get_positions().await?;
            state.balances = exchange.get_balances().await?;
            state.open_orders = exchange.get_open_orders(&symbol).await?;
        }
        Ok(())
    }

    /// Get mutable access to exchanges for subscribe/connect operations.
    pub(crate) fn exchanges_mut(&mut self) -> &mut HashMap<String, Box<dyn Exchange>> {
        &mut self.exchanges
    }
}

/// The strategy trait. Implement this to define trading logic.
///
/// All lifecycle methods return `Result` - non-fatal errors are logged
/// by the engine and execution continues. Fatal errors stop the bot.
#[async_trait]
pub trait Strategy: Send + 'static {
    /// Human-readable strategy name for logging.
    fn name(&self) -> &str;

    /// Called once after all exchanges are connected and subscribed.
    /// Use this to cancel stale orders, fetch initial state, etc.
    async fn on_start(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError>;

    /// Called on a configurable timer interval (the "heartbeat").
    /// Use for periodic rebalancing, health checks, P&L logging.
    async fn on_tick(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError>;

    /// Called for each exchange event (book update, fill, etc.).
    async fn on_event(
        &mut self,
        ctx: &mut StrategyContext,
        event: ExchangeEvent,
    ) -> Result<(), BotError>;

    /// Called when the bot is shutting down. Cancel all orders here.
    async fn on_stop(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError>;

    /// Return strategy-specific status for the HTTP status API.
    fn status(&self) -> serde_json::Value;
}
