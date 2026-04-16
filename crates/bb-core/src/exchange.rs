use async_trait::async_trait;

use crate::error::BotError;
use crate::types::*;

/// Unified exchange interface. Adapters implement this to connect any exchange.
///
/// Each exchange adapter encapsulates REST + WebSocket connectivity,
/// type conversion, and authentication for a specific venue.
#[async_trait]
pub trait Exchange: Send + Sync + 'static {
    /// Human-readable exchange name (e.g., "bullet", "hyperliquid").
    fn name(&self) -> &str;

    /// Connect to the exchange: authenticate, load instruments, etc.
    async fn connect(&mut self) -> Result<(), BotError>;

    /// Fetch a snapshot of the orderbook for a symbol.
    async fn get_orderbook(&self, symbol: &str, depth: usize) -> Result<OrderBook, BotError>;

    /// Fetch account balances.
    async fn get_balances(&self) -> Result<Vec<Balance>, BotError>;

    /// Fetch open positions.
    async fn get_positions(&self) -> Result<Vec<Position>, BotError>;

    /// Fetch open orders for a symbol.
    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>, BotError>;

    /// Place one or more orders. Returns a result per order.
    async fn place_orders(&self, orders: &[NewOrder]) -> Result<Vec<OrderResult>, BotError>;

    /// Cancel one or more orders by ID.
    async fn cancel_orders(&self, cancels: &[CancelOrder]) -> Result<Vec<CancelResult>, BotError>;

    /// Cancel all open orders for a symbol.
    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), BotError>;

    /// Subscribe to real-time market data and private order updates for a symbol.
    async fn subscribe(&mut self, symbol: &str) -> Result<(), BotError>;

    /// Receive the next event from the exchange.
    /// Returns `None` if the stream is closed (engine will reconnect).
    async fn recv_event(&mut self) -> Option<ExchangeEvent>;
}
