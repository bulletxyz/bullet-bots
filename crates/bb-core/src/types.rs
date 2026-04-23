use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "buy"),
            Side::Sell => write!(f, "sell"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    PostOnly,
    Market,
}

impl std::fmt::Display for OrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderType::Limit => write!(f, "limit"),
            OrderType::PostOnly => write!(f, "post_only"),
            OrderType::Market => write!(f, "market"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct Level {
    pub price: Decimal,
    pub quantity: Decimal,
}

/// Order book with bids (descending by price) and asks (ascending by price).
#[derive(Debug, Clone, Default, Serialize)]
pub struct OrderBook {
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
    pub last_update_id: u64,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<Level> {
        self.bids.iter().next_back().map(|(&price, &quantity)| Level { price, quantity })
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks.iter().next().map(|(&price, &quantity)| Level { price, quantity })
    }

    pub fn midpoint(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / Decimal::from(2)),
            _ => None,
        }
    }

    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask.price - bid.price),
            _ => None,
        }
    }

    /// Would a PostOnly order at (`side`, `price`) cross the top of book?
    ///
    /// Returns `true` when a venue would reject the order as in-cross —
    /// buys at or above the best ask, sells at or below the best bid. When
    /// the relevant side of the book is empty we return `false` (no info =
    /// don't block placement; the venue is authoritative).
    pub fn would_cross(&self, side: Side, price: Decimal) -> bool {
        match side {
            Side::Buy => self.best_ask().is_some_and(|ask| price >= ask.price),
            Side::Sell => self.best_bid().is_some_and(|bid| price <= bid.price),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Order {
    pub id: String,
    pub client_id: Option<String>,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Decimal,
    pub quantity: Decimal,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
}

#[derive(Debug, Clone)]
pub struct NewOrder {
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Decimal,
    pub quantity: Decimal,
    pub client_id: Option<String>,
    pub reduce_only: bool,
}

#[derive(Debug, Clone)]
pub struct CancelOrder {
    pub symbol: String,
    pub order_id: String,
    /// Optional caller-assigned `ClientOrderId`. Adapters should prefer this
    /// when `order_id` is empty — useful for cancelling orders whose exchange
    /// `order_id` hasn't landed in the cache yet.
    pub client_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderResult {
    pub order_id: String,
    pub client_id: Option<String>,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CancelResult {
    pub order_id: String,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Position {
    pub symbol: String,
    pub side: Option<Side>,
    pub size: Decimal,
    pub entry_price: Decimal,
    pub unrealized_pnl: Decimal,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Balance {
    pub asset: String,
    pub available: Decimal,
    pub total: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orderbook_midpoint() {
        let mut book = OrderBook::default();
        book.bids.insert(Decimal::from(100), Decimal::from(1));
        book.asks.insert(Decimal::from(102), Decimal::from(1));
        assert_eq!(book.midpoint(), Some(Decimal::from(101)));
    }

    #[test]
    fn orderbook_spread() {
        let mut book = OrderBook::default();
        book.bids.insert(Decimal::from(100), Decimal::from(1));
        book.asks.insert(Decimal::from(102), Decimal::from(1));
        assert_eq!(book.spread(), Some(Decimal::from(2)));
    }

    #[test]
    fn orderbook_best_bid_ask() {
        let mut book = OrderBook::default();
        book.bids.insert(Decimal::from(99), Decimal::from(5));
        book.bids.insert(Decimal::from(100), Decimal::from(3));
        book.asks.insert(Decimal::from(101), Decimal::from(2));
        book.asks.insert(Decimal::from(102), Decimal::from(4));

        let best_bid = book.best_bid().unwrap();
        assert_eq!(best_bid.price, Decimal::from(100));
        assert_eq!(best_bid.quantity, Decimal::from(3));

        let best_ask = book.best_ask().unwrap();
        assert_eq!(best_ask.price, Decimal::from(101));
        assert_eq!(best_ask.quantity, Decimal::from(2));
    }

    #[test]
    fn empty_orderbook() {
        let book = OrderBook::default();
        assert!(book.best_bid().is_none());
        assert!(book.best_ask().is_none());
        assert!(book.midpoint().is_none());
        assert!(book.spread().is_none());
    }

    #[test]
    fn side_opposite() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
    }

    #[test]
    fn would_cross_buy_at_or_above_best_ask() {
        let mut book = OrderBook::default();
        book.bids.insert(Decimal::from(100), Decimal::from(1));
        book.asks.insert(Decimal::from(101), Decimal::from(1));
        assert!(book.would_cross(Side::Buy, Decimal::from(101)));
        assert!(book.would_cross(Side::Buy, Decimal::from(102)));
        assert!(!book.would_cross(Side::Buy, Decimal::from(100)));
    }

    #[test]
    fn would_cross_sell_at_or_below_best_bid() {
        let mut book = OrderBook::default();
        book.bids.insert(Decimal::from(100), Decimal::from(1));
        book.asks.insert(Decimal::from(101), Decimal::from(1));
        assert!(book.would_cross(Side::Sell, Decimal::from(100)));
        assert!(book.would_cross(Side::Sell, Decimal::from(99)));
        assert!(!book.would_cross(Side::Sell, Decimal::from(101)));
    }

    #[test]
    fn would_cross_empty_side_returns_false() {
        // Sparse books happen during reconnect / at startup — don't block
        // placement just because the relevant side isn't seeded yet.
        let mut only_bids = OrderBook::default();
        only_bids.bids.insert(Decimal::from(100), Decimal::from(1));
        assert!(!only_bids.would_cross(Side::Buy, Decimal::from(1_000_000)));

        let mut only_asks = OrderBook::default();
        only_asks.asks.insert(Decimal::from(100), Decimal::from(1));
        assert!(!only_asks.would_cross(Side::Sell, Decimal::from(1)));
    }
}
