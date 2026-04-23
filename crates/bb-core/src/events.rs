//! Canonical event types that flow through the harness bus.
//!
//! Split guarantees one canonical source per kind of fact:
//!
//! - **`Trade`** — an execution. Adapters emit one `Trade` per fill. This is
//!   the *only* event strategies use to update position/PnL. Double-counting
//!   bugs are structurally impossible: if a Bullet `OrderUpdateData::TradeFill`
//!   arrives, the adapter emits a `Trade` (for inventory) and a
//!   `OrderLifecycle` (for reconcile) as independent events. An HL `UserFills`
//!   message produces a `Trade`; the parallel `OrderUpdates` message produces
//!   only lifecycle info, never a duplicate `Trade`.
//!
//! - **`OrderLifecycle`** — status transitions (Open → PartiallyFilled →
//!   Filled / Cancelled / Rejected). Used by strategies for reconcile: "is
//!   this order still resting?", "did my cancel go through?". Never used for
//!   position updates.
//!
//! - **`BookUpdate`** — orderbook state. Strategies read the latest for
//!   pricing decisions.
//!
//! - **`MarkPriceUpdate`** — mark price + funding rate from the venue.
//!
//! - **`Tick`** — periodic heartbeat. Produced by a framework-provided
//!   `TickFeed` so periodic work fits the same event model as everything else.

use std::time::Instant;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::types::{Order, OrderBook, Side};

/// A single execution against our account.
///
/// Deliberately does not carry a maker/taker flag: not every venue reports
/// it reliably on the fill stream, and no current strategy needs it for
/// position tracking. Fee accounting — the obvious future consumer — should
/// derive maker/taker from the order we placed, which the actor already
/// tracks via its `order_type`.
#[derive(Debug, Clone, Serialize)]
pub struct Trade {
    pub exchange: String,
    pub symbol: String,
    pub order_id: String,
    /// Set if the order was placed with a caller-assigned client id.
    pub client_id: Option<String>,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
}

/// Status transition for one of our orders.
#[derive(Debug, Clone, Serialize)]
pub struct OrderLifecycle {
    pub exchange: String,
    pub order: Order,
}

/// Orderbook state.
#[derive(Debug, Clone, Serialize)]
pub struct BookUpdate {
    pub exchange: String,
    pub symbol: String,
    pub orderbook: OrderBook,
}

/// Mark price and/or funding rate.
#[derive(Debug, Clone, Serialize)]
pub struct MarkPriceUpdate {
    pub exchange: String,
    pub symbol: String,
    pub mark_price: Decimal,
    pub funding_rate: Decimal,
}

/// Periodic heartbeat. `at` is monotonic time.
#[derive(Debug, Clone)]
pub struct Tick {
    pub at: Instant,
}
