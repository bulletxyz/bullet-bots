//! Reusable building blocks for strategy implementations. These are plain
//! types — no framework magic — so strategies can drop them in without
//! buying the whole harness if they want.

pub mod client_id;
pub mod inventory;
pub mod parse;
pub mod recent_ids;
pub mod tick_feed;

pub use client_id::ClientIdIssuer;
pub use inventory::InventoryTracker;
pub use parse::parse_decimal_or_warn;
pub use recent_ids::RecentIds;
pub use tick_feed::TickFeed;
