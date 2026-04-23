//! Reusable building blocks for strategy implementations. These are plain
//! types — no framework magic — so strategies can drop them in without
//! buying the whole harness if they want.

pub mod client_id;
pub mod inventory;
pub mod tick_feed;

pub use client_id::ClientIdIssuer;
pub use inventory::InventoryTracker;
pub use tick_feed::TickFeed;
