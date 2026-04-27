//! Core framework for bullet-bots.
//!
//! Build a trading bot by wiring together [`HarnessBuilder`]:
//! feeds (market-data producers), actors (strategy consumers), and
//! brokers (order-placement handles). The harness drives the
//! init → event-loop → wind-down lifecycle and guarantees clean shutdown.
//!
//! # Quick start
//!
//! ```ignore
//! use std::sync::Arc;
//! use bb_core::prelude::*;
//!
//! let harness = HarnessBuilder::new()
//!     .enable_signal_shutdown()
//!     .wire_broker("venue", Arc::new(my_broker))
//!     .wire_feed_named("trades", my_trade_feed)
//!     .wire_feed_named("ticks",  TickFeed::every_ms(5_000))
//!     .wire_actor(ActorSpec::new("strategy", my_actor).sub_critical::<Trade>().sub::<Tick>())
//!     .build()?;
//! harness.run().await?;
//! ```
//!
//! See [`harness`] for a detailed walkthrough of all four concepts.

pub mod broker;
pub mod config;
pub mod error;
pub mod events;
pub mod harness;
pub mod helpers;
pub mod types;

/// Convenience re-exports for the most commonly used framework types.
pub mod prelude {
    pub use crate::error::BotError;
    pub use crate::events::{BookUpdate, MarkPriceUpdate, OrderLifecycle, Tick, Trade};
    pub use crate::harness::{ActorSpec, HarnessBuilder};
    pub use crate::helpers::{ClientIdIssuer, InventoryTracker, TickFeed};
}
