//! Simple starter market maker.
//!
//! This crate is intentionally boring: one bid, one ask, fixed spreads around
//! the current mid, periodic refresh, and a hard inventory cap. It exists as
//! the easiest strategy to copy when learning the harness.

pub mod config;
pub mod strategy;

pub use config::SimpleMmConfig;
pub use strategy::SimpleMmActor;
