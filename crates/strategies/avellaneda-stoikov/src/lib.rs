//! Avellaneda-Stoikov market-making strategy.
//!
//! Quotes a bid/ask ladder around a reservation price that shifts with
//! inventory. Follows the classic finite-horizon A-S closed form (2008) as
//! implemented by Hummingbot's `avellaneda_market_making`, with standard
//! production tricks: volatility from a rolling window, min/max spread
//! clamps, fee-floor guardrail, and a hard `max_position` backstop.

pub mod config;
pub mod model;
pub mod strategy;
pub mod volatility;

pub use config::AvellanedaStoikovConfig;
pub use strategy::AvellanedaStoikovActor;
