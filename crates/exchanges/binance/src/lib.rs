//! Binance read-only reference price adapter.
//!
//! Unlike the bullet/hyperliquid adapters, this crate has no `Broker`: we
//! never place orders on Binance. It exists to surface a public reference
//! mid-price into the event bus so strategies (e.g. `reference-arb`) can
//! compare a Bullet-side price to a global fair-value signal.
//!
//! Single event type: [`ReferencePriceUpdate`] emitted from an [`MpscFeed`]
//! connected via [`connect_binance`].

pub mod feed;

pub use feed::{BinanceMarket, ReferencePriceUpdate, connect_binance};
pub use bb_core::harness::MpscFeed;
