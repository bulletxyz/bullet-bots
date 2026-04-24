//! Binance read-only reference price adapter.
//!
//! Unlike the bullet/hyperliquid adapters, this crate has no `Broker`: we
//! never place orders on Binance. It exists to surface a public reference
//! mid-price into the event bus so strategies (e.g. `reference-arb`) can
//! compare a Bullet-side price to a global fair-value signal.
//!
//! Single event type: [`ReferencePriceUpdate`] emitted from a single feed
//! [`BinanceReferencePriceFeed`] connected via [`connect_binance`].

pub mod feed;

pub use feed::{BinanceMarket, BinanceReferencePriceFeed, ReferencePriceUpdate, connect_binance};
