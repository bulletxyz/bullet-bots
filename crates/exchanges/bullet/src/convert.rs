//! Shared value-conversion helpers used by the Bullet broker + connection.

use std::collections::BTreeMap;
use std::str::FromStr;

use bullet_rust_sdk::types::PriceLevel;
use rust_decimal::Decimal;

/// Parse a `[price, qty]` price level into typed decimals. Returns `None` if
/// either string fails to parse.
pub fn parse_level_tuple(level: &PriceLevel) -> Option<(Decimal, Decimal)> {
    let price = Decimal::from_str(&level.0).ok()?;
    let qty = Decimal::from_str(&level.1).ok()?;
    Some((price, qty))
}

/// Parse a REST orderbook (`Vec<Vec<String>>`) into a `BTreeMap<Decimal, Decimal>`.
pub fn parse_orderbook_levels(raw: &[Vec<String>]) -> BTreeMap<Decimal, Decimal> {
    raw.iter()
        .filter_map(|lvl| {
            let price = Decimal::from_str(lvl.first()?).ok()?;
            let qty = Decimal::from_str(lvl.get(1)?).ok()?;
            Some((price, qty))
        })
        .collect()
}
