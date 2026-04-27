//! Parsing helpers for use at exchange adapter boundaries.

use rust_decimal::Decimal;

/// Parse a decimal string, logging a warning and returning `None` on failure.
///
/// Use this for numeric fields arriving as strings from venue APIs. Callers
/// decide how to handle `None`:
/// - For critical fields (price, qty): skip the event entirely.
/// - For supplemental fields (funding_rate): substitute a sentinel like
///   `Decimal::ZERO`, knowing the warning has already fired.
pub fn parse_decimal_or_warn(s: &str, field: &str) -> Option<Decimal> {
    match s.parse() {
        Ok(d) => Some(d),
        Err(_) => {
            tracing::warn!(raw = s, field, "failed to parse decimal — skipping");
            None
        }
    }
}
