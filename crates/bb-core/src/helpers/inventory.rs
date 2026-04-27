//! Shared inventory / realized-PnL tracker. Strategies call `record_fill` from
//! their `EventHandler<Trade>` impl; the tracker maintains net position,
//! weighted-average entry price, and realized `PnL` using the standard
//! open-closed split.
//!
//! Units: base-asset quantity, quote-asset price, quote-asset realized `PnL`.
//! Buys increase position; sells decrease. Crossing zero splits a fill into
//! a closing-portion (realized against prior avg entry) and an opening-portion
//! (becomes the new avg entry).

use rust_decimal::Decimal;
use serde::Serialize;

use crate::types::{Position, Side};

#[derive(Debug, Clone, Default, Serialize)]
pub struct InventoryTracker {
    pub net_position: Decimal,
    /// Volume-weighted average entry price of the open portion. Zero when flat.
    pub avg_entry_price: Decimal,
    pub realized_pnl: Decimal,
    pub total_fills: u64,
}

impl InventoryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a fill. Returns the realized `PnL` contribution from this fill
    /// (nonzero only when the fill closes or partially closes an existing
    /// position).
    pub fn record_fill(&mut self, side: Side, price: Decimal, quantity: Decimal) -> Decimal {
        self.total_fills += 1;
        let signed_qty = match side {
            Side::Buy => quantity,
            Side::Sell => -quantity,
        };

        // Flat → opening a new position.
        if self.net_position.is_zero() {
            self.net_position = signed_qty;
            self.avg_entry_price = price;
            return Decimal::ZERO;
        }

        let same_direction = self.net_position.is_sign_positive() == signed_qty.is_sign_positive();
        if same_direction {
            // Adding to an existing position — weighted-average the entry price.
            let old_abs = self.net_position.abs();
            let add_abs = signed_qty.abs();
            let new_abs = old_abs + add_abs;
            self.avg_entry_price = (self.avg_entry_price * old_abs + price * add_abs) / new_abs;
            self.net_position += signed_qty;
            return Decimal::ZERO;
        }

        // Opposite direction — closing (possibly flipping).
        let close_abs = self.net_position.abs().min(signed_qty.abs());
        let pnl_per_unit = if self.net_position.is_sign_positive() {
            price - self.avg_entry_price // closing long: sell high
        } else {
            self.avg_entry_price - price // closing short: buy low
        };
        let realized = pnl_per_unit * close_abs;
        self.realized_pnl += realized;

        let remaining = signed_qty.abs() - close_abs;
        self.net_position += signed_qty;
        if self.net_position.is_zero() {
            self.avg_entry_price = Decimal::ZERO;
        } else if remaining > Decimal::ZERO {
            // Flipped — the leftover opens a new position at the fill price.
            self.avg_entry_price = price;
        }
        realized
    }

    pub fn is_flat(&self) -> bool {
        self.net_position.is_zero()
    }

    /// Seed the tracker from a venue-reported position, typically at strategy
    /// startup after reconnecting/restarting with existing exposure.
    pub fn seed_from_position(&mut self, position: &Position) {
        self.net_position = match position.side {
            Some(Side::Buy) => position.size,
            Some(Side::Sell) => -position.size,
            None => Decimal::ZERO,
        };
        self.avg_entry_price =
            if self.net_position.is_zero() { Decimal::ZERO } else { position.entry_price };
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::prelude::FromPrimitive;

    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn open_long_no_pnl() {
        let mut inv = InventoryTracker::new();
        let pnl = inv.record_fill(Side::Buy, d("100"), d("1"));
        assert_eq!(pnl, Decimal::ZERO);
        assert_eq!(inv.net_position, d("1"));
        assert_eq!(inv.avg_entry_price, d("100"));
    }

    #[test]
    fn close_long_at_profit() {
        let mut inv = InventoryTracker::new();
        inv.record_fill(Side::Buy, d("100"), d("1"));
        let pnl = inv.record_fill(Side::Sell, d("110"), d("1"));
        assert_eq!(pnl, d("10"));
        assert!(inv.is_flat());
        assert_eq!(inv.realized_pnl, d("10"));
        assert_eq!(inv.avg_entry_price, Decimal::ZERO);
    }

    #[test]
    fn scale_in_weighted_average() {
        let mut inv = InventoryTracker::new();
        inv.record_fill(Side::Buy, d("100"), d("1"));
        inv.record_fill(Side::Buy, d("110"), d("1"));
        // avg = (100*1 + 110*1) / 2 = 105
        assert_eq!(inv.avg_entry_price, d("105"));
        assert_eq!(inv.net_position, d("2"));
    }

    #[test]
    fn partial_close_keeps_residual() {
        let mut inv = InventoryTracker::new();
        inv.record_fill(Side::Buy, d("100"), d("2"));
        let pnl = inv.record_fill(Side::Sell, d("110"), d("1"));
        assert_eq!(pnl, d("10"));
        assert_eq!(inv.net_position, d("1"));
        // Avg entry unchanged by partial close.
        assert_eq!(inv.avg_entry_price, d("100"));
    }

    #[test]
    fn flip_splits_into_close_then_open() {
        let mut inv = InventoryTracker::new();
        inv.record_fill(Side::Buy, d("100"), d("1"));
        // Sell 2 at 110 → closes long with +10, opens short 1 @ 110.
        let pnl = inv.record_fill(Side::Sell, d("110"), d("2"));
        assert_eq!(pnl, d("10"));
        assert_eq!(inv.net_position, d("-1"));
        assert_eq!(inv.avg_entry_price, d("110"));
    }

    #[test]
    fn short_round_trip() {
        let mut inv = InventoryTracker::new();
        inv.record_fill(Side::Sell, d("110"), d("1"));
        let pnl = inv.record_fill(Side::Buy, d("100"), d("1"));
        assert_eq!(pnl, d("10"));
        assert!(inv.is_flat());
    }

    #[test]
    fn sanity_on_f64_roundtrip() {
        // Regression: avg calc must not blow up on non-round decimals.
        let mut inv = InventoryTracker::new();
        inv.record_fill(Side::Buy, Decimal::from_f64(100.5).unwrap(), d("0.5"));
        inv.record_fill(Side::Buy, Decimal::from_f64(101.5).unwrap(), d("0.5"));
        assert_eq!(inv.avg_entry_price, Decimal::from_f64(101.0).unwrap());
    }

    #[test]
    fn seed_from_position_sets_signed_inventory() {
        let mut inv = InventoryTracker::new();
        inv.seed_from_position(&Position {
            symbol: "BTC-USD".into(),
            side: Some(Side::Sell),
            size: d("0.5"),
            entry_price: d("100"),
            unrealized_pnl: Decimal::ZERO,
        });

        assert_eq!(inv.net_position, d("-0.5"));
        assert_eq!(inv.avg_entry_price, d("100"));
    }
}
