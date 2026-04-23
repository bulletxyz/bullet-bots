//! Static-grid state: fixed level prices indexed `0..num_levels`, tracked
//! through a three-state lifecycle (`Dormant` → `Pending` → `Active` →
//! `Dormant`).
//!
//! Invariants:
//!   - Level prices are set once in [`compute_levels`] and never move.
//!   - At any time, at most one of {buy, sell} rests at a given level.
//!   - `Dormant` = the level has no resting order. It becomes `Pending` when
//!     the *adjacent* level fills ("a buy at N fills → re-arm level N+1 as a
//!     sell" and symmetric) or, for a freshly computed grid, when the actor
//!     first populates levels from config at startup.
//!   - Profit per round trip = `spacing × order_size`; the actor's
//!     `InventoryTracker` records it on each fill cycle.

use std::time::Instant;

use bb_core::types::Side;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;

use crate::config::{GridConfig, TrendFilterConfig};
use crate::ema::Ema;

/// Per-level lifecycle. Flat enum keeps match sites readable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum LevelState {
    /// No resting order here. Re-armed on an adjacent fill.
    Dormant,
    /// Needs placement on the next `place_pending_orders` pass.
    Pending,
    /// Order is live on the exchange.
    Active,
}

#[derive(Debug, Clone, Serialize)]
pub struct GridLevel {
    pub index: usize,
    pub price: Decimal,
    /// `None` only in `Dormant`. When `Pending` or `Active`, this is the
    /// side of the resting (or to-be-placed) order. Fill events flip this.
    pub side: Option<Side>,
    pub state: LevelState,
    pub client_id: Option<String>,
    pub order_id: Option<String>,
}

impl GridLevel {
    fn pending(index: usize, price: Decimal, side: Side) -> Self {
        Self {
            index,
            price,
            side: Some(side),
            state: LevelState::Pending,
            client_id: None,
            order_id: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct GridState {
    pub levels: Vec<GridLevel>,
    fast_ema: Option<Ema>,
    slow_ema: Option<Ema>,
    pub paused: bool,
}

impl GridState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable trend-filter EMAs at construction time.
    pub fn with_trend_filter(mut self, cfg: &TrendFilterConfig) -> Self {
        self.fast_ema = Some(Ema::new(cfg.fast_secs as f64));
        self.slow_ema = Some(Ema::new(cfg.slow_secs as f64));
        self
    }

    /// Populate `self.levels` from the config, using `anchor` to divide
    /// levels into initial buys (≤ anchor) and sells (> anchor). Levels
    /// *at* the anchor price are treated as buys — the rule is a strict
    /// inequality on the sell side, so an operator can place the anchor at
    /// any grid line and the bias is explicit.
    pub fn compute_levels(&mut self, anchor: Decimal, config: &GridConfig) {
        let spacing = config.spacing();
        self.levels = (0..config.num_levels)
            .map(|i| {
                let price = config.lower_price + spacing * Decimal::from(i as u64);
                let side = if price <= anchor { Side::Buy } else { Side::Sell };
                GridLevel::pending(i, price, side)
            })
            .collect();
    }

    /// Find the level that matches an incoming fill. Falls back from
    /// `client_id` to `order_id` since Bullet doesn't return oids synchronously.
    pub fn find_fill_target(
        &mut self,
        client_id: Option<&str>,
        order_id: &str,
    ) -> Option<usize> {
        self.levels.iter().position(|l| {
            if l.state != LevelState::Active {
                return false;
            }
            match client_id {
                Some(cid) => l.client_id.as_deref() == Some(cid),
                None => l.order_id.as_deref() == Some(order_id),
            }
        })
    }

    /// Mark the filled level as dormant (no resting order, no side) and
    /// return the index of the *adjacent* level to re-arm plus the side
    /// to place there. Returns `None` when the filled level sits at the
    /// edge of the grid and the adjacent position doesn't exist.
    pub fn on_fill(&mut self, idx: usize, filled_side: Side) -> Option<(usize, Side)> {
        let lvl = self.levels.get_mut(idx)?;
        lvl.state = LevelState::Dormant;
        lvl.side = None;
        lvl.client_id = None;
        lvl.order_id = None;

        let replacement_side = filled_side.opposite();
        let target_idx = match filled_side {
            Side::Buy => idx.checked_add(1)?,
            Side::Sell => idx.checked_sub(1)?,
        };
        if target_idx >= self.levels.len() {
            return None;
        }
        let target = &mut self.levels[target_idx];
        // Only re-arm if the adjacent level is dormant. A level already
        // resting with an order (either side) is left alone: for an
        // already-matching side we'd be placing a duplicate; for an opposite
        // side we'd self-cross. Either way, skip.
        if target.state != LevelState::Dormant {
            return None;
        }
        target.state = LevelState::Pending;
        target.side = Some(replacement_side);
        Some((target_idx, replacement_side))
    }

    /// Update trend-filter EMAs from a fresh mid observation and return the
    /// current `(divergence_bps, paused)`. Noop / returns `(0, paused)` when
    /// the filter isn't configured.
    pub fn update_trend_filter(
        &mut self,
        mid: Decimal,
        now: Instant,
        cfg: &TrendFilterConfig,
    ) -> (f64, bool) {
        let Some(sample) = mid.to_f64() else {
            return (0.0, self.paused);
        };
        let fast = self.fast_ema.get_or_insert_with(|| Ema::new(cfg.fast_secs as f64));
        let f = fast.update(sample, now);
        let slow = self.slow_ema.get_or_insert_with(|| Ema::new(cfg.slow_secs as f64));
        let s = slow.update(sample, now);
        if s == 0.0 {
            return (0.0, self.paused);
        }
        let divergence_bps = (f - s) / s * 10_000.0;
        let threshold = cfg.pause_divergence_bps.to_f64().unwrap_or(f64::INFINITY);
        self.paused = divergence_bps.abs() > threshold;
        (divergence_bps, self.paused)
    }

    pub fn active_count(&self) -> usize {
        self.levels.iter().filter(|l| l.state == LevelState::Active).count()
    }

    /// Check if a new order on `side` would push past `max_position`.
    pub fn at_max_position(
        net_position: Decimal,
        side: Side,
        max_position: Decimal,
    ) -> bool {
        match side {
            Side::Buy => net_position >= max_position,
            Side::Sell => net_position <= -max_position,
        }
    }

    /// Pause placement and drop every level back to dormant. Called when
    /// the trend filter first trips so we fully disengage.
    pub fn suspend_all(&mut self) {
        self.paused = true;
        for l in &mut self.levels {
            l.state = LevelState::Dormant;
            l.side = None;
            l.client_id = None;
            l.order_id = None;
        }
    }

    /// Resume from a trend-filter pause: re-arm every dormant level with its
    /// bias-determined initial side. Active levels (if any survived) keep
    /// their state.
    pub fn resume(&mut self, anchor: Decimal) {
        self.paused = false;
        for l in &mut self.levels {
            if l.state == LevelState::Dormant {
                let side = if l.price <= anchor { Side::Buy } else { Side::Sell };
                l.state = LevelState::Pending;
                l.side = Some(side);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bb_core::types::OrderType;

    fn cfg(lower: i64, upper: i64, n: usize) -> GridConfig {
        GridConfig {
            exchange: "bullet".into(),
            symbol: "BTC-USD".into(),
            lower_price: Decimal::from(lower),
            upper_price: Decimal::from(upper),
            num_levels: n,
            anchor_price: None,
            order_size: Decimal::ONE,
            order_type: OrderType::PostOnly,
            max_position: Decimal::from(100),
            fees: None,
            trend_filter: None,
        }
    }

    #[test]
    fn compute_levels_uniform_spacing() {
        let mut s = GridState::new();
        let c = cfg(70, 80, 11); // 11 levels, spacing $1
        s.compute_levels(Decimal::from(75), &c);
        assert_eq!(s.levels.len(), 11);
        assert_eq!(s.levels[0].price, Decimal::from(70));
        assert_eq!(s.levels[10].price, Decimal::from(80));
        assert_eq!(c.spacing(), Decimal::ONE);
    }

    #[test]
    fn compute_levels_bias_by_anchor() {
        let mut s = GridState::new();
        let c = cfg(70, 80, 11);
        // Anchor at 72 → levels 70,71,72 are buys (3); 73..80 are sells (8). Short bias.
        s.compute_levels(Decimal::from(72), &c);
        let buys = s.levels.iter().filter(|l| l.side == Some(Side::Buy)).count();
        let sells = s.levels.iter().filter(|l| l.side == Some(Side::Sell)).count();
        assert_eq!(buys, 3);
        assert_eq!(sells, 8);
    }

    #[test]
    fn anchor_below_range_makes_all_sells() {
        let mut s = GridState::new();
        let c = cfg(70, 80, 11);
        s.compute_levels(Decimal::from(50), &c);
        assert!(s.levels.iter().all(|l| l.side == Some(Side::Sell)));
    }

    #[test]
    fn on_fill_buy_rearms_dormant_level_above() {
        let mut s = GridState::new();
        let c = cfg(70, 80, 11);
        s.compute_levels(Decimal::from(75), &c);
        // Simulate level 5 (price 75) going Active, then filling. Also
        // mark level 6 dormant so the re-arm has somewhere to land.
        s.levels[5].state = LevelState::Active;
        s.levels[6].state = LevelState::Dormant;
        s.levels[6].side = None;
        let rearm = s.on_fill(5, Side::Buy);
        assert_eq!(rearm, Some((6, Side::Sell)));
        assert_eq!(s.levels[5].state, LevelState::Dormant);
        assert_eq!(s.levels[5].side, None);
        assert_eq!(s.levels[6].state, LevelState::Pending);
        assert_eq!(s.levels[6].side, Some(Side::Sell));
    }

    #[test]
    fn on_fill_sell_rearms_level_below() {
        let mut s = GridState::new();
        let c = cfg(70, 80, 11);
        s.compute_levels(Decimal::from(75), &c);
        s.levels[7].state = LevelState::Active;
        // Dormant the one below to prove re-arm fires only when dormant.
        s.levels[6].state = LevelState::Dormant;
        s.levels[6].side = None;
        let rearm = s.on_fill(7, Side::Sell);
        assert_eq!(rearm, Some((6, Side::Buy)));
    }

    #[test]
    fn on_fill_skips_when_adjacent_is_not_dormant() {
        let mut s = GridState::new();
        let c = cfg(70, 80, 11);
        s.compute_levels(Decimal::from(75), &c);
        // Level 6 starts as Sell/Pending from compute_levels. Don't re-arm.
        s.levels[5].state = LevelState::Active;
        let rearm = s.on_fill(5, Side::Buy);
        assert_eq!(rearm, None);
        // But level 5 still became dormant.
        assert_eq!(s.levels[5].state, LevelState::Dormant);
    }

    #[test]
    fn on_fill_at_top_edge_has_no_rearm() {
        let mut s = GridState::new();
        let c = cfg(70, 80, 11);
        s.compute_levels(Decimal::from(75), &c);
        s.levels[10].state = LevelState::Active;
        let rearm = s.on_fill(10, Side::Buy);
        assert_eq!(rearm, None);
    }

    #[test]
    fn full_round_trip_generates_complementary_rearm() {
        let mut s = GridState::new();
        let c = cfg(74, 78, 5); // levels 74..78 spacing 1
        s.compute_levels(Decimal::new(755, 1), &c); // anchor 75.5
        // Init: 74,75 = Buy/Pending; 76,77,78 = Sell/Pending.
        s.levels[1].state = LevelState::Active; // 75 buy goes live
        s.levels[2].state = LevelState::Active; // 76 sell goes live
        // Buy at 75 fills → re-arm 76 to sell (already Pending? no, Active). Skip.
        assert_eq!(s.on_fill(1, Side::Buy), None);
        assert_eq!(s.levels[1].state, LevelState::Dormant);
        // Then sell at 76 fills → re-arm 75 as buy (now dormant).
        assert_eq!(s.on_fill(2, Side::Sell), Some((1, Side::Buy)));
        assert_eq!(s.levels[1].state, LevelState::Pending);
        assert_eq!(s.levels[1].side, Some(Side::Buy));
    }

    #[test]
    fn max_position_check() {
        assert!(GridState::at_max_position(Decimal::from(5), Side::Buy, Decimal::from(5)));
        assert!(!GridState::at_max_position(Decimal::from(5), Side::Sell, Decimal::from(5)));
    }

    #[test]
    fn suspend_all_clears_state() {
        let mut s = GridState::new();
        let c = cfg(70, 80, 11);
        s.compute_levels(Decimal::from(75), &c);
        s.levels[3].state = LevelState::Active;
        s.suspend_all();
        assert!(s.paused);
        assert!(s.levels.iter().all(|l| l.state == LevelState::Dormant));
    }

    #[test]
    fn resume_re_arms_by_anchor_bias() {
        let mut s = GridState::new();
        let c = cfg(70, 80, 11);
        s.compute_levels(Decimal::from(75), &c);
        s.suspend_all();
        s.resume(Decimal::from(72));
        assert!(!s.paused);
        let buys = s.levels.iter().filter(|l| l.side == Some(Side::Buy)).count();
        assert_eq!(buys, 3);
    }
}
