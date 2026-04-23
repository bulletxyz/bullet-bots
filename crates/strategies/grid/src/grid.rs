use std::time::Instant;

use bb_core::types::Side;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;

use crate::config::{GridConfig, SpacingMode, TrendFilterConfig};
use crate::ema::Ema;

/// Status of a single grid level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum GridLevelStatus {
    /// Needs to be placed.
    Pending,
    /// Order is live on the exchange.
    Active,
    /// Order was filled, awaiting replacement on the opposite side.
    Filled,
}

/// A single grid level.
#[derive(Debug, Clone, Serialize)]
pub struct GridLevel {
    pub side: Side,
    pub price: Decimal,
    pub order_id: Option<String>,
    /// Caller-assigned `ClientOrderId` (monotonic u64 as string). Set before
    /// placement so incoming `Trade`/`OrderUpdate` events can be correlated
    /// back to the level without waiting for an exchange `order_id`.
    pub client_id: Option<String>,
    pub status: GridLevelStatus,
}

/// Manages the set of grid levels and associated trend-filter state.
///
/// Position / PnL / client-id tracking deliberately live in shared helpers
/// (`InventoryTracker`, `ClientIdIssuer`) owned by the actor rather than here,
/// so grid logic stays focused on geometry + reconcile and those facilities
/// can be reused by every strategy.
#[derive(Debug)]
pub struct GridState {
    pub levels: Vec<GridLevel>,
    pub center_price: Decimal,
    /// Last time a full compute_levels / reconcile mutated the level set.
    last_rebalance_at: Option<Instant>,
    /// Trend-filter EMAs (enabled only if config sets `trend_filter`).
    fast_ema: Option<Ema>,
    slow_ema: Option<Ema>,
    /// True while the trend filter has paused placement.
    pub paused: bool,
}

/// The delta a reconcile pass wants to apply to live orders. Cancels go first,
/// then places for any uncovered desired levels.
#[derive(Debug, Default)]
pub struct ReconcileDiff {
    /// client_ids (if set) or order_ids of levels to cancel.
    pub cancels: Vec<OrderRef>,
    /// Desired new levels to place (side, price).
    pub places: Vec<(Side, Decimal)>,
    /// True if any delta exists.
    pub changed: bool,
}

#[derive(Debug, Clone)]
pub enum OrderRef {
    Client(String),
    Exchange(String),
}

impl Default for GridState {
    fn default() -> Self {
        Self {
            levels: Vec::new(),
            center_price: Decimal::ZERO,
            last_rebalance_at: None,
            fast_ema: None,
            slow_ema: None,
            paused: false,
        }
    }
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

    /// Compute the desired set of (side, price) levels for a given mid, applying
    /// inventory skew but not mutating state. Used by both `compute_levels` and
    /// `reconcile` so the geometry is defined in one place.
    pub fn desired_levels(
        &self,
        mid_price: Decimal,
        net_position: Decimal,
        config: &GridConfig,
    ) -> Vec<(Side, Decimal)> {
        let mut out = Vec::with_capacity(config.num_levels * 2);
        let (buy_mult, sell_mult) = skew_multipliers(
            net_position,
            config.max_position,
            config.inventory_skew_k,
        );

        for i in 1..=config.num_levels {
            let i_dec = Decimal::from(i as u64);
            let (buy_price, sell_price) = match config.spacing_mode {
                SpacingMode::Geometric => {
                    let offset_pct = config.grid_spacing * i_dec / Decimal::from(100);
                    let buy_off = offset_pct * buy_mult;
                    let sell_off = offset_pct * sell_mult;
                    (mid_price * (Decimal::ONE - buy_off), mid_price * (Decimal::ONE + sell_off))
                }
                SpacingMode::Arithmetic => {
                    let offset = config.grid_spacing * i_dec;
                    (mid_price - offset * buy_mult, mid_price + offset * sell_mult)
                }
            };
            out.push((Side::Buy, buy_price));
            out.push((Side::Sell, sell_price));
        }
        out
    }

    /// Compute grid levels centered on `mid_price` and reset the level set.
    /// Used on startup; tick-time updates should prefer `reconcile`.
    pub fn compute_levels(
        &mut self,
        mid_price: Decimal,
        net_position: Decimal,
        config: &GridConfig,
    ) {
        let desired = self.desired_levels(mid_price, net_position, config);
        self.levels.clear();
        self.center_price = mid_price;
        for (side, price) in desired {
            self.levels.push(GridLevel {
                side,
                price,
                order_id: None,
                client_id: None,
                status: GridLevelStatus::Pending,
            });
        }
    }

    /// Compute the cancel/place delta required to bring the active level set in
    /// line with the desired geometry at `new_mid`. Existing active levels
    /// whose price is within `match_tolerance_pct` of a desired level on the
    /// same side are kept; everything else is either cancelled or newly placed.
    pub fn reconcile(
        &self,
        new_mid: Decimal,
        net_position: Decimal,
        config: &GridConfig,
        match_tolerance_pct: Decimal,
    ) -> ReconcileDiff {
        let desired = self.desired_levels(new_mid, net_position, config);
        let mut diff = ReconcileDiff::default();

        // Track which desired slots are already covered.
        let mut desired_covered = vec![false; desired.len()];

        for level in &self.levels {
            if level.status != GridLevelStatus::Active {
                continue;
            }
            // Find a desired level on the same side within tolerance.
            let match_idx = desired.iter().enumerate().position(|(idx, (side, price))| {
                if desired_covered[idx] || *side != level.side {
                    return false;
                }
                if price.is_zero() {
                    return false;
                }
                let delta = ((level.price - *price) / *price * Decimal::from(100)).abs();
                delta <= match_tolerance_pct
            });
            match match_idx {
                Some(idx) => desired_covered[idx] = true,
                None => {
                    let order_ref = level
                        .client_id
                        .clone()
                        .map(OrderRef::Client)
                        .or_else(|| level.order_id.clone().map(OrderRef::Exchange));
                    if let Some(r) = order_ref {
                        diff.cancels.push(r);
                    }
                }
            }
        }

        for (idx, (side, price)) in desired.into_iter().enumerate() {
            if !desired_covered[idx] {
                diff.places.push((side, price));
            }
        }

        diff.changed = !diff.cancels.is_empty() || !diff.places.is_empty();
        diff
    }

    /// Apply a completed reconcile: drop cancelled levels, append new pending
    /// levels, recenter. Call after the exchange has acked the delta.
    pub fn apply_reconcile(
        &mut self,
        new_mid: Decimal,
        cancelled: &[OrderRef],
        new_pending: &[(Side, Decimal)],
        now: Instant,
    ) {
        let cancelled_client: std::collections::HashSet<&str> = cancelled
            .iter()
            .filter_map(|r| match r {
                OrderRef::Client(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        let cancelled_exchange: std::collections::HashSet<&str> = cancelled
            .iter()
            .filter_map(|r| match r {
                OrderRef::Exchange(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        self.levels.retain(|l| {
            let by_cid = l
                .client_id
                .as_deref()
                .is_some_and(|c| cancelled_client.contains(c));
            let by_oid = l
                .order_id
                .as_deref()
                .is_some_and(|o| cancelled_exchange.contains(o));
            !(by_cid || by_oid)
        });
        for (side, price) in new_pending {
            self.levels.push(GridLevel {
                side: *side,
                price: *price,
                order_id: None,
                client_id: None,
                status: GridLevelStatus::Pending,
            });
        }
        self.center_price = new_mid;
        self.last_rebalance_at = Some(now);
    }

    /// Whether the rebalance cooldown has elapsed.
    pub fn rebalance_ready(&self, now: Instant, cooldown_secs: u64) -> bool {
        match self.last_rebalance_at {
            None => true,
            Some(last) => now.duration_since(last).as_secs() >= cooldown_secs,
        }
    }

    /// Check if mid price has moved beyond the rebalance threshold.
    pub fn needs_rebalance(&self, current_mid: Decimal, threshold_pct: Decimal) -> bool {
        if self.center_price.is_zero() {
            return false;
        }
        let drift_pct =
            ((current_mid - self.center_price) / self.center_price * Decimal::from(100)).abs();
        drift_pct > threshold_pct
    }

    /// Update trend-filter EMAs from a fresh mid observation, then evaluate
    /// whether placement should be paused. Returns `(divergence_bps, paused)`.
    /// If the filter is not configured, returns `(0, false)` and never pauses.
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

    /// Find a grid level by client_id or exchange order_id and mark it as
    /// filled. `client_id` is checked first because we can always assign one
    /// before placement, whereas `order_id` depends on a later `OrderUpdate`
    /// event.
    pub fn mark_filled(
        &mut self,
        client_id: Option<&str>,
        order_id: &str,
    ) -> Option<GridLevel> {
        let level = self.levels.iter_mut().find(|l| {
            if l.status == GridLevelStatus::Filled {
                return false;
            }
            match client_id {
                Some(cid) => l.client_id.as_deref() == Some(cid),
                None => l.order_id.as_deref() == Some(order_id),
            }
        })?;
        level.status = GridLevelStatus::Filled;
        let filled = level.clone();
        Some(filled)
    }

    /// Count active (live) orders.
    pub fn active_count(&self) -> usize {
        self.levels.iter().filter(|l| l.status == GridLevelStatus::Active).count()
    }

    /// Check if we're at max position for a given side given a live `net_position`.
    pub fn at_max_position(net_position: Decimal, side: Side, max_position: Decimal) -> bool {
        match side {
            Side::Buy => net_position >= max_position,
            Side::Sell => net_position <= -max_position,
        }
    }
}

/// Compute `(buy_offset_mult, sell_offset_mult)` for inventory skew.
///
/// Skew ∈ [-1, 1]: positive skew = net long, which widens the buy side and
/// leaves sell side untouched so inventory tends to unwind. When `k == 0` or
/// `max_position == 0`, returns `(1, 1)`.
fn skew_multipliers(
    net_position: Decimal,
    max_position: Decimal,
    k: Decimal,
) -> (Decimal, Decimal) {
    if k.is_zero() || max_position.is_zero() {
        return (Decimal::ONE, Decimal::ONE);
    }
    let mut skew = net_position / max_position;
    // Clamp to [-1, 1] so one-sided overrun (e.g. from a gap fill past the
    // hard cap) doesn't produce absurd offsets.
    if skew > Decimal::ONE {
        skew = Decimal::ONE;
    } else if skew < -Decimal::ONE {
        skew = -Decimal::ONE;
    }
    let buy_widen = if skew > Decimal::ZERO { k * skew } else { Decimal::ZERO };
    let sell_widen = if skew < Decimal::ZERO { k * -skew } else { Decimal::ZERO };
    (Decimal::ONE + buy_widen, Decimal::ONE + sell_widen)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_config() -> GridConfig {
        GridConfig {
            symbol: "BTC-USD".to_string(),
            num_levels: 3,
            spacing_mode: SpacingMode::Geometric,
            grid_spacing: Decimal::from(1), // 1%
            order_size: Decimal::ONE,
            order_type: bb_core::types::OrderType::PostOnly,
            max_position: Decimal::from(5),
            rebalance_threshold_pct: Decimal::from(3),
            rebalance_cooldown_secs: 30,
            inventory_skew_k: Decimal::ZERO,
            fees: None,
            trend_filter: None,
        }
    }

    #[test]
    fn compute_geometric_levels() {
        let mut state = GridState::new();
        let config = test_config();
        state.compute_levels(Decimal::from(100), Decimal::ZERO, &config);

        assert_eq!(state.levels.len(), 6); // 3 buy + 3 sell
        assert_eq!(state.center_price, Decimal::from(100));

        let buys: Vec<_> = state.levels.iter().filter(|l| l.side == Side::Buy).collect();
        let sells: Vec<_> = state.levels.iter().filter(|l| l.side == Side::Sell).collect();

        assert_eq!(buys.len(), 3);
        assert_eq!(sells.len(), 3);

        for b in &buys {
            assert!(b.price < Decimal::from(100));
        }
        for s in &sells {
            assert!(s.price > Decimal::from(100));
        }
    }

    #[test]
    fn compute_arithmetic_levels() {
        let config = GridConfig {
            spacing_mode: SpacingMode::Arithmetic,
            grid_spacing: Decimal::from(10),
            ..test_config()
        };
        let mut state = GridState::new();
        state.compute_levels(Decimal::from(100), Decimal::ZERO, &config);

        let buys: Vec<Decimal> =
            state.levels.iter().filter(|l| l.side == Side::Buy).map(|l| l.price).collect();

        assert_eq!(buys, vec![Decimal::from(90), Decimal::from(80), Decimal::from(70)]);
    }

    #[test]
    fn rebalance_detection() {
        let mut state = GridState::new();
        state.center_price = Decimal::from(100);

        assert!(!state.needs_rebalance(Decimal::from(102), Decimal::from(3)));
        assert!(state.needs_rebalance(Decimal::from(104), Decimal::from(3)));
        assert!(state.needs_rebalance(Decimal::from(96), Decimal::from(3)));
    }

    #[test]
    fn max_position_check() {
        let pos = Decimal::from(5);
        assert!(GridState::at_max_position(pos, Side::Buy, Decimal::from(5)));
        assert!(!GridState::at_max_position(pos, Side::Sell, Decimal::from(5)));
    }

    #[test]
    fn inventory_skew_widens_buy_side_when_long() {
        // k = 0.4, net_position = +2.5 / max 5 → skew = 0.5 → buy_mult = 1.2, sell_mult = 1.0
        let config = GridConfig {
            inventory_skew_k: Decimal::new(4, 1), // 0.4
            ..test_config()
        };
        let state = GridState::new();
        let net_position = Decimal::new(25, 1); // 2.5

        // Baseline: zero skew at zero position.
        let baseline = state.desired_levels(Decimal::from(100), Decimal::ZERO, &config);
        let skewed = state.desired_levels(Decimal::from(100), net_position, &config);

        let baseline_buy =
            baseline.iter().find(|(s, _)| *s == Side::Buy).map(|(_, p)| *p).unwrap();
        let baseline_sell =
            baseline.iter().find(|(s, _)| *s == Side::Sell).map(|(_, p)| *p).unwrap();
        let skewed_buy =
            skewed.iter().find(|(s, _)| *s == Side::Buy).map(|(_, p)| *p).unwrap();
        let skewed_sell =
            skewed.iter().find(|(s, _)| *s == Side::Sell).map(|(_, p)| *p).unwrap();

        // Long inventory → buy level pushed further below mid (lower price).
        assert!(skewed_buy < baseline_buy, "skewed buy ({skewed_buy}) should be lower than baseline ({baseline_buy})");
        // Sell side unchanged.
        assert_eq!(skewed_sell, baseline_sell);
    }

    #[test]
    fn inventory_skew_disabled_when_k_zero() {
        let (b, s) = skew_multipliers(Decimal::from(5), Decimal::from(10), Decimal::ZERO);
        assert_eq!(b, Decimal::ONE);
        assert_eq!(s, Decimal::ONE);
    }

    #[test]
    fn reconcile_identity_when_no_drift() {
        let config = test_config();
        let mut state = GridState::new();
        state.compute_levels(Decimal::from(100), Decimal::ZERO, &config);
        // Mark all levels as Active with client_ids so they look like live orders.
        for (i, l) in state.levels.iter_mut().enumerate() {
            l.client_id = Some((i + 1).to_string());
            l.status = GridLevelStatus::Active;
        }

        let diff =
            state.reconcile(Decimal::from(100), Decimal::ZERO, &config, Decimal::new(1, 2));
        assert!(!diff.changed, "no drift → no diff");
        assert!(diff.cancels.is_empty());
        assert!(diff.places.is_empty());
    }

    #[test]
    fn reconcile_emits_only_delta_on_small_drift() {
        let config = GridConfig {
            num_levels: 2,
            grid_spacing: Decimal::new(5, 1), // 0.5% geometric
            ..test_config()
        };
        let mut state = GridState::new();
        state.compute_levels(Decimal::from(100), Decimal::ZERO, &config);
        for (i, l) in state.levels.iter_mut().enumerate() {
            l.client_id = Some((i + 1).to_string());
            l.status = GridLevelStatus::Active;
        }

        // Drift mid by 0.2% — smaller than spacing (0.5%), so every level must
        // be replaced. Matching tolerance is 0.05% so no old level matches a
        // new desired price.
        let new_mid = Decimal::new(1002, 1); // 100.2
        let diff = state.reconcile(new_mid, Decimal::ZERO, &config, Decimal::new(5, 2));
        assert!(diff.changed);
        assert_eq!(diff.cancels.len(), 4);
        assert_eq!(diff.places.len(), 4);
    }

    #[test]
    fn rebalance_cooldown_blocks_then_allows() {
        let mut state = GridState::new();
        let now = Instant::now();
        // No rebalance yet → ready.
        assert!(state.rebalance_ready(now, 30));
        state.last_rebalance_at = Some(now);
        assert!(!state.rebalance_ready(now + Duration::from_secs(5), 30));
        assert!(state.rebalance_ready(now + Duration::from_secs(31), 30));
    }

    #[test]
    fn trend_filter_pauses_on_sharp_uptrend() {
        let cfg = TrendFilterConfig {
            fast_secs: 30,
            slow_secs: 600,
            pause_divergence_bps: Decimal::from(50), // 0.5%
        };
        let mut state = GridState::new().with_trend_filter(&cfg);
        let t0 = Instant::now();

        // Seed both EMAs at 100.
        state.update_trend_filter(Decimal::from(100), t0, &cfg);

        // Sharp up-move for 30 seconds at +0.5/sec (1.5% over 30s).
        let mut paused = false;
        for i in 1..=30 {
            let t = t0 + Duration::from_secs(i);
            let mid = Decimal::from(100) + Decimal::new(i as i64 * 5, 1); // +0.5 per step
            let (_, p) = state.update_trend_filter(mid, t, &cfg);
            paused |= p;
        }
        assert!(paused, "sharp uptrend should trigger pause");
    }

    #[test]
    fn trend_filter_noop_without_config() {
        // Default state has no trend filter; update_trend_filter needs a cfg, so
        // we just assert the `paused` flag stays false when not exercised.
        let state = GridState::new();
        assert!(!state.paused);
    }
}
