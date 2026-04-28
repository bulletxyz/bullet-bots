//! Avellaneda-Stoikov closed-form quote math.
//!
//! Reference: Avellaneda & Stoikov (2008), *High-frequency trading in a limit
//! order book*. The finite-horizon formulas used here are the ones Hummingbot
//! implements in `avellaneda_market_making`; they extend to the GLFT
//! stationary limit as τ grows.
//!
//! Given mid `s`, inventory `q` (base units, signed), risk aversion `γ > 0`,
//! order-flow intensity `κ > 0`, volatility `σ`, and horizon `τ`:
//!
//! ```text
//! reservation_price = s - q · γ · σ² · τ
//! optimal_spread    = γ · σ² · τ + (2/γ) · ln(1 + γ/κ)
//! bid = reservation_price - optimal_spread / 2
//! ask = reservation_price + optimal_spread / 2
//! ```
//!
//! All inputs and outputs here are `f64`; the strategy layer converts to/from
//! `Decimal` at the boundary.

/// A single pair of quotes produced by the model.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub reservation_price: f64,
    pub half_spread: f64,
}

/// Bounds enforced on the half-spread after A-S produces a raw value.
#[derive(Debug, Clone, Copy)]
pub struct SpreadBounds {
    pub min_half_spread_bps: f64,
    pub max_half_spread_bps: f64,
}

/// One rung of a multi-level quote ladder.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LadderRung {
    pub level: usize,
    pub bid: f64,
    pub ask: f64,
    pub half_spread: f64,
}

/// Core A-S quote computation.
///
/// `inventory_normalized` should be `(net_position - inventory_target) /
/// max_position`, clipped to [-1, 1]. Normalizing makes γ stable across
/// position sizes — exactly what Hummingbot's `inventory_target_base_pct`
/// achieves by a different spelling.
pub fn quote(
    mid: f64,
    inventory_normalized: f64,
    gamma: f64,
    kappa: f64,
    sigma: f64,
    tau_secs: f64,
    bounds: SpreadBounds,
) -> Quote {
    debug_assert!(gamma > 0.0 && kappa > 0.0, "gamma and kappa must be > 0");
    let q = inventory_normalized.clamp(-1.0, 1.0);
    let sigma2 = sigma * sigma;

    // Reservation price: inventory drags it away from mid.
    let reservation_price = mid - q * gamma * sigma2 * tau_secs;

    // A-S optimal *total* spread. Second term dominates when σ is small; first
    // term widens in vol or long horizons.
    let raw_spread = gamma * sigma2 * tau_secs + (2.0 / gamma) * (1.0 + gamma / kappa).ln();
    let raw_half = raw_spread / 2.0;

    // Clamp to configured bounds (in bps of mid, converted to price terms).
    let min_half = mid * bounds.min_half_spread_bps / 10_000.0;
    let max_half = mid * bounds.max_half_spread_bps / 10_000.0;
    let half_spread = raw_half.clamp(min_half, max_half);

    let bid = reservation_price - half_spread;
    let ask = reservation_price + half_spread;

    // Post-clamp sanity: ensure bid ≤ mid ≤ ask. Heavy inventory can shift
    // the reservation price far enough that bid > mid (heavy short) or
    // ask < mid (heavy long). Pin only the offending side to mid ± min_half
    // and keep the A-S-optimal quote on the other side — that preserves the
    // inventory-unwind skew (wide ask when short / wide bid when long) while
    // respecting PostOnly. The `ask` in the `bid > mid` branch is already
    // ≥ mid + min_half because `half ≥ min_half`, so keeping it is safe.
    let (bid, ask) = if bid > mid {
        (mid - min_half, ask)
    } else if ask < mid {
        (bid, mid + min_half)
    } else {
        (bid, ask)
    };

    Quote { bid, ask, reservation_price, half_spread }
}

/// Multi-level quote ladder. Level 0 is the A-S inner quote (output of
/// [`quote`]); each outer level steps out by `level_spread_bps × mid / 10_000`
/// from the inner half-spread, anchored to the same reservation price. This
/// is the layout Hummingbot's `avellaneda_market_making` uses via
/// `order_levels` / `order_level_spread`.
// Math API — named scalars beat a struct for the call-site intuition of the
// formula. One extra parameter beyond clippy's soft limit is fine.
#[allow(clippy::too_many_arguments, clippy::cast_precision_loss)]
pub fn quote_ladder(
    mid: f64,
    inventory_normalized: f64,
    gamma: f64,
    kappa: f64,
    sigma: f64,
    tau_secs: f64,
    bounds: SpreadBounds,
    levels: usize,
    level_spread_bps: f64,
) -> (Quote, Vec<LadderRung>) {
    let inner = quote(mid, inventory_normalized, gamma, kappa, sigma, tau_secs, bounds);
    let step = mid * level_spread_bps / 10_000.0;
    let n = levels.max(1);
    let mut rungs = Vec::with_capacity(n);
    for i in 0..n {
        let half = inner.half_spread + (i as f64) * step;
        rungs.push(LadderRung {
            level: i,
            bid: inner.reservation_price - half,
            ask: inner.reservation_price + half,
            half_spread: half,
        });
    }
    (inner, rungs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds() -> SpreadBounds {
        SpreadBounds { min_half_spread_bps: 5.0, max_half_spread_bps: 500.0 }
    }

    #[test]
    fn neutral_inventory_quotes_symmetric() {
        let q = quote(100.0, 0.0, 0.5, 1.5, 0.01, 60.0, bounds());
        assert!((q.reservation_price - 100.0).abs() < 1e-9);
        assert!((q.bid + q.ask - 200.0).abs() < 1e-9, "should be symmetric around 100");
        assert!(q.bid < 100.0 && q.ask > 100.0);
    }

    #[test]
    fn long_inventory_skews_quotes_down() {
        let neutral = quote(100.0, 0.0, 0.5, 1.5, 0.01, 60.0, bounds());
        let long = quote(100.0, 0.5, 0.5, 1.5, 0.01, 60.0, bounds());
        // Reservation price should shift below mid when long.
        assert!(long.reservation_price < neutral.reservation_price);
        // Both quotes should shift down — buy side gets more selective, sell
        // side gets easier to hit, naturally unwinding inventory.
        assert!(long.bid < neutral.bid);
        assert!(long.ask < neutral.ask);
    }

    #[test]
    fn short_inventory_skews_quotes_up() {
        let neutral = quote(100.0, 0.0, 0.5, 1.5, 0.01, 60.0, bounds());
        let short = quote(100.0, -0.5, 0.5, 1.5, 0.01, 60.0, bounds());
        assert!(short.reservation_price > neutral.reservation_price);
        assert!(short.bid > neutral.bid);
        assert!(short.ask > neutral.ask);
    }

    #[test]
    fn higher_gamma_increases_skew_magnitude() {
        let low = quote(100.0, 0.5, 0.1, 1.5, 0.01, 60.0, bounds());
        let high = quote(100.0, 0.5, 2.0, 1.5, 0.01, 60.0, bounds());
        // Higher γ → reservation moves further below mid under same inventory.
        assert!(high.reservation_price < low.reservation_price);
    }

    #[test]
    fn low_vol_gets_floored_by_min_spread() {
        // Choose params where both A-S terms are tiny so the raw half-spread
        // is well below the floor. γ=10, κ=1000 → (2/γ)ln(1+γ/κ) ≈ 0.002; σ
        // tiny and τ tiny → γσ²τ negligible.
        let q = quote(100.0, 0.0, 10.0, 1000.0, 1e-6, 1.0, bounds());
        let min_half = 100.0 * 5.0 / 10_000.0;
        assert!(
            (q.half_spread - min_half).abs() < 1e-9,
            "half_spread {} should equal min {}",
            q.half_spread,
            min_half
        );
    }

    #[test]
    fn high_vol_gets_capped_by_max_spread() {
        // Huge σ and long horizon blow up A-S; ceiling must clamp.
        let q = quote(100.0, 0.0, 0.5, 1.5, 5.0, 600.0, bounds());
        let max_half = 100.0 * 500.0 / 10_000.0;
        assert!(
            (q.half_spread - max_half).abs() < 1e-9,
            "half_spread {} should equal max {}",
            q.half_spread,
            max_half
        );
    }

    #[test]
    fn clamps_preserve_bid_leq_mid_leq_ask() {
        // Extreme long inventory — reservation pushed well below mid.
        let q = quote(100.0, 1.0, 2.0, 1.5, 0.1, 600.0, bounds());
        assert!(q.bid <= 100.0, "bid {} should not cross above mid", q.bid);
        assert!(q.ask >= 100.0, "ask {} should not cross below mid", q.ask);
    }

    #[test]
    fn heavy_short_inventory_keeps_ask_wide() {
        // Heavy short → reservation well above mid, so the raw bid would be
        // above mid. We pin bid = mid - min_half, but the ask (well above
        // mid) should be preserved unchanged — wide ask is the signal that
        // discourages more shorting.
        let q = quote(100.0, -1.0, 2.0, 1.5, 0.1, 600.0, bounds());
        assert!(q.bid <= 100.0);
        // Ask should be materially above mid + min_half, not collapsed to it.
        let min_half = 100.0 * 5.0 / 10_000.0;
        assert!(
            q.ask > 100.0 + min_half + 1e-6,
            "ask {} should preserve the A-S skew (> mid + min_half = {})",
            q.ask,
            100.0 + min_half
        );
    }

    #[test]
    fn heavy_long_inventory_keeps_bid_wide() {
        // Symmetric case: heavy long → ask would fall below mid. Pin ask;
        // keep the (wide, below-mid) bid that discourages adding to the long.
        let q = quote(100.0, 1.0, 2.0, 1.5, 0.1, 600.0, bounds());
        assert!(q.ask >= 100.0);
        let min_half = 100.0 * 5.0 / 10_000.0;
        assert!(
            q.bid < 100.0 - min_half - 1e-6,
            "bid {} should preserve the A-S skew (< mid - min_half = {})",
            q.bid,
            100.0 - min_half
        );
    }

    #[test]
    fn ladder_steps_outward_and_preserves_inner() {
        let (inner, rungs) = quote_ladder(100.0, 0.0, 0.5, 1.5, 0.01, 60.0, bounds(), 3, 10.0);
        assert_eq!(rungs.len(), 3);
        // Level 0 matches the inner A-S quote exactly.
        assert!((rungs[0].bid - inner.bid).abs() < 1e-12);
        assert!((rungs[0].ask - inner.ask).abs() < 1e-12);
        // Each outer level is 10 bps of mid = 0.1 further out than the prior.
        assert!((rungs[1].half_spread - rungs[0].half_spread - 0.1).abs() < 1e-9);
        assert!((rungs[2].half_spread - rungs[1].half_spread - 0.1).abs() < 1e-9);
        // Bids strictly decrease, asks strictly increase with level.
        assert!(rungs[1].bid < rungs[0].bid && rungs[2].bid < rungs[1].bid);
        assert!(rungs[1].ask > rungs[0].ask && rungs[2].ask > rungs[1].ask);
    }

    #[test]
    fn ladder_single_level_matches_inner() {
        let (inner, rungs) = quote_ladder(100.0, 0.3, 0.5, 1.5, 0.01, 60.0, bounds(), 1, 10.0);
        assert_eq!(rungs.len(), 1);
        assert!((rungs[0].bid - inner.bid).abs() < 1e-12);
        assert!((rungs[0].ask - inner.ask).abs() < 1e-12);
    }

    #[test]
    fn kappa_tightens_base_spread() {
        // Increasing κ (more competitive market) shrinks the (2/γ)·ln(1+γ/κ) term.
        // Use small τ so the γσ²τ term doesn't dominate, and disable clamps.
        let loose_bounds = SpreadBounds { min_half_spread_bps: 0.0, max_half_spread_bps: 10_000.0 };
        let low_k = quote(100.0, 0.0, 0.5, 0.5, 0.01, 1.0, loose_bounds);
        let high_k = quote(100.0, 0.0, 0.5, 50.0, 0.01, 1.0, loose_bounds);
        assert!(high_k.half_spread < low_k.half_spread);
    }
}
