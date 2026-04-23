//! Configuration for the reference-price arb strategy.
//!
//! Thresholds are in bps (basis points = 0.01%) as unsigned magnitudes —
//! the sign is derived from position side inside the strategy. The
//! fee-floor guardrail in `validate()` mirrors the grid strategy's
//! approach: refuse to run a parameterization whose edge is eaten by
//! round-trip taker fees.

use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ReferenceArbConfig {
    /// Name of the tradable venue (registered broker, typically `"bullet"`).
    pub exchange: String,
    /// Internal symbol on the tradable venue (e.g. `"BTC-USD"`).
    pub symbol: String,
    /// Binance stream symbol (e.g. `"btcusdt"`). Case-insensitive on compare.
    pub binance_symbol: String,

    pub order_size: Decimal,
    pub max_position: Decimal,

    /// Enter when `|bullet_mid − binance_mid| / binance_mid * 10_000 >= this`.
    pub entry_threshold_bps: Decimal,
    /// TP when spread has reverted to within this magnitude on the entry side.
    pub exit_threshold_bps: Decimal,
    /// SL when spread has widened further than this magnitude on the entry side.
    pub stop_loss_bps: Decimal,

    /// Require the signal to persist across this many consecutive evaluations
    /// before firing. Filters single-evaluation divergences caused by the
    /// Binance↔Bullet WS latency gap.
    #[serde(default = "default_persistence_ticks")]
    pub persistence_ticks: u32,

    /// Force-exit a `Holding` position after this many ticks regardless of
    /// TP/SL. Guards against spreads that neither close nor blow out.
    #[serde(default = "default_max_hold_ticks")]
    pub max_hold_ticks: u32,

    /// If no Binance update has arrived within this many seconds, treat the
    /// reference as stale and refuse to trade until it recovers.
    #[serde(default = "default_reference_stale_secs")]
    pub reference_stale_secs: u64,

    /// Bullet taker fee in bps. Used only for the fee-floor guard.
    pub taker_fee_bps: Decimal,
    /// Edge must exceed this × round-trip fees.
    #[serde(default = "default_min_edge_multiple")]
    pub min_edge_multiple: Decimal,
}

fn default_persistence_ticks() -> u32 {
    2
}
fn default_max_hold_ticks() -> u32 {
    24
}
fn default_reference_stale_secs() -> u64 {
    10
}
fn default_min_edge_multiple() -> Decimal {
    Decimal::new(15, 1) // 1.5
}

impl ReferenceArbConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.order_size <= Decimal::ZERO {
            return Err("order_size must be positive".into());
        }
        if self.max_position < self.order_size {
            return Err("max_position must be >= order_size".into());
        }
        if self.entry_threshold_bps <= self.exit_threshold_bps {
            return Err("entry_threshold_bps must be > exit_threshold_bps (else TP never triggers)".into());
        }
        if self.stop_loss_bps <= self.entry_threshold_bps {
            return Err("stop_loss_bps must be > entry_threshold_bps (else SL triggers at entry)".into());
        }
        if self.persistence_ticks == 0 {
            return Err("persistence_ticks must be >= 1".into());
        }
        if self.taker_fee_bps < Decimal::ZERO {
            return Err("taker_fee_bps must be non-negative".into());
        }
        if self.min_edge_multiple < Decimal::ONE {
            return Err("min_edge_multiple must be >= 1".into());
        }

        let edge = self.entry_threshold_bps - self.exit_threshold_bps;
        let required = self.min_edge_multiple * Decimal::from(2) * self.taker_fee_bps;
        if edge < required {
            return Err(format!(
                "fee floor violated: edge {edge} bps < required {required} bps \
                 (min_edge_multiple × 2 × taker_fee_bps). Raise entry_threshold_bps, \
                 lower exit_threshold_bps, or accept a lower min_edge_multiple."
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> ReferenceArbConfig {
        ReferenceArbConfig {
            exchange: "bullet".into(),
            symbol: "BTC-USD".into(),
            binance_symbol: "btcusdt".into(),
            order_size: Decimal::new(1, 3),     // 0.001
            max_position: Decimal::new(3, 3),   // 0.003
            entry_threshold_bps: Decimal::from(15),
            exit_threshold_bps: Decimal::from(3),
            stop_loss_bps: Decimal::from(40),
            persistence_ticks: 2,
            max_hold_ticks: 24,
            reference_stale_secs: 10,
            taker_fee_bps: Decimal::from(4),
            min_edge_multiple: Decimal::new(15, 1),
        }
    }

    #[test]
    fn base_config_is_valid() {
        base().validate().expect("base config should validate");
    }

    #[test]
    fn entry_must_exceed_exit() {
        let mut c = base();
        c.entry_threshold_bps = c.exit_threshold_bps;
        assert!(c.validate().is_err());
    }

    #[test]
    fn stop_loss_must_exceed_entry() {
        let mut c = base();
        c.stop_loss_bps = c.entry_threshold_bps;
        assert!(c.validate().is_err());
    }

    #[test]
    fn fee_floor_rejects_thin_edge() {
        let mut c = base();
        // edge = 12 bps, taker = 5 bps, required = 1.5 * 2 * 5 = 15. Must reject.
        c.entry_threshold_bps = Decimal::from(15);
        c.exit_threshold_bps = Decimal::from(3);
        c.taker_fee_bps = Decimal::from(5);
        assert!(c.validate().is_err());
    }

    #[test]
    fn max_position_below_order_size_rejected() {
        let mut c = base();
        c.max_position = c.order_size - Decimal::new(1, 6);
        assert!(c.validate().is_err());
    }
}
