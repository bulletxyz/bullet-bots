use rust_decimal::Decimal;
use serde::Deserialize;

/// How the arb places entry/exit orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderMode {
    /// Aggressive `IoC` at best opposing + configured slippage.
    Aggressive,
    /// Passive post-only at the near touch. Only makes sense for thin flow;
    /// trades off fill certainty for maker rebates.
    Passive,
}

/// Configuration for the funding rate arbitrage strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct FundingArbConfig {
    /// Trading symbol (e.g. "BTC-USD").
    pub symbol: String,

    /// Name of exchange A (the "long funding" side when `rate_a` > `rate_b`).
    pub exchange_a: String,

    /// Name of exchange B.
    pub exchange_b: String,

    /// Minimum funding rate differential (per hour) to enter a position.
    /// e.g., "0.0005" = 0.05%/hr.
    pub entry_threshold: Decimal,

    /// Exit when differential drops below this (per hour).
    /// e.g., "0.0001" = 0.01%/hr.
    pub exit_threshold: Decimal,

    /// Base asset size per leg.
    pub order_size: Decimal,

    /// Maximum net delta imbalance before emergency flatten.
    #[serde(default = "default_max_delta")]
    pub max_delta_imbalance: Decimal,

    /// Reject funding rates above this (anomaly filter).
    #[serde(default = "default_max_rate")]
    pub max_funding_rate: Decimal,

    /// Order mode: TOML `"aggressive"` (default, `IoC`) or `"passive"` (`PostOnly`).
    #[serde(default = "default_order_mode")]
    pub order_mode: OrderMode,

    /// Timeout for Entering/Exiting phases before emergency flatten (seconds).
    #[serde(default = "default_phase_timeout_secs")]
    pub phase_timeout_secs: u64,

    /// Slippage tolerance for aggressive orders (fraction, e.g., 0.001 = 0.1%).
    #[serde(default = "default_slippage")]
    pub slippage: Decimal,

    /// Minimum seconds to remain `Flat` before a fresh entry is allowed.
    /// Guards against rapid re-entry when the spread wobbles around the
    /// threshold; also prevents an insta-re-enter after an emergency flatten
    /// while the condition that triggered it is still live.
    #[serde(default = "default_min_flat_hold_secs")]
    pub min_flat_hold_secs: u64,
}

fn default_max_delta() -> Decimal {
    Decimal::new(5, 3) // 0.005
}

fn default_max_rate() -> Decimal {
    Decimal::new(5, 3) // 0.005
}

fn default_order_mode() -> OrderMode {
    OrderMode::Aggressive
}

fn default_phase_timeout_secs() -> u64 {
    30
}

fn default_slippage() -> Decimal {
    Decimal::new(1, 3) // 0.001
}

fn default_min_flat_hold_secs() -> u64 {
    60
}

impl FundingArbConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.order_size <= Decimal::ZERO {
            return Err("order_size must be positive".into());
        }
        if self.entry_threshold <= Decimal::ZERO {
            return Err("entry_threshold must be positive".into());
        }
        if self.exit_threshold >= self.entry_threshold {
            return Err(format!(
                "exit_threshold ({}) must be less than entry_threshold ({})",
                self.exit_threshold, self.entry_threshold
            ));
        }
        if self.max_delta_imbalance <= Decimal::ZERO {
            return Err("max_delta_imbalance must be positive".into());
        }
        if self.slippage < Decimal::ZERO {
            return Err("slippage must be non-negative".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> FundingArbConfig {
        FundingArbConfig {
            symbol: "BTC-PERP".into(),
            exchange_a: "bullet".into(),
            exchange_b: "hl".into(),
            entry_threshold: Decimal::new(5, 4),
            exit_threshold: Decimal::new(1, 4),
            order_size: Decimal::ONE,
            max_delta_imbalance: Decimal::new(5, 3),
            max_funding_rate: Decimal::new(5, 3),
            order_mode: OrderMode::Aggressive,
            phase_timeout_secs: 30,
            slippage: Decimal::new(1, 3),
            min_flat_hold_secs: 60,
        }
    }

    #[test]
    fn base_config_is_valid() {
        assert!(base().validate().is_ok());
    }

    #[test]
    fn exit_threshold_must_be_less_than_entry() {
        let mut c = base();
        c.exit_threshold = c.entry_threshold; // equal → invalid
        assert!(c.validate().is_err());
        c.exit_threshold = c.entry_threshold + Decimal::ONE; // greater → invalid
        assert!(c.validate().is_err());
    }

    #[test]
    fn order_size_must_be_positive() {
        let mut c = base();
        c.order_size = Decimal::ZERO;
        assert!(c.validate().is_err());
    }

    #[test]
    fn max_delta_imbalance_must_be_positive() {
        let mut c = base();
        c.max_delta_imbalance = Decimal::ZERO;
        assert!(c.validate().is_err());
    }
}
