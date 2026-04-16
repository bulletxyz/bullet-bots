use rust_decimal::Decimal;
use serde::Deserialize;

/// Configuration for the funding rate arbitrage strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct FundingArbConfig {
    /// Name of exchange A (the "long funding" side when rate_a > rate_b).
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

    /// Maximum USD notional per leg.
    #[serde(default = "default_max_notional")]
    pub max_notional_usd: Decimal,

    /// Maximum net delta imbalance before emergency flatten.
    #[serde(default = "default_max_delta")]
    pub max_delta_imbalance: Decimal,

    /// Reject funding rates above this (anomaly filter).
    #[serde(default = "default_max_rate")]
    pub max_funding_rate: Decimal,

    /// Order mode: "aggressive" (IoC) or "passive" (PostOnly).
    #[serde(default = "default_order_mode")]
    pub order_mode: String,

    /// Timeout for Entering/Exiting phases before emergency flatten (seconds).
    #[serde(default = "default_phase_timeout_secs")]
    pub phase_timeout_secs: u64,

    /// Slippage tolerance for aggressive orders (fraction, e.g., 0.001 = 0.1%).
    #[serde(default = "default_slippage")]
    pub slippage: Decimal,
}

fn default_max_notional() -> Decimal {
    Decimal::new(10_000, 0)
}

fn default_max_delta() -> Decimal {
    Decimal::new(5, 3) // 0.005
}

fn default_max_rate() -> Decimal {
    Decimal::new(5, 3) // 0.005
}

fn default_order_mode() -> String {
    "aggressive".to_string()
}

fn default_phase_timeout_secs() -> u64 {
    30
}

fn default_slippage() -> Decimal {
    Decimal::new(1, 3) // 0.001
}
