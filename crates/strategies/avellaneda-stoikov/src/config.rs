use bb_core::types::OrderType;
use rust_decimal::Decimal;
use serde::Deserialize;

/// Configuration for the Avellaneda-Stoikov market-making strategy.
///
/// Parameter intuition:
/// - `gamma` — risk aversion. Higher = more aggressive inventory skew
///   (reservation price moves further from mid as inventory grows).
/// - `kappa` — order-flow intensity. Higher = more competitive book, tighter
///   optimal spread. Hummingbot calibrates this from recent trade arrivals;
///   we expose it directly for simplicity.
/// - `order_horizon_secs` (τ) — the finite horizon in the A-S formula. In
///   crypto perps there's no natural T; treat this as a tuning knob that
///   scales the inventory-skew term. 60–300s is a reasonable starting range.
#[derive(Debug, Clone, Deserialize)]
pub struct AvellanedaStoikovConfig {
    /// Trading symbol (e.g. "BTC-USD").
    pub symbol: String,

    /// Order quantity per quote.
    pub order_size: Decimal,

    /// Hard position cap. Used both to normalize inventory for the skew
    /// calculation and as a safety gate on the leaned side.
    pub max_position: Decimal,

    /// Optional target inventory in base units (default 0 = neutral).
    #[serde(default)]
    pub inventory_target: Decimal,

    /// Risk aversion γ. Must be > 0.
    pub gamma: Decimal,

    /// Order-flow intensity κ. Must be > 0.
    pub kappa: Decimal,

    /// Finite horizon τ (seconds) used in the A-S closed form.
    #[serde(default = "default_order_horizon_secs")]
    pub order_horizon_secs: u64,

    /// Rolling window for the volatility estimator.
    #[serde(default = "default_vol_window_secs")]
    pub vol_window_secs: u64,

    /// Re-quote cadence. Quotes are refreshed on the first tick that hits this.
    #[serde(default = "default_order_refresh_secs")]
    pub order_refresh_secs: u64,

    /// Half-spread floor in basis points (bid/ask kept at least this far from
    /// reservation price). Prevents A-S from collapsing to 0 in low-vol
    /// regimes, which would quote inside fees.
    #[serde(default = "default_min_half_spread_bps")]
    pub min_half_spread_bps: Decimal,

    /// Half-spread ceiling in basis points. Caps A-S in vol spikes so we
    /// don't quote so wide that no fills are possible.
    #[serde(default = "default_max_half_spread_bps")]
    pub max_half_spread_bps: Decimal,

    /// Number of quote levels per side. `1` = classic textbook A-S (single
    /// bid/ask). Production MMs typically use 3–5. Outer levels are stepped
    /// outward from the A-S inner quote by `order_level_spread_bps`.
    #[serde(default = "default_order_levels")]
    pub order_levels: usize,

    /// Additional half-spread (in bps of mid) between consecutive levels.
    /// Level `i` sits at `reservation ± (inner_half_spread + i × step)`.
    /// Only used when `order_levels > 1`.
    #[serde(default = "default_order_level_spread_bps")]
    pub order_level_spread_bps: Decimal,

    /// Per-level size multiplier increment. Level `i` size is
    /// `order_size × (1 + i × amount_step)`. `0` = flat size across levels.
    #[serde(default)]
    pub order_level_amount_step: Decimal,

    /// Order type for quotes. TOML: `"post_only"` (default), `"limit"`, `"market"`.
    #[serde(default = "default_order_type")]
    pub order_type: OrderType,

    /// Optional fee-floor guardrail. If set, `on_start` refuses to run when
    /// the configured `min_half_spread_bps` can't cover round-trip maker fees.
    #[serde(default)]
    pub fees: Option<AsFees>,
}

/// Fee-floor guardrail for the A-S strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct AsFees {
    pub maker_bps: Decimal,
    #[serde(default)]
    pub taker_bps: Decimal,
    /// `min_half_spread_bps × 2` must be at least `multiplier × 2 × maker_bps`.
    #[serde(default = "default_min_spread_fee_multiplier")]
    pub min_spread_fee_multiplier: Decimal,
}

fn default_order_horizon_secs() -> u64 {
    60
}

fn default_vol_window_secs() -> u64 {
    600
}

fn default_order_refresh_secs() -> u64 {
    10
}

fn default_min_half_spread_bps() -> Decimal {
    Decimal::from(5)
}

fn default_max_half_spread_bps() -> Decimal {
    Decimal::from(500)
}

fn default_order_type() -> OrderType {
    OrderType::PostOnly
}

fn default_min_spread_fee_multiplier() -> Decimal {
    Decimal::from(3)
}

fn default_order_levels() -> usize {
    3
}

fn default_order_level_spread_bps() -> Decimal {
    Decimal::from(10)
}
