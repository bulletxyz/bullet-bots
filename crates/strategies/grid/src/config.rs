use bb_core::types::OrderType;
use rust_decimal::Decimal;
use serde::Deserialize;

/// Grid spacing mode.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SpacingMode {
    /// Grid levels spaced by percentage of price (e.g., 0.5% apart).
    Geometric,
    /// Grid levels spaced by fixed price increment (e.g., $50 apart).
    Arithmetic,
}

/// Configuration for the grid trading strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct GridConfig {
    /// Trading symbol (e.g. "BTC-USD"). Lives here rather than on the engine
    /// so multi-strategy setups can each target their own market.
    pub symbol: String,

    /// Number of grid levels on each side (e.g., 5 = 5 buy + 5 sell = 10 total).
    pub num_levels: usize,

    /// How grid levels are spaced.
    #[serde(default = "default_spacing_mode")]
    pub spacing_mode: SpacingMode,

    /// Grid spacing value. For geometric: percentage (e.g., "0.5" = 0.5%).
    /// For arithmetic: absolute price (e.g., "50" = $50).
    pub grid_spacing: Decimal,

    /// Order quantity per grid level.
    pub order_size: Decimal,

    /// Order type for grid orders. TOML values: `"limit"`, `"post_only"`,
    /// `"market"`. Typos fail at parse time instead of silently falling
    /// through to a default.
    #[serde(default = "default_order_type")]
    pub order_type: OrderType,

    /// Maximum net position before pausing one side.
    pub max_position: Decimal,

    /// Rebalance the grid when mid has drifted this far from grid center (%).
    ///
    /// **Rule of thumb:** keep this ≤ `grid_spacing`. If it's larger, the
    /// market can move by more than one level before rebalance fires, and
    /// inner levels end up crossing the top of book — the venue will reject
    /// every PostOnly submission as in-cross until the next rebalance lands.
    /// `GridActor` tolerates this (it skips crossed levels with a warning)
    /// but the grid quotes one-sided until the book moves back or the
    /// threshold trips.
    #[serde(default = "default_rebalance_threshold")]
    pub rebalance_threshold_pct: Decimal,

    /// Minimum seconds between rebalances. Prevents churn in whippy markets.
    #[serde(default = "default_rebalance_cooldown_secs")]
    pub rebalance_cooldown_secs: u64,

    /// Inventory skew coefficient in [0, ~1]. When >0, the leaned side's
    /// levels are pushed further from mid proportional to current inventory
    /// (a cheap Avellaneda-Stoikov-style skew). Zero disables.
    #[serde(default)]
    pub inventory_skew_k: Decimal,

    /// Optional fee-floor guardrail. If set, `on_start` refuses to run when
    /// configured spacing cannot cover round-trip fees by the given multiplier.
    #[serde(default)]
    pub fees: Option<GridFees>,

    /// Optional EMA-slope trend filter. If set, grid placement is paused when
    /// fast/slow EMA divergence exceeds threshold.
    #[serde(default)]
    pub trend_filter: Option<TrendFilterConfig>,
}

/// Fee-floor startup guardrail config. All values in basis points.
#[derive(Debug, Clone, Deserialize)]
pub struct GridFees {
    pub maker_bps: Decimal,
    #[serde(default)]
    pub taker_bps: Decimal,
    #[serde(default = "default_min_spacing_fee_multiplier")]
    pub min_spacing_fee_multiplier: Decimal,
}

/// Trend-filter config. Pauses placement when EMA divergence in basis points
/// (`10000 * (fast - slow) / slow`) exceeds `pause_divergence_bps`.
#[derive(Debug, Clone, Deserialize)]
pub struct TrendFilterConfig {
    /// Fast EMA time constant, seconds.
    #[serde(default = "default_fast_secs")]
    pub fast_secs: u64,
    /// Slow EMA time constant, seconds.
    #[serde(default = "default_slow_secs")]
    pub slow_secs: u64,
    /// Pause when `|fast - slow| / slow * 10000` exceeds this (basis points).
    pub pause_divergence_bps: Decimal,
}

fn default_spacing_mode() -> SpacingMode {
    SpacingMode::Geometric
}

fn default_order_type() -> OrderType {
    OrderType::PostOnly
}

fn default_rebalance_threshold() -> Decimal {
    Decimal::from(3)
}

fn default_rebalance_cooldown_secs() -> u64 {
    30
}

fn default_min_spacing_fee_multiplier() -> Decimal {
    Decimal::from(3)
}

fn default_fast_secs() -> u64 {
    300
}

fn default_slow_secs() -> u64 {
    3600
}
