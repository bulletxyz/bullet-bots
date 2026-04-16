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

    /// Order type to use for grid orders.
    #[serde(default = "default_order_type")]
    pub order_type: String,

    /// Maximum net position before pausing one side.
    pub max_position: Decimal,

    /// Rebalance the grid if mid price moves this far from grid center (percentage).
    #[serde(default = "default_rebalance_threshold")]
    pub rebalance_threshold_pct: Decimal,
}

fn default_spacing_mode() -> SpacingMode {
    SpacingMode::Geometric
}

fn default_order_type() -> String {
    "PostOnly".to_string()
}

fn default_rebalance_threshold() -> Decimal {
    Decimal::from(3)
}
