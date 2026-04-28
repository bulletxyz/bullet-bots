use bb_core::config::ValidateConfig;
use bb_core::types::OrderType;
use rust_decimal::Decimal;
use serde::Deserialize;

/// Configuration for the static grid strategy.
///
/// Bias is expressed geometrically: an `anchor_price` inside `[lower_price,
/// upper_price]` divides the range into buys (below the anchor) and sells
/// (above). Move the anchor up → more buy levels → long bias. Move it down
/// → more sell levels → short bias. Place it at the midpoint for neutral.
///
/// Levels are generated once at startup and never move. When a buy at level
/// `N` fills the actor places a sell at level `N+1`; when a sell at level
/// `M` fills it places a buy at level `M-1`. Profit per completed round trip
/// equals `spacing × order_size`, where `spacing = (upper - lower) / (num_levels - 1)`.
#[derive(Debug, Clone, Deserialize)]
pub struct GridConfig {
    /// Broker to trade against — name matches the one passed to
    /// `HarnessBuilder::wire_broker` (e.g. `"bullet"`).
    #[serde(default = "default_exchange")]
    pub exchange: String,

    /// Trading symbol (e.g. `"BTC-USD"`).
    pub symbol: String,

    /// Lower bound of the grid, inclusive (absolute price).
    pub lower_price: Decimal,

    /// Upper bound of the grid, inclusive (absolute price).
    pub upper_price: Decimal,

    /// Total number of grid levels across `[lower_price, upper_price]`.
    /// Must be ≥ 2. Spacing between adjacent levels is
    /// `(upper - lower) / (num_levels - 1)`.
    pub num_levels: usize,

    /// Price below which levels are buys and above which they are sells.
    /// Defaults to current mid at startup. Omit for "neutral at start"; set
    /// toward `lower_price` for long bias, toward `upper_price` for short.
    #[serde(default)]
    pub anchor_price: Option<Decimal>,

    /// Order quantity per grid level.
    pub order_size: Decimal,

    /// Order type for grid orders. TOML values: `"limit"`, `"post_only"`
    /// (default), `"market"`. Typos fail at parse time.
    #[serde(default = "default_order_type")]
    pub order_type: OrderType,

    /// Hard inventory cap. Placement pauses on the leaned side when reached.
    pub max_position: Decimal,

    /// Optional fee-floor guardrail: refuse to run when level spacing can't
    /// cover `min_spacing_fee_multiplier × round-trip maker fees`.
    #[serde(default)]
    pub fees: Option<GridFees>,

    /// Optional EMA-slope trend filter. Pauses placement when fast/slow EMA
    /// divergence exceeds `pause_divergence_bps`. Useful on a static grid
    /// to stop adding to your bag in a strong trend.
    #[serde(default)]
    pub trend_filter: Option<TrendFilterConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GridFees {
    pub maker_bps: Decimal,
    #[serde(default)]
    pub taker_bps: Decimal,
    #[serde(default = "default_min_spacing_fee_multiplier")]
    pub min_spacing_fee_multiplier: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrendFilterConfig {
    #[serde(default = "default_fast_secs")]
    pub fast_secs: u64,
    #[serde(default = "default_slow_secs")]
    pub slow_secs: u64,
    pub pause_divergence_bps: Decimal,
}

fn default_order_type() -> OrderType {
    OrderType::PostOnly
}

fn default_exchange() -> String {
    "bullet".to_string()
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

impl GridConfig {
    /// Absolute spacing between adjacent levels (in quote-asset price units).
    ///
    /// Callers are expected to have already run [`Self::validate`] at
    /// startup — `debug_assert`s enforce the invariant in development and
    /// panic at the call site rather than returning a nonsense value that
    /// propagates silently. In release builds, a misconfigured grid
    /// produces a well-defined (but useless) spacing of zero.
    pub fn spacing(&self) -> Decimal {
        debug_assert!(
            self.num_levels >= 2,
            "grid config: num_levels must be ≥ 2 (validate() first)"
        );
        if self.num_levels < 2 {
            return Decimal::ZERO;
        }
        (self.upper_price - self.lower_price) / Decimal::from(self.num_levels as u64 - 1)
    }

    pub fn validate(&self) -> Result<(), String> {
        <Self as ValidateConfig>::validate(self)
    }
}

impl ValidateConfig for GridConfig {
    /// Validate the config at startup. Returns a readable error for common
    /// misconfigurations rather than letting them surface as weird runtime
    /// behavior.
    fn validate(&self) -> Result<(), String> {
        if self.num_levels < 2 {
            return Err("num_levels must be ≥ 2".to_string());
        }
        if self.lower_price >= self.upper_price {
            return Err(format!(
                "lower_price ({}) must be < upper_price ({})",
                self.lower_price, self.upper_price
            ));
        }
        if self.lower_price <= Decimal::ZERO {
            return Err("lower_price must be > 0".to_string());
        }
        if self.order_size <= Decimal::ZERO {
            return Err("order_size must be > 0".to_string());
        }
        if let Some(anchor) = self.anchor_price
            && (anchor < self.lower_price || anchor > self.upper_price)
        {
            return Err(format!(
                "anchor_price ({}) must be within [{}, {}]",
                anchor, self.lower_price, self.upper_price
            ));
        }
        Ok(())
    }
}
