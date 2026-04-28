use bb_core::config::ValidateConfig;
use bb_core::types::OrderType;
use rust_decimal::Decimal;
use serde::Deserialize;

/// Configuration for the simple one-level market maker.
#[derive(Debug, Clone, Deserialize)]
pub struct SimpleMmConfig {
    /// Broker to trade against, matching `HarnessBuilder::wire_broker`.
    #[serde(default = "default_exchange")]
    pub exchange: String,

    /// Trading symbol, e.g. `"BTC-USD"`.
    pub symbol: String,

    /// Bid distance below mid, in basis points.
    pub bid_spread_bps: Decimal,

    /// Ask distance above mid, in basis points.
    pub ask_spread_bps: Decimal,

    /// Quantity for each resting order.
    pub order_size: Decimal,

    /// Hard cap on absolute net position.
    pub max_position: Decimal,

    /// Cancel/replace cadence.
    #[serde(default = "default_refresh_secs")]
    pub refresh_secs: u64,

    /// Do not refresh an existing quote unless its price drifted by at least
    /// this many bps from the desired price.
    #[serde(default = "default_refresh_threshold_bps")]
    pub refresh_threshold_bps: Decimal,

    /// `"post_only"` by default. `Limit` and `Market` are accepted but this
    /// starter strategy is designed for maker orders.
    #[serde(default = "default_order_type")]
    pub order_type: OrderType,

    /// Dry-run mode updates quote intent and paper inventory, but never sends
    /// orders to the broker.
    #[serde(default)]
    pub dry_run: bool,

    /// Optional fee-floor guardrail.
    #[serde(default)]
    pub fees: Option<SimpleMmFees>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SimpleMmFees {
    pub maker_bps: Decimal,
    #[serde(default = "default_min_spread_fee_multiplier")]
    pub min_spread_fee_multiplier: Decimal,
}

fn default_exchange() -> String {
    "bullet".to_string()
}

fn default_refresh_secs() -> u64 {
    5
}

fn default_refresh_threshold_bps() -> Decimal {
    Decimal::ONE
}

fn default_order_type() -> OrderType {
    OrderType::PostOnly
}

fn default_min_spread_fee_multiplier() -> Decimal {
    Decimal::from(3)
}

impl ValidateConfig for SimpleMmConfig {
    fn validate(&self) -> Result<(), String> {
        if self.bid_spread_bps <= Decimal::ZERO {
            return Err("bid_spread_bps must be positive".into());
        }
        if self.ask_spread_bps <= Decimal::ZERO {
            return Err("ask_spread_bps must be positive".into());
        }
        if self.order_size <= Decimal::ZERO {
            return Err("order_size must be positive".into());
        }
        if self.max_position < self.order_size {
            return Err("max_position must be >= order_size".into());
        }
        if self.refresh_secs == 0 {
            return Err("refresh_secs must be >= 1".into());
        }
        if self.refresh_threshold_bps < Decimal::ZERO {
            return Err("refresh_threshold_bps must be non-negative".into());
        }
        if let Some(fees) = &self.fees {
            if fees.maker_bps < Decimal::ZERO {
                return Err("fees.maker_bps must be non-negative".into());
            }
            if fees.min_spread_fee_multiplier < Decimal::ONE {
                return Err("fees.min_spread_fee_multiplier must be >= 1".into());
            }
            let total_spread = self.bid_spread_bps + self.ask_spread_bps;
            let required = Decimal::from(2) * fees.maker_bps * fees.min_spread_fee_multiplier;
            if total_spread < required {
                return Err(format!(
                    "total spread {total_spread} bps < required {required} bps \
                     (2 * maker_bps * min_spread_fee_multiplier)"
                ));
            }
        }
        Ok(())
    }
}

impl SimpleMmConfig {
    pub fn validate(&self) -> Result<(), String> {
        <Self as ValidateConfig>::validate(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> SimpleMmConfig {
        SimpleMmConfig {
            exchange: "bullet".into(),
            symbol: "BTC-USD".into(),
            bid_spread_bps: Decimal::from(10),
            ask_spread_bps: Decimal::from(10),
            order_size: Decimal::new(1, 3),
            max_position: Decimal::new(5, 3),
            refresh_secs: 5,
            refresh_threshold_bps: Decimal::ONE,
            order_type: OrderType::PostOnly,
            dry_run: false,
            fees: Some(SimpleMmFees {
                maker_bps: Decimal::ONE,
                min_spread_fee_multiplier: Decimal::from(3),
            }),
        }
    }

    #[test]
    fn base_config_is_valid() {
        cfg().validate().expect("base config should validate");
    }

    #[test]
    fn max_position_must_cover_order_size() {
        let mut c = cfg();
        c.max_position = Decimal::new(5, 4);
        assert!(c.validate().is_err());
    }

    #[test]
    fn fee_floor_rejects_too_tight_spread() {
        let mut c = cfg();
        c.bid_spread_bps = Decimal::ONE;
        c.ask_spread_bps = Decimal::ONE;
        assert!(c.validate().is_err());
    }
}
