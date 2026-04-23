use bb_core::types::Side;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::config::{GridConfig, SpacingMode};

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

/// Manages the set of grid levels and position tracking.
#[derive(Debug)]
pub struct GridState {
    pub levels: Vec<GridLevel>,
    pub center_price: Decimal,
    pub net_position: Decimal,
    pub realized_pnl: Decimal,
    pub total_fills: u64,
    /// Monotonic counter for `ClientOrderId` assignment; never reused.
    next_client_id: u64,
}

impl GridState {
    pub fn new() -> Self {
        Self {
            levels: Vec::new(),
            center_price: Decimal::ZERO,
            net_position: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            total_fills: 0,
            next_client_id: 1,
        }
    }

    /// Issue a fresh `ClientOrderId` as a decimal-encoded string.
    pub fn issue_client_id(&mut self) -> String {
        let id = self.next_client_id;
        self.next_client_id += 1;
        id.to_string()
    }

    /// Compute grid levels centered on `mid_price`.
    pub fn compute_levels(&mut self, mid_price: Decimal, config: &GridConfig) {
        self.levels.clear();
        self.center_price = mid_price;

        for i in 1..=config.num_levels {
            let i_dec = Decimal::from(i as u64);

            let (buy_price, sell_price) = match config.spacing_mode {
                SpacingMode::Geometric => {
                    // Percentage-based spacing
                    let offset_pct = config.grid_spacing * i_dec / Decimal::from(100);
                    let buy = mid_price * (Decimal::ONE - offset_pct);
                    let sell = mid_price * (Decimal::ONE + offset_pct);
                    (buy, sell)
                }
                SpacingMode::Arithmetic => {
                    // Fixed price spacing
                    let offset = config.grid_spacing * i_dec;
                    (mid_price - offset, mid_price + offset)
                }
            };

            self.levels.push(GridLevel {
                side: Side::Buy,
                price: buy_price,
                order_id: None,
                client_id: None,
                status: GridLevelStatus::Pending,
            });
            self.levels.push(GridLevel {
                side: Side::Sell,
                price: sell_price,
                order_id: None,
                client_id: None,
                status: GridLevelStatus::Pending,
            });
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

    /// Record a fill and update position tracking.
    pub fn record_fill(&mut self, side: Side, price: Decimal, quantity: Decimal) {
        match side {
            Side::Buy => self.net_position += quantity,
            Side::Sell => self.net_position -= quantity,
        }
        // Rough realized PnL tracking: for a sell, profit = (sell_price - center) * qty
        // For a buy, profit = (center - buy_price) * qty
        let profit = match side {
            Side::Buy => (self.center_price - price) * quantity,
            Side::Sell => (price - self.center_price) * quantity,
        };
        self.realized_pnl += profit;
        self.total_fills += 1;
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
        let level = self.levels.iter_mut().find(|l| match client_id {
            Some(cid) => l.client_id.as_deref() == Some(cid),
            None => l.order_id.as_deref() == Some(order_id),
        })?;
        level.status = GridLevelStatus::Filled;
        let filled = level.clone();
        Some(filled)
    }

    /// Get all levels that need orders placed.
    pub fn pending_levels(&self) -> Vec<&GridLevel> {
        self.levels.iter().filter(|l| l.status == GridLevelStatus::Pending).collect()
    }

    /// Count active (live) orders.
    pub fn active_count(&self) -> usize {
        self.levels.iter().filter(|l| l.status == GridLevelStatus::Active).count()
    }

    /// Check if we're at max position for a given side.
    pub fn at_max_position(&self, side: Side, max_position: Decimal) -> bool {
        match side {
            Side::Buy => self.net_position >= max_position,
            Side::Sell => self.net_position <= -max_position,
        }
    }

    /// Reset all levels to pending (for rebalance).
    pub fn reset_levels(&mut self) {
        for level in &mut self.levels {
            level.order_id = None;
            level.client_id = None;
            level.status = GridLevelStatus::Pending;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> GridConfig {
        GridConfig {
            num_levels: 3,
            spacing_mode: SpacingMode::Geometric,
            grid_spacing: Decimal::from(1), // 1%
            order_size: Decimal::ONE,
            order_type: "PostOnly".to_string(),
            max_position: Decimal::from(5),
            rebalance_threshold_pct: Decimal::from(3),
        }
    }

    #[test]
    fn compute_geometric_levels() {
        let mut state = GridState::new();
        let config = test_config();
        state.compute_levels(Decimal::from(100), &config);

        assert_eq!(state.levels.len(), 6); // 3 buy + 3 sell
        assert_eq!(state.center_price, Decimal::from(100));

        let buys: Vec<_> = state.levels.iter().filter(|l| l.side == Side::Buy).collect();
        let sells: Vec<_> = state.levels.iter().filter(|l| l.side == Side::Sell).collect();

        assert_eq!(buys.len(), 3);
        assert_eq!(sells.len(), 3);

        // Buy levels should be below mid, sell above
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
            grid_spacing: Decimal::from(10), // $10 apart
            ..test_config()
        };
        let mut state = GridState::new();
        state.compute_levels(Decimal::from(100), &config);

        let buys: Vec<Decimal> =
            state.levels.iter().filter(|l| l.side == Side::Buy).map(|l| l.price).collect();

        assert_eq!(buys, vec![Decimal::from(90), Decimal::from(80), Decimal::from(70)]);
    }

    #[test]
    fn rebalance_detection() {
        let mut state = GridState::new();
        state.center_price = Decimal::from(100);

        // 2% drift, threshold 3% -> no rebalance
        assert!(!state.needs_rebalance(Decimal::from(102), Decimal::from(3)));
        // 4% drift -> rebalance
        assert!(state.needs_rebalance(Decimal::from(104), Decimal::from(3)));
        // Negative drift
        assert!(state.needs_rebalance(Decimal::from(96), Decimal::from(3)));
    }

    #[test]
    fn position_tracking() {
        let mut state = GridState::new();
        state.center_price = Decimal::from(100);

        state.record_fill(Side::Buy, Decimal::from(99), Decimal::ONE);
        assert_eq!(state.net_position, Decimal::ONE);
        assert_eq!(state.total_fills, 1);

        state.record_fill(Side::Sell, Decimal::from(101), Decimal::ONE);
        assert_eq!(state.net_position, Decimal::ZERO);
        assert_eq!(state.total_fills, 2);
        // Profit: buy at 99 (center-99 = 1), sell at 101 (101-center = 1), total = 2
        assert_eq!(state.realized_pnl, Decimal::from(2));
    }

    #[test]
    fn max_position_check() {
        let mut state = GridState::new();
        state.net_position = Decimal::from(5);

        assert!(state.at_max_position(Side::Buy, Decimal::from(5)));
        assert!(!state.at_max_position(Side::Sell, Decimal::from(5)));
    }
}
