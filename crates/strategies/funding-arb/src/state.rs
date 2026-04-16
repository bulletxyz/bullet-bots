use std::time::Instant;

use rust_decimal::Decimal;
use serde::Serialize;

use bb_core::types::Side;

/// Lifecycle phase of the arb position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ArbPhase {
    /// No position. Monitoring funding rates.
    Flat,
    /// Orders placed on both legs, waiting for fills.
    Entering,
    /// Both legs filled, collecting funding spread.
    Active,
    /// Closing orders placed on both legs, waiting for fills.
    Exiting,
}

/// One leg of the arb (one exchange).
#[derive(Debug, Clone, Serialize)]
pub struct ArbLeg {
    pub exchange: String,
    pub side: Side,
    pub target_size: Decimal,
    pub filled_size: Decimal,
    pub avg_entry_price: Decimal,
}

impl ArbLeg {
    pub fn new(exchange: String, side: Side, target_size: Decimal) -> Self {
        Self {
            exchange,
            side,
            target_size,
            filled_size: Decimal::ZERO,
            avg_entry_price: Decimal::ZERO,
        }
    }

    pub fn is_filled(&self) -> bool {
        self.filled_size >= self.target_size
    }

    pub fn record_fill(&mut self, price: Decimal, quantity: Decimal) {
        let total_cost =
            self.avg_entry_price * self.filled_size + price * quantity;
        self.filled_size += quantity;
        if !self.filled_size.is_zero() {
            self.avg_entry_price = total_cost / self.filled_size;
        }
    }

    pub fn reset(&mut self) {
        self.filled_size = Decimal::ZERO;
        self.avg_entry_price = Decimal::ZERO;
    }
}

/// Full arb state: phase, legs, rates, and delta tracking.
#[derive(Debug)]
pub struct ArbState {
    pub phase: ArbPhase,
    pub leg_a: Option<ArbLeg>,
    pub leg_b: Option<ArbLeg>,

    /// Cached funding rate per exchange (per hour, raw).
    pub rate_a: Decimal,
    pub rate_b: Decimal,

    /// Mark prices per exchange.
    pub mark_a: Decimal,
    pub mark_b: Decimal,

    /// When the current phase started (for timeout detection).
    pub phase_entered_at: Option<Instant>,

    /// Cumulative realized PnL (from closed arb cycles).
    pub realized_pnl: Decimal,

    /// Total number of completed arb cycles.
    pub cycles_completed: u32,
}

impl ArbState {
    pub fn new() -> Self {
        Self {
            phase: ArbPhase::Flat,
            leg_a: None,
            leg_b: None,
            rate_a: Decimal::ZERO,
            rate_b: Decimal::ZERO,
            mark_a: Decimal::ZERO,
            mark_b: Decimal::ZERO,
            phase_entered_at: None,
            realized_pnl: Decimal::ZERO,
            cycles_completed: 0,
        }
    }

    /// Transition to a new phase, recording the timestamp.
    pub fn transition(&mut self, phase: ArbPhase) {
        tracing::info!(from = ?self.phase, to = ?phase, "Phase transition");
        self.phase = phase;
        self.phase_entered_at = Some(Instant::now());
    }

    /// Signed funding rate spread: rate_a - rate_b.
    /// Positive means A pays more funding than B.
    pub fn rate_spread(&self) -> Decimal {
        self.rate_a - self.rate_b
    }

    /// Absolute funding rate spread.
    pub fn abs_rate_spread(&self) -> Decimal {
        self.rate_spread().abs()
    }

    /// Net delta across both legs. Should be near zero when Active.
    /// Positive = net long, negative = net short.
    pub fn net_delta(&self) -> Decimal {
        let leg_a_delta = self.leg_a.as_ref().map_or(Decimal::ZERO, |l| {
            match l.side {
                Side::Buy => l.filled_size,
                Side::Sell => -l.filled_size,
            }
        });
        let leg_b_delta = self.leg_b.as_ref().map_or(Decimal::ZERO, |l| {
            match l.side {
                Side::Buy => l.filled_size,
                Side::Sell => -l.filled_size,
            }
        });
        leg_a_delta + leg_b_delta
    }

    /// Whether both legs are fully filled.
    pub fn both_legs_filled(&self) -> bool {
        self.leg_a.as_ref().is_some_and(ArbLeg::is_filled)
            && self.leg_b.as_ref().is_some_and(ArbLeg::is_filled)
    }

    /// Check if the phase has timed out.
    pub fn phase_timed_out(&self, timeout_secs: u64) -> bool {
        self.phase_entered_at
            .is_some_and(|t| t.elapsed().as_secs() >= timeout_secs)
    }

    /// Clear legs and go Flat.
    pub fn go_flat(&mut self) {
        self.leg_a = None;
        self.leg_b = None;
        self.transition(ArbPhase::Flat);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_is_flat() {
        let state = ArbState::new();
        assert_eq!(state.phase, ArbPhase::Flat);
        assert_eq!(state.net_delta(), Decimal::ZERO);
    }

    #[test]
    fn rate_spread_sign() {
        let mut state = ArbState::new();
        state.rate_a = Decimal::new(5, 4); // 0.0005
        state.rate_b = Decimal::new(1, 4); // 0.0001
        assert_eq!(state.rate_spread(), Decimal::new(4, 4)); // 0.0004
        assert_eq!(state.abs_rate_spread(), Decimal::new(4, 4));
    }

    #[test]
    fn net_delta_balanced() {
        let mut state = ArbState::new();
        let mut leg_a = ArbLeg::new("bullet".into(), Side::Buy, Decimal::new(1, 2));
        leg_a.filled_size = Decimal::new(1, 2);
        let mut leg_b = ArbLeg::new("hyperliquid".into(), Side::Sell, Decimal::new(1, 2));
        leg_b.filled_size = Decimal::new(1, 2);
        state.leg_a = Some(leg_a);
        state.leg_b = Some(leg_b);
        assert_eq!(state.net_delta(), Decimal::ZERO);
    }

    #[test]
    fn net_delta_imbalanced() {
        let mut state = ArbState::new();
        let mut leg_a = ArbLeg::new("bullet".into(), Side::Buy, Decimal::new(1, 2));
        leg_a.filled_size = Decimal::new(1, 2);
        let mut leg_b = ArbLeg::new("hyperliquid".into(), Side::Sell, Decimal::new(1, 2));
        leg_b.filled_size = Decimal::new(5, 3); // only half filled
        state.leg_a = Some(leg_a);
        state.leg_b = Some(leg_b);
        // net = 0.01 (long) + (-0.005) (short) = 0.005
        assert_eq!(state.net_delta(), Decimal::new(5, 3));
    }

    #[test]
    fn leg_fill_tracking() {
        let mut leg = ArbLeg::new("test".into(), Side::Buy, Decimal::new(1, 2));
        leg.record_fill(Decimal::new(50000, 0), Decimal::new(5, 3));
        leg.record_fill(Decimal::new(50100, 0), Decimal::new(5, 3));
        assert_eq!(leg.filled_size, Decimal::new(1, 2));
        assert!(leg.is_filled());
        // avg = (50000*0.005 + 50100*0.005) / 0.01 = 50050
        assert_eq!(leg.avg_entry_price, Decimal::new(50050, 0));
    }

    #[test]
    fn phase_transitions() {
        let mut state = ArbState::new();
        state.transition(ArbPhase::Entering);
        assert_eq!(state.phase, ArbPhase::Entering);
        assert!(state.phase_entered_at.is_some());

        state.transition(ArbPhase::Active);
        assert_eq!(state.phase, ArbPhase::Active);

        state.go_flat();
        assert_eq!(state.phase, ArbPhase::Flat);
        assert!(state.leg_a.is_none());
    }
}
