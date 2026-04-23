//! Funding arb state types. The actor owns position tracking via shared
//! `InventoryTracker`s (one per venue) rather than bespoke fill-counting.

use std::time::Instant;

use rust_decimal::Decimal;
use serde::Serialize;

use bb_core::types::Side;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub enum ArbPhase {
    /// No position. Monitoring funding rates.
    #[default]
    Flat,
    /// Entry orders placed on both legs, waiting for fills.
    Entering,
    /// Both legs filled, collecting funding spread.
    Active,
    /// Exit orders placed on both legs, waiting for fills.
    Exiting,
}

/// One leg of the arb — which venue, which direction, and what target size.
/// Position is tracked elsewhere (`InventoryTracker` per venue), so this is
/// just the intent we're working toward.
#[derive(Debug, Clone, Serialize)]
pub struct ArbLeg {
    pub exchange: String,
    pub entry_side: Side,
    pub target_size: Decimal,
}

/// Lightweight top-level state owned by the actor: phase, legs, and the
/// cached rates/marks the strategy reads. Inventory lives elsewhere.
#[derive(Debug, Default)]
pub struct ArbState {
    pub phase: ArbPhase,
    pub leg_a: Option<ArbLeg>,
    pub leg_b: Option<ArbLeg>,
    pub rate_a: Decimal,
    pub rate_b: Decimal,
    pub mark_a: Decimal,
    pub mark_b: Decimal,
    pub phase_entered_at: Option<Instant>,
    pub cycles_completed: u32,
}

impl ArbState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn transition(&mut self, phase: ArbPhase) {
        tracing::info!(from = ?self.phase, to = ?phase, "Phase transition");
        self.phase = phase;
        self.phase_entered_at = Some(Instant::now());
    }

    /// Signed funding rate spread: rate_a - rate_b. Positive means A pays more.
    pub fn rate_spread(&self) -> Decimal {
        self.rate_a - self.rate_b
    }

    pub fn abs_rate_spread(&self) -> Decimal {
        self.rate_spread().abs()
    }

    pub fn phase_timed_out(&self, timeout_secs: u64) -> bool {
        self.phase_entered_at
            .is_some_and(|t| t.elapsed().as_secs() >= timeout_secs)
    }

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
    }

    #[test]
    fn rate_spread_sign() {
        let mut state = ArbState::new();
        state.rate_a = Decimal::new(5, 4);
        state.rate_b = Decimal::new(1, 4);
        assert_eq!(state.rate_spread(), Decimal::new(4, 4));
        assert_eq!(state.abs_rate_spread(), Decimal::new(4, 4));
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
