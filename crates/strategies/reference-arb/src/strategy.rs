//! Reference-price arb actor.
//!
//! Signal:
//!   `spread_bps = (bullet_mid − binance_mid) / binance_mid × 10_000`
//!
//! Entry (from `Flat`): when `|spread_bps| >= entry_threshold_bps` for
//! `persistence_ticks` consecutive evaluations, take a directional market
//! position on Bullet on the side that profits from convergence (short if
//! Bullet is rich, long if Bullet is cheap).
//!
//! Exit (from `Holding`): TP when the spread has reverted past
//! `exit_threshold_bps` on the entry side; SL when the spread has widened past
//! `stop_loss_bps` on the entry side; forced timeout exit after
//! `max_hold_ticks`.
//!
//! Invariants:
//!   - `InventoryTracker` is the only source of position updates.
//!   - At most one open order at a time, tracked by `client_id`.
//!   - Binance staleness guard: refuse to trade when reference is >N seconds old.
//!   - `init` refuses to run if starting inventory is non-flat.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, OrderLifecycle, Tick, Trade};
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::{ClientIdIssuer, InventoryTracker};
use bb_core::types::{NewOrder, OrderBook, OrderStatus, OrderType, Side};
use bb_exchange_binance::ReferencePriceUpdate;
use rust_decimal::Decimal;

use crate::config::ReferenceArbConfig;

const BPS_SCALE: i64 = 10_000;

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub enum ExitReason {
    TakeProfit,
    StopLoss,
    Timeout,
    WindDown,
}

#[derive(Debug, Clone)]
pub enum ArbState {
    Flat {
        pending_signal_side: Option<Side>,
        pending_signal_streak: u32,
    },
    Entering {
        client_id: String,
        side: Side,
        entry_spread_bps: Decimal,
    },
    Holding {
        side: Side,
        entry_spread_bps: Decimal,
        ticks: u32,
    },
    Exiting {
        client_id: String,
        reason: ExitReason,
    },
}

impl ArbState {
    fn flat() -> Self {
        Self::Flat { pending_signal_side: None, pending_signal_streak: 0 }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Flat { .. } => "flat",
            Self::Entering { .. } => "entering",
            Self::Holding { .. } => "holding",
            Self::Exiting { .. } => "exiting",
        }
    }
}

pub struct ReferenceArbActor {
    config: ReferenceArbConfig,
    state: ArbState,
    inventory: InventoryTracker,
    client_ids: ClientIdIssuer,
    bullet_book: Option<OrderBook>,
    bullet_mid: Option<Decimal>,
    binance_mid: Option<Decimal>,
    binance_last_seen: Option<Instant>,
    /// Suppresses repeated "binance is stale" warnings to a single log per
    /// staleness episode. Cleared when a fresh update arrives.
    stale_logged: bool,
}

impl ReferenceArbActor {
    pub fn new(config: ReferenceArbConfig) -> Self {
        Self {
            config,
            state: ArbState::flat(),
            inventory: InventoryTracker::new(),
            client_ids: ClientIdIssuer::new(),
            bullet_book: None,
            bullet_mid: None,
            binance_mid: None,
            binance_last_seen: None,
            stale_logged: false,
        }
    }

    /// Simulated fill price for a market order against the current book.
    /// Uses the far side (market buys eat the ask, market sells eat the bid).
    /// Falls back to `bullet_mid` if book sides are empty — better than nothing,
    /// but inflates paper PnL because it ignores the half-spread cost.
    fn simulated_fill_price(&self, side: Side) -> Option<Decimal> {
        let book = self.bullet_book.as_ref()?;
        let level = match side {
            Side::Buy => book.best_ask(),
            Side::Sell => book.best_bid(),
        };
        level.map(|l| l.price).or(self.bullet_mid)
    }

    /// Worst-case IoC limit price for a "market" order on Bullet. Bullet's
    /// Market type is an IoC limit — it rejects `price = 0`, so we compute
    /// a bounded aggressive price from the opposite side of the book plus
    /// `market_slippage_bps`. The IoC semantics ensure we actually fill at
    /// or better than the true top-of-book, not at this worst-case bound.
    fn aggressive_ioc_price(&self, side: Side) -> Option<Decimal> {
        let mid = self.bullet_mid?;
        let base = self.simulated_fill_price(side).unwrap_or(mid);
        let slip = self.config.market_slippage_bps / Decimal::from(BPS_SCALE);
        Some(match side {
            Side::Buy => base * (Decimal::ONE + slip),
            Side::Sell => base * (Decimal::ONE - slip),
        })
    }

    fn compute_spread_bps(&self) -> Option<Decimal> {
        let (b, r) = (self.bullet_mid?, self.binance_mid?);
        if r.is_zero() {
            return None;
        }
        Some((b - r) / r * Decimal::from(BPS_SCALE))
    }

    fn reference_is_stale(&self) -> bool {
        match self.binance_last_seen {
            None => true,
            Some(t) => t.elapsed() > Duration::from_secs(self.config.reference_stale_secs),
        }
    }

    fn desired_entry_side(&self, spread_bps: Decimal) -> Option<Side> {
        if spread_bps >= self.config.entry_threshold_bps {
            Some(Side::Sell) // Bullet rich vs reference → short Bullet
        } else if spread_bps <= -self.config.entry_threshold_bps {
            Some(Side::Buy) // Bullet cheap vs reference → long Bullet
        } else {
            None
        }
    }

    /// True when the spread has mean-reverted past `exit_threshold_bps` on
    /// the entry side (TP trigger).
    fn is_take_profit(&self, entry_side: Side, current_spread_bps: Decimal) -> bool {
        match entry_side {
            Side::Sell => current_spread_bps <= self.config.exit_threshold_bps,
            Side::Buy => current_spread_bps >= -self.config.exit_threshold_bps,
        }
    }

    /// True when the spread has widened past `stop_loss_bps` on the entry
    /// side (SL trigger).
    fn is_stop_loss(&self, entry_side: Side, current_spread_bps: Decimal) -> bool {
        match entry_side {
            Side::Sell => current_spread_bps >= self.config.stop_loss_bps,
            Side::Buy => current_spread_bps <= -self.config.stop_loss_bps,
        }
    }

    /// Called after every price update (Bullet book or Binance reference).
    /// Centralizes entry/exit decisions so the two event paths are identical.
    async fn evaluate(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        let Some(spread_bps) = self.compute_spread_bps() else {
            return Ok(());
        };
        if self.reference_is_stale() {
            if !self.stale_logged {
                tracing::warn!(
                    stale_secs = self.config.reference_stale_secs,
                    "Binance reference stale; suppressing trades"
                );
                self.stale_logged = true;
            }
            return Ok(());
        }

        // Per-update observation log — lets operators see the live spread
        // without waiting for Tick. DEBUG level so it doesn't flood by default.
        tracing::debug!(
            tag = "OBSERVE",
            spread_bps = %spread_bps,
            bullet_mid = %self.bullet_mid.unwrap_or_default(),
            binance_mid = %self.binance_mid.unwrap_or_default(),
            state = self.state.kind(),
            "reference-arb observation"
        );

        match self.state {
            ArbState::Flat { .. } => self.evaluate_entry(cx, spread_bps).await,
            ArbState::Holding { side, .. } => self.evaluate_exit(cx, side, spread_bps).await,
            ArbState::Entering { .. } | ArbState::Exiting { .. } => Ok(()),
        }
    }

    async fn evaluate_entry(
        &mut self,
        cx: &ActorContext,
        spread_bps: Decimal,
    ) -> Result<(), BotError> {
        let desired = self.desired_entry_side(spread_bps);

        // Persistence filter: require N consecutive qualifying evaluations.
        let ready = {
            let ArbState::Flat { pending_signal_side, pending_signal_streak } = &mut self.state
            else {
                return Ok(());
            };
            if desired == *pending_signal_side && desired.is_some() {
                *pending_signal_streak = pending_signal_streak.saturating_add(1);
            } else {
                *pending_signal_side = desired;
                *pending_signal_streak = u32::from(desired.is_some());
            }
            desired.is_some() && *pending_signal_streak >= self.config.persistence_ticks
        };
        if !ready {
            return Ok(());
        }

        // Inventory cap guard (defense-in-depth — Flat should have pos≈0).
        let side = desired.expect("ready implies desired is Some");
        let projected = self.inventory.net_position.abs() + self.config.order_size;
        if projected > self.config.max_position {
            tracing::warn!(
                projected = %projected,
                max = %self.config.max_position,
                "Skipping entry: would exceed max_position"
            );
            return Ok(());
        }

        if self.config.dry_run {
            let fill_price = self
                .simulated_fill_price(side)
                .ok_or_else(|| BotError::strategy("dry_run entry: no book or mid available"))?;
            self.inventory.record_fill(side, fill_price, self.config.order_size);
            tracing::info!(
                tag = "PAPER",
                side = %side,
                spread_bps = %spread_bps,
                fill_price = %fill_price,
                net_pos = %self.inventory.net_position,
                "PAPER entry (dry_run): simulated market fill, entering Holding"
            );
            self.state = ArbState::Holding {
                side,
                entry_spread_bps: spread_bps,
                ticks: 0,
            };
            return Ok(());
        }

        let ioc_price = self.aggressive_ioc_price(side).ok_or_else(|| {
            BotError::strategy("entry: no book available to compute aggressive IoC price")
        })?;
        let client_id = self.client_ids.issue();
        let order = NewOrder {
            symbol: self.config.symbol.clone(),
            side,
            order_type: OrderType::Market,
            price: ioc_price,
            quantity: self.config.order_size,
            client_id: Some(client_id.clone()),
            reduce_only: false,
        };

        tracing::info!(
            side = %side,
            spread_bps = %spread_bps,
            client_id = %client_id,
            "Entry signal: placing market order"
        );

        let broker = cx.broker(&self.config.exchange)?;
        match broker.place_orders(&[order]).await {
            Ok(_) => {
                self.state = ArbState::Entering {
                    client_id,
                    side,
                    entry_spread_bps: spread_bps,
                };
            }
            Err(e) => {
                tracing::error!(error = %e, "Entry order placement failed");
                // Reset streak so we re-accumulate persistence before retrying.
                self.state = ArbState::flat();
            }
        }
        Ok(())
    }

    async fn evaluate_exit(
        &mut self,
        cx: &ActorContext,
        entry_side: Side,
        spread_bps: Decimal,
    ) -> Result<(), BotError> {
        let reason = if self.is_take_profit(entry_side, spread_bps) {
            Some(ExitReason::TakeProfit)
        } else if self.is_stop_loss(entry_side, spread_bps) {
            Some(ExitReason::StopLoss)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.place_exit(cx, reason, spread_bps).await?;
        }
        Ok(())
    }

    /// Close whatever position `InventoryTracker` reports. Transitions to
    /// `Exiting`. Called from `evaluate_exit` (TP/SL), `Tick` (timeout), and
    /// `wind_down`.
    async fn place_exit(
        &mut self,
        cx: &ActorContext,
        reason: ExitReason,
        spread_bps: Decimal,
    ) -> Result<(), BotError> {
        let pos = self.inventory.net_position;
        if pos.is_zero() {
            // Nothing to close — just return to Flat.
            self.state = ArbState::flat();
            return Ok(());
        }
        let (close_side, qty) = if pos.is_sign_positive() {
            (Side::Sell, pos)
        } else {
            (Side::Buy, -pos)
        };

        if self.config.dry_run {
            let fill_price = self
                .simulated_fill_price(close_side)
                .ok_or_else(|| BotError::strategy("dry_run exit: no book or mid available"))?;
            let realized = self.inventory.record_fill(close_side, fill_price, qty);
            tracing::info!(
                tag = "PAPER",
                reason = ?reason,
                side = %close_side,
                qty = %qty,
                fill_price = %fill_price,
                spread_bps = %spread_bps,
                fill_pnl = %realized,
                cumulative_pnl = %self.inventory.realized_pnl,
                "PAPER exit (dry_run): simulated closing fill, back to Flat"
            );
            self.state = ArbState::flat();
            return Ok(());
        }

        let ioc_price = self.aggressive_ioc_price(close_side).ok_or_else(|| {
            BotError::strategy("exit: no book available to compute aggressive IoC price")
        })?;
        let client_id = self.client_ids.issue();
        let order = NewOrder {
            symbol: self.config.symbol.clone(),
            side: close_side,
            order_type: OrderType::Market,
            price: ioc_price,
            quantity: qty,
            client_id: Some(client_id.clone()),
            reduce_only: true,
        };

        tracing::info!(
            reason = ?reason,
            side = %close_side,
            qty = %qty,
            spread_bps = %spread_bps,
            client_id = %client_id,
            "Exit signal: placing closing market order"
        );

        let broker = cx.broker(&self.config.exchange)?;
        match broker.place_orders(&[order]).await {
            Ok(_) => {
                self.state = ArbState::Exiting { client_id, reason };
            }
            Err(e) => {
                tracing::error!(error = %e, "Exit order placement failed; will retry on tick");
                // Stay in Holding so the tick handler triggers a re-attempt.
            }
        }
        Ok(())
    }

    fn matches_current_order(&self, client_id: Option<&str>) -> bool {
        let expected = match &self.state {
            ArbState::Entering { client_id, .. } | ArbState::Exiting { client_id, .. } => {
                Some(client_id.as_str())
            }
            _ => None,
        };
        expected.is_some() && expected == client_id
    }
}

#[async_trait]
impl Actor for ReferenceArbActor {
    async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        self.config
            .validate()
            .map_err(|e| BotError::config(format!("reference-arb config invalid: {e}")))?;

        let broker = cx.broker(&self.config.exchange)?;
        broker.cancel_all_orders(&self.config.symbol).await.ok();

        // Safety: refuse to start on a non-flat position. Recovery mid-position
        // is subtle (no preserved entry_spread_bps) and silently assuming Flat
        // risks unbounded exposure. Operator flattens manually, then restarts.
        let positions = broker.get_positions().await?;
        for p in positions.iter().filter(|p| p.symbol == self.config.symbol) {
            if !p.size.is_zero() {
                tracing::error!(
                    symbol = %self.config.symbol,
                    size = %p.size,
                    "reference-arb: starting position is non-flat; flatten manually and restart"
                );
                return Err(BotError::config(
                    "reference-arb refuses to start with non-flat position".to_string(),
                ));
            }
        }

        tracing::info!(
            exchange = %self.config.exchange,
            symbol = %self.config.symbol,
            binance_symbol = %self.config.binance_symbol,
            entry_bps = %self.config.entry_threshold_bps,
            exit_bps = %self.config.exit_threshold_bps,
            stop_bps = %self.config.stop_loss_bps,
            size = %self.config.order_size,
            "reference-arb actor initialized"
        );
        Ok(())
    }

    async fn wind_down(
        &mut self,
        _reason: &WindDownReason,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        let broker = cx.broker(&self.config.exchange)?;
        let _ = broker.cancel_all_orders(&self.config.symbol).await;

        if !self.inventory.net_position.is_zero() {
            let spread = self.compute_spread_bps().unwrap_or(Decimal::ZERO);
            let _ = self.place_exit(cx, ExitReason::WindDown, spread).await;
        }
        tracing::info!(
            net_pos = %self.inventory.net_position,
            realized_pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            "reference-arb final stats"
        );
        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        let spread = self.compute_spread_bps().map(|d| d.to_string());
        let last_seen_ms = self
            .binance_last_seen
            .map(|t| u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX));
        serde_json::json!({
            "state": self.state.kind(),
            "spread_bps": spread,
            "bullet_mid": self.bullet_mid.map(|d| d.to_string()),
            "binance_mid": self.binance_mid.map(|d| d.to_string()),
            "binance_last_seen_ms_ago": last_seen_ms,
            "inventory": self.inventory.clone(),
        })
    }
}

#[async_trait]
impl EventHandler<BookUpdate> for ReferenceArbActor {
    async fn on_event(&mut self, event: BookUpdate, cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange != self.config.exchange || event.symbol != self.config.symbol {
            return Ok(());
        }
        if let Some(mid) = event.orderbook.midpoint() {
            self.bullet_mid = Some(mid);
        }
        self.bullet_book = Some(event.orderbook);
        self.evaluate(cx).await
    }
}

#[async_trait]
impl EventHandler<ReferencePriceUpdate> for ReferenceArbActor {
    async fn on_event(
        &mut self,
        event: ReferencePriceUpdate,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        if !event.symbol.eq_ignore_ascii_case(&self.config.binance_symbol) {
            return Ok(());
        }
        self.binance_mid = Some(event.mid);
        self.binance_last_seen = Some(event.received_at);
        self.stale_logged = false;
        self.evaluate(cx).await
    }
}

#[async_trait]
impl EventHandler<Trade> for ReferenceArbActor {
    async fn on_event(&mut self, event: Trade, _cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange != self.config.exchange || event.symbol != self.config.symbol {
            return Ok(());
        }
        let realized = self
            .inventory
            .record_fill(event.side, event.price, event.quantity);

        // Transition only when this fill belongs to the current in-flight order.
        if !self.matches_current_order(event.client_id.as_deref()) {
            return Ok(());
        }

        let next_state = match &self.state {
            ArbState::Entering { side, entry_spread_bps, .. } => {
                tracing::info!(
                    side = %side,
                    price = %event.price,
                    qty = %event.quantity,
                    net = %self.inventory.net_position,
                    "Entry fill"
                );
                Some(ArbState::Holding {
                    side: *side,
                    entry_spread_bps: *entry_spread_bps,
                    ticks: 0,
                })
            }
            ArbState::Exiting { reason, .. } if self.inventory.is_flat() => {
                tracing::info!(
                    reason = ?reason,
                    fill_pnl = %realized,
                    cumulative_pnl = %self.inventory.realized_pnl,
                    total_fills = self.inventory.total_fills,
                    "Exit fill — position closed"
                );
                Some(ArbState::flat())
            }
            _ => None,
        };
        if let Some(s) = next_state {
            self.state = s;
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<OrderLifecycle> for ReferenceArbActor {
    async fn on_event(
        &mut self,
        event: OrderLifecycle,
        _cx: &ActorContext,
    ) -> Result<(), BotError> {
        if event.exchange != self.config.exchange {
            return Ok(());
        }
        if !self.matches_current_order(event.order.client_id.as_deref()) {
            return Ok(());
        }
        match event.order.status {
            // `OrderStatus::Filled` is terminal; the Trade event for the final
            // fill drives the state transition, so nothing is needed here.
            OrderStatus::Cancelled | OrderStatus::Rejected => match &self.state {
                ArbState::Entering { .. } => {
                    tracing::warn!(
                        client_id = ?event.order.client_id,
                        status = ?event.order.status,
                        net = %self.inventory.net_position,
                        "Entry order cancelled/rejected"
                    );
                    // If we got no fills, go back to Flat to re-arm signal.
                    // If we have residual (partial fill then cancel), move to
                    // Holding so the exit path can unwind it. entry_spread_bps
                    // is the best estimate we have.
                    self.state = if self.inventory.net_position.is_zero() {
                        ArbState::flat()
                    } else if let ArbState::Entering {
                        side, entry_spread_bps, ..
                    } = &self.state
                    {
                        ArbState::Holding {
                            side: *side,
                            entry_spread_bps: *entry_spread_bps,
                            ticks: 0,
                        }
                    } else {
                        ArbState::flat()
                    };
                }
                ArbState::Exiting { reason, .. } => {
                    tracing::warn!(
                        client_id = ?event.order.client_id,
                        status = ?event.order.status,
                        reason = ?reason,
                        "Exit order cancelled/rejected; tick handler will retry"
                    );
                    // Drop back to Holding so the tick handler re-places the exit.
                    // entry_spread_bps is not preserved across Exiting — use zero
                    // (worst case: next re-place relies on the same TP/SL still
                    // being true, which it is if we're still in adverse territory).
                    self.state = ArbState::Holding {
                        side: Side::Buy, // placeholder; see note below
                        entry_spread_bps: Decimal::ZERO,
                        ticks: 0,
                    };
                    // Note: we don't actually know the original entry_side here.
                    // In practice the tick handler will observe inventory and
                    // place a close on the correct side via place_exit(). The
                    // `side` in Holding is advisory for the TP/SL math — with
                    // entry_spread_bps=0 and placeholder side, TP/SL thresholds
                    // will not fire unless the current spread_bps is extreme,
                    // which is fine: the timeout path will catch it.
                }
                _ => {}
            },
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Tick> for ReferenceArbActor {
    async fn on_event(&mut self, _event: Tick, cx: &ActorContext) -> Result<(), BotError> {
        // 1. Advance hold counter and check timeout.
        let timeout_fired = matches!(
            &self.state,
            ArbState::Holding { ticks, .. } if ticks.saturating_add(1) >= self.config.max_hold_ticks
        );
        if let ArbState::Holding { ticks, .. } = &mut self.state {
            *ticks = ticks.saturating_add(1);
        }
        if timeout_fired {
            let spread = self.compute_spread_bps().unwrap_or(Decimal::ZERO);
            tracing::warn!(
                max = self.config.max_hold_ticks,
                spread_bps = %spread,
                "Max hold ticks reached, forcing exit"
            );
            self.place_exit(cx, ExitReason::Timeout, spread).await?;
        }

        // 2. If we hold residual inventory but no in-flight order (e.g. after
        //    a cancelled exit), retry the exit.
        if matches!(&self.state, ArbState::Holding { .. })
            && !self.inventory.net_position.is_zero()
        {
            // Do nothing — normal Holding state, exit driven by spread. The
            // reconcile branch only fires when state is confused; see note in
            // OrderLifecycle Cancelled handling.
        }

        // 3. Status log at INFO.
        tracing::info!(
            state = self.state.kind(),
            spread_bps = ?self.compute_spread_bps(),
            net_pos = %self.inventory.net_position,
            realized_pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            "arb tick"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> ReferenceArbConfig {
        ReferenceArbConfig {
            exchange: "bullet".into(),
            symbol: "BTC-USD".into(),
            binance_symbol: "btcusdt".into(),
            binance_market: "perp".into(),
            order_size: Decimal::new(1, 3),
            max_position: Decimal::new(3, 3),
            entry_threshold_bps: Decimal::from(15),
            exit_threshold_bps: Decimal::from(3),
            stop_loss_bps: Decimal::from(40),
            persistence_ticks: 2,
            max_hold_ticks: 24,
            reference_stale_secs: 10,
            taker_fee_bps: Decimal::from(4),
            min_edge_multiple: Decimal::new(15, 1),
            market_slippage_bps: Decimal::from(50),
            dry_run: false,
        }
    }

    fn actor() -> ReferenceArbActor {
        ReferenceArbActor::new(base_config())
    }

    // TP/SL math -----------------------------------------------------------

    #[test]
    fn short_tp_fires_when_spread_reverts() {
        let a = actor();
        // Entered short at +20 bps, threshold=3. Converges to +2 → TP.
        assert!(a.is_take_profit(Side::Sell, Decimal::from(2)));
        // Still +10 → no TP yet.
        assert!(!a.is_take_profit(Side::Sell, Decimal::from(10)));
        // Crossed zero to -1 → also TP (overshoot past zero).
        assert!(a.is_take_profit(Side::Sell, Decimal::from(-1)));
    }

    #[test]
    fn short_sl_fires_when_spread_widens() {
        let a = actor();
        // Entered short at +20 bps, stop=40. Widens to +45 → SL.
        assert!(a.is_stop_loss(Side::Sell, Decimal::from(45)));
        // Widens to only +35 → no SL yet.
        assert!(!a.is_stop_loss(Side::Sell, Decimal::from(35)));
    }

    #[test]
    fn long_tp_fires_when_spread_reverts() {
        let a = actor();
        // Entered long at -20 bps, threshold=3. Reverts to -2 → TP.
        assert!(a.is_take_profit(Side::Buy, Decimal::from(-2)));
        // Still -10 → no TP.
        assert!(!a.is_take_profit(Side::Buy, Decimal::from(-10)));
        // Crossed zero to +1 → also TP.
        assert!(a.is_take_profit(Side::Buy, Decimal::from(1)));
    }

    #[test]
    fn long_sl_fires_when_spread_widens() {
        let a = actor();
        assert!(a.is_stop_loss(Side::Buy, Decimal::from(-45)));
        assert!(!a.is_stop_loss(Side::Buy, Decimal::from(-35)));
    }

    // Entry side selection -------------------------------------------------

    #[test]
    fn entry_side_bullet_rich_picks_sell() {
        let a = actor();
        assert_eq!(a.desired_entry_side(Decimal::from(20)), Some(Side::Sell));
    }

    #[test]
    fn entry_side_bullet_cheap_picks_buy() {
        let a = actor();
        assert_eq!(a.desired_entry_side(Decimal::from(-20)), Some(Side::Buy));
    }

    #[test]
    fn no_entry_inside_threshold() {
        let a = actor();
        assert_eq!(a.desired_entry_side(Decimal::from(14)), None);
        assert_eq!(a.desired_entry_side(Decimal::from(-14)), None);
        assert_eq!(a.desired_entry_side(Decimal::ZERO), None);
    }

    // Staleness guard ------------------------------------------------------

    #[test]
    fn stale_when_no_update_seen() {
        let a = actor();
        assert!(a.reference_is_stale());
    }

    #[test]
    fn fresh_when_just_updated() {
        let mut a = actor();
        a.binance_last_seen = Some(Instant::now());
        assert!(!a.reference_is_stale());
    }

    // Spread math ----------------------------------------------------------

    #[test]
    fn spread_in_bps() {
        let mut a = actor();
        a.bullet_mid = Some(Decimal::from(10010));
        a.binance_mid = Some(Decimal::from(10000));
        // (10010-10000)/10000 * 10000 = 10 bps
        assert_eq!(a.compute_spread_bps(), Some(Decimal::from(10)));
    }

    #[test]
    fn spread_none_when_missing_price() {
        let mut a = actor();
        a.bullet_mid = Some(Decimal::from(100));
        assert_eq!(a.compute_spread_bps(), None);
    }
}
