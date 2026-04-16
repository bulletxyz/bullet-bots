use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::strategy::{Strategy, StrategyContext};
use bb_core::types::*;
use rust_decimal::Decimal;

use crate::config::FundingArbConfig;
use crate::state::{ArbLeg, ArbPhase, ArbState};

/// Funding rate arbitrage strategy.
///
/// Monitors funding rates on two exchanges. When the differential exceeds
/// `entry_threshold`, opens a delta-neutral position (short the higher-rate
/// exchange, long the lower-rate exchange). Exits when the spread narrows
/// below `exit_threshold`.
pub struct FundingArbStrategy {
    config: FundingArbConfig,
    state: ArbState,
}

impl FundingArbStrategy {
    pub fn new(config: FundingArbConfig) -> Self {
        Self { config, state: ArbState::new() }
    }

    /// Determine which exchange to short (the one paying higher funding)
    /// and which to long. Returns (short_exchange, long_exchange).
    fn pick_sides(&self) -> (&str, &str) {
        if self.state.rate_a > self.state.rate_b {
            // A pays more funding -> short A, long B
            (&self.config.exchange_a, &self.config.exchange_b)
        } else {
            // B pays more funding -> short B, long A
            (&self.config.exchange_b, &self.config.exchange_a)
        }
    }

    /// Check if funding rates pass the anomaly filter.
    fn rates_sane(&self) -> bool {
        self.state.rate_a.abs() < self.config.max_funding_rate
            && self.state.rate_b.abs() < self.config.max_funding_rate
    }

    /// Build an aggressive order at the best opposing price + slippage.
    fn aggressive_order(
        &self,
        ctx: &StrategyContext,
        exchange: &str,
        side: Side,
    ) -> NewOrder {
        let symbol = ctx.config.symbol.clone();
        let slippage = self.config.slippage;

        // Get best price from cached orderbook, or fall back to mark price.
        let base_price = ctx
            .orderbook(exchange)
            .and_then(|book| match side {
                Side::Buy => book.best_ask().map(|l| l.price),
                Side::Sell => book.best_bid().map(|l| l.price),
            })
            .unwrap_or_else(|| {
                if exchange == self.config.exchange_a {
                    self.state.mark_a
                } else {
                    self.state.mark_b
                }
            });

        let price = match side {
            Side::Buy => base_price * (Decimal::ONE + slippage),
            Side::Sell => base_price * (Decimal::ONE - slippage),
        };

        let order_type = match self.config.order_mode.as_str() {
            "passive" => OrderType::PostOnly,
            _ => OrderType::Market,
        };

        NewOrder {
            symbol,
            side,
            order_type,
            price,
            quantity: self.config.order_size,
            client_id: None,
            reduce_only: false,
        }
    }

    /// Enter: place orders on both legs simultaneously.
    async fn enter(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        let (short_ex, long_ex) = self.pick_sides();
        let short_ex = short_ex.to_string();
        let long_ex = long_ex.to_string();

        tracing::info!(
            spread = %self.state.abs_rate_spread(),
            short = %short_ex,
            long = %long_ex,
            "Entering arb position"
        );

        self.state.leg_a = Some(ArbLeg::new(
            short_ex.clone(),
            Side::Sell,
            self.config.order_size,
        ));
        self.state.leg_b = Some(ArbLeg::new(
            long_ex.clone(),
            Side::Buy,
            self.config.order_size,
        ));
        self.state.transition(ArbPhase::Entering);

        // Place both legs. We do them sequentially here; the engine's tick timeout
        // provides the safety net if one hangs.
        let short_orders = [self.aggressive_order(ctx, &short_ex, Side::Sell)];
        let long_orders = [self.aggressive_order(ctx, &long_ex, Side::Buy)];

        let (short_result, long_result) = tokio::join!(
            ctx.place_orders(&short_ex, &short_orders),
            ctx.place_orders(&long_ex, &long_orders),
        );

        if let Err(e) = &short_result {
            tracing::error!(exchange = %short_ex, error = %e, "Failed to place short leg");
        }
        if let Err(e) = &long_result {
            tracing::error!(exchange = %long_ex, error = %e, "Failed to place long leg");
        }

        Ok(())
    }

    /// Exit: place close orders on both legs.
    async fn exit(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        tracing::info!(
            spread = %self.state.abs_rate_spread(),
            "Exiting arb position"
        );
        self.state.transition(ArbPhase::Exiting);

        if let Some(ref leg) = self.state.leg_a {
            let close_side = leg.side.opposite();
            let order = self.aggressive_order(ctx, &leg.exchange, close_side);
            let ex = leg.exchange.clone();
            if let Err(e) = ctx.place_orders(&ex, &[order]).await {
                tracing::error!(exchange = %ex, error = %e, "Failed to place close order");
            }
        }
        if let Some(ref leg) = self.state.leg_b {
            let close_side = leg.side.opposite();
            let order = self.aggressive_order(ctx, &leg.exchange, close_side);
            let ex = leg.exchange.clone();
            if let Err(e) = ctx.place_orders(&ex, &[order]).await {
                tracing::error!(exchange = %ex, error = %e, "Failed to place close order");
            }
        }

        Ok(())
    }

    /// Emergency flatten: cancel everything, market-close all positions.
    async fn emergency_flatten(
        &mut self,
        ctx: &mut StrategyContext,
        reason: &str,
    ) -> Result<(), BotError> {
        tracing::warn!(reason, "Emergency flatten");
        let symbol = ctx.config.symbol.clone();

        // Cancel all on both exchanges.
        let _ = ctx.cancel_all_orders(&self.config.exchange_a, &symbol).await;
        let _ = ctx.cancel_all_orders(&self.config.exchange_b, &symbol).await;

        // Close any filled leg.
        if let Some(ref leg) = self.state.leg_a {
            if !leg.filled_size.is_zero() {
                let order = NewOrder {
                    symbol: symbol.clone(),
                    side: leg.side.opposite(),
                    order_type: OrderType::Market,
                    price: if leg.side == Side::Buy {
                        self.state.mark_a * (Decimal::ONE - self.config.slippage)
                    } else {
                        self.state.mark_a * (Decimal::ONE + self.config.slippage)
                    },
                    quantity: leg.filled_size,
                    client_id: None,
                    reduce_only: true,
                };
                let _ = ctx.place_orders(&leg.exchange, &[order]).await;
            }
        }
        if let Some(ref leg) = self.state.leg_b {
            if !leg.filled_size.is_zero() {
                let order = NewOrder {
                    symbol: symbol.clone(),
                    side: leg.side.opposite(),
                    order_type: OrderType::Market,
                    price: if leg.side == Side::Buy {
                        self.state.mark_b * (Decimal::ONE - self.config.slippage)
                    } else {
                        self.state.mark_b * (Decimal::ONE + self.config.slippage)
                    },
                    quantity: leg.filled_size,
                    client_id: None,
                    reduce_only: true,
                };
                let _ = ctx.place_orders(&leg.exchange, &[order]).await;
            }
        }

        self.state.go_flat();
        Ok(())
    }

    /// Process a fill event, updating the appropriate leg.
    fn apply_fill(&mut self, exchange: &str, side: Side, price: Decimal, quantity: Decimal) {
        let leg = if self.state.leg_a.as_ref().is_some_and(|l| l.exchange == exchange) {
            self.state.leg_a.as_mut()
        } else if self.state.leg_b.as_ref().is_some_and(|l| l.exchange == exchange) {
            self.state.leg_b.as_mut()
        } else {
            return;
        };

        if let Some(leg) = leg {
            if leg.side == side {
                leg.record_fill(price, quantity);
                tracing::info!(
                    exchange,
                    side = %side,
                    price = %price,
                    qty = %quantity,
                    filled = %leg.filled_size,
                    target = %leg.target_size,
                    "Arb leg fill"
                );
            }
        }
    }
}

#[async_trait]
impl Strategy for FundingArbStrategy {
    fn name(&self) -> &str {
        "funding-arb"
    }

    async fn on_start(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();

        // Cancel stale orders on both exchanges.
        tracing::info!("Cancelling stale orders on both exchanges");
        let _ = ctx.cancel_all_orders(&self.config.exchange_a, &symbol).await;
        let _ = ctx.cancel_all_orders(&self.config.exchange_b, &symbol).await;

        tracing::info!(
            exchange_a = %self.config.exchange_a,
            exchange_b = %self.config.exchange_b,
            entry = %self.config.entry_threshold,
            exit = %self.config.exit_threshold,
            size = %self.config.order_size,
            "Funding arb strategy started"
        );

        Ok(())
    }

    async fn on_tick(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        match self.state.phase {
            ArbPhase::Flat => {
                // Log current rates.
                tracing::info!(
                    rate_a = %self.state.rate_a,
                    rate_b = %self.state.rate_b,
                    spread = %self.state.abs_rate_spread(),
                    "Monitoring funding rates"
                );
            }
            ArbPhase::Entering => {
                // Check timeout.
                if self.state.phase_timed_out(self.config.phase_timeout_secs) {
                    self.emergency_flatten(ctx, "entering phase timed out").await?;
                    return Ok(());
                }
                // Check if both legs filled.
                if self.state.both_legs_filled() {
                    tracing::info!("Both legs filled, position active");
                    self.state.transition(ArbPhase::Active);
                }
            }
            ArbPhase::Active => {
                // Delta imbalance check.
                let delta = self.state.net_delta().abs();
                if delta > self.config.max_delta_imbalance {
                    self.emergency_flatten(ctx, "delta imbalance exceeded").await?;
                    return Ok(());
                }

                // Check if spread has narrowed below exit threshold.
                if self.state.abs_rate_spread() < self.config.exit_threshold {
                    self.exit(ctx).await?;
                    return Ok(());
                }

                tracing::info!(
                    spread = %self.state.abs_rate_spread(),
                    delta = %self.state.net_delta(),
                    "Active arb position"
                );
            }
            ArbPhase::Exiting => {
                if self.state.phase_timed_out(self.config.phase_timeout_secs) {
                    self.emergency_flatten(ctx, "exiting phase timed out").await?;
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn on_event(
        &mut self,
        ctx: &mut StrategyContext,
        event: ExchangeEvent,
    ) -> Result<(), BotError> {
        match event {
            ExchangeEvent::MarkPrice { ref exchange, mark_price, funding_rate, .. } => {
                if exchange == &self.config.exchange_a {
                    self.state.mark_a = mark_price;
                    self.state.rate_a = funding_rate;
                } else if exchange == &self.config.exchange_b {
                    self.state.mark_b = mark_price;
                    self.state.rate_b = funding_rate;
                }

                // Entry signal check (only when Flat).
                if self.state.phase == ArbPhase::Flat
                    && self.state.abs_rate_spread() > self.config.entry_threshold
                    && self.rates_sane()
                    && !self.state.mark_a.is_zero()
                    && !self.state.mark_b.is_zero()
                {
                    self.enter(ctx).await?;
                }
            }
            ExchangeEvent::Trade {
                ref exchange, side, price, quantity, ..
            } => {
                self.apply_fill(exchange, side, price, quantity);

                // Check if entering phase is done.
                if self.state.phase == ArbPhase::Entering && self.state.both_legs_filled() {
                    tracing::info!("Both legs filled, position active");
                    self.state.transition(ArbPhase::Active);
                }
            }
            ExchangeEvent::OrderUpdate { ref order, .. } => {
                if order.status == OrderStatus::Filled {
                    self.apply_fill(
                        order.symbol.as_str(), // fallback; exchange name not on Order
                        order.side,
                        order.price,
                        order.quantity,
                    );
                }

                // Exiting: if both legs close, go flat.
                if self.state.phase == ArbPhase::Exiting {
                    // Simplified: when we see filled close orders, reduce leg size.
                    // A more precise implementation would track close fills separately.
                    let a_done = self.state.leg_a.as_ref().map_or(true, |l| l.filled_size.is_zero());
                    let b_done = self.state.leg_b.as_ref().map_or(true, |l| l.filled_size.is_zero());
                    if a_done && b_done {
                        self.state.cycles_completed += 1;
                        tracing::info!(cycles = self.state.cycles_completed, "Arb cycle completed");
                        self.state.go_flat();
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn on_stop(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        tracing::info!("Shutting down funding arb strategy");

        if self.state.phase != ArbPhase::Flat {
            self.emergency_flatten(ctx, "shutdown").await?;
        } else {
            let symbol = ctx.config.symbol.clone();
            let _ = ctx.cancel_all_orders(&self.config.exchange_a, &symbol).await;
            let _ = ctx.cancel_all_orders(&self.config.exchange_b, &symbol).await;
        }

        tracing::info!(
            cycles = self.state.cycles_completed,
            pnl = %self.state.realized_pnl,
            "Funding arb final stats"
        );
        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "phase": format!("{:?}", self.state.phase),
            "rate_a": self.state.rate_a.to_string(),
            "rate_b": self.state.rate_b.to_string(),
            "spread": self.state.abs_rate_spread().to_string(),
            "net_delta": self.state.net_delta().to_string(),
            "mark_a": self.state.mark_a.to_string(),
            "mark_b": self.state.mark_b.to_string(),
            "cycles_completed": self.state.cycles_completed,
            "realized_pnl": self.state.realized_pnl.to_string(),
            "leg_a": self.state.leg_a.as_ref().map(|l| serde_json::json!({
                "exchange": l.exchange,
                "side": format!("{:?}", l.side),
                "filled": l.filled_size.to_string(),
                "target": l.target_size.to_string(),
            })),
            "leg_b": self.state.leg_b.as_ref().map(|l| serde_json::json!({
                "exchange": l.exchange,
                "side": format!("{:?}", l.side),
                "filled": l.filled_size.to_string(),
                "target": l.target_size.to_string(),
            })),
        })
    }
}
