use std::time::{Duration, Instant};

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::strategy::{Strategy, StrategyContext};
use bb_core::types::*;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::Serialize;

use crate::config::AvellanedaStoikovConfig;
use crate::model::{LadderRung, Quote, SpreadBounds, quote_ladder};
use crate::volatility::Volatility;

/// A single live quote on one rung of the ladder.
#[derive(Debug, Clone, Serialize)]
struct QuoteSlot {
    side: Side,
    level: usize,
    price: Decimal,
    size: Decimal,
    client_id: Option<String>,
    order_id: Option<String>,
    placed_at: Option<std::time::SystemTime>,
}

/// Avellaneda-Stoikov market-making strategy.
pub struct AvellanedaStoikovStrategy {
    config: AvellanedaStoikovConfig,
    exchange_name: String,
    // Runtime state — one slot per (side, level).
    slots: Vec<QuoteSlot>,
    net_position: Decimal,
    realized_pnl: Decimal,
    total_fills: u64,
    next_client_id: u64,
    last_quote_at: Option<Instant>,
    volatility: Volatility,
    last_mid: Option<Decimal>,
}

impl AvellanedaStoikovStrategy {
    pub fn new(config: AvellanedaStoikovConfig, exchange_name: String) -> Self {
        let volatility = Volatility::new(config.vol_window_secs);
        Self {
            config,
            exchange_name,
            slots: Vec::new(),
            net_position: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            total_fills: 0,
            next_client_id: 1,
            last_quote_at: None,
            volatility,
            last_mid: None,
        }
    }

    fn issue_client_id(&mut self) -> String {
        let id = self.next_client_id;
        self.next_client_id += 1;
        id.to_string()
    }

    fn parse_order_type(&self) -> OrderType {
        match self.config.order_type.as_str() {
            "PostOnly" | "post_only" => OrderType::PostOnly,
            "Market" | "market" => OrderType::Market,
            _ => OrderType::Limit,
        }
    }

    /// Refuse to start if the configured min-half-spread floor can't cover
    /// round-trip maker fees. Analogue of the grid strategy's fee-floor check.
    fn check_fee_floor(&self) -> Result<(), BotError> {
        let Some(fees) = &self.config.fees else {
            tracing::warn!(
                "No [strategy.avellaneda-stoikov.fees] configured — skipping fee-floor check."
            );
            return Ok(());
        };
        let round_trip_bps = Decimal::from(2) * fees.maker_bps;
        let required_bps = round_trip_bps * fees.min_spread_fee_multiplier;
        // Full spread = 2 × half-spread; we compare full spread to required.
        let min_full_spread_bps = Decimal::from(2) * self.config.min_half_spread_bps;

        tracing::info!(
            min_full_spread_bps = %min_full_spread_bps,
            required_bps = %required_bps,
            maker_bps = %fees.maker_bps,
            "A-S fee-floor check"
        );

        if min_full_spread_bps < required_bps {
            return Err(BotError::strategy(format!(
                "min_half_spread_bps × 2 = {min_full_spread_bps} bps is below required \
                 {required_bps} bps ({} × round-trip maker fee). Raise min_half_spread_bps.",
                fees.min_spread_fee_multiplier
            )));
        }
        Ok(())
    }

    /// Build the quote ladder for the current state. Returns `None` if we
    /// don't yet have a volatility estimate (first few ticks after startup).
    fn build_ladder(&self, mid: Decimal) -> Option<(Quote, Vec<LadderRung>)> {
        let sigma = self.volatility.sigma()?;
        let mid_f = mid.to_f64()?;
        let gamma = self.config.gamma.to_f64()?;
        let kappa = self.config.kappa.to_f64()?;
        let tau = self.config.order_horizon_secs as f64;
        let max_pos = self.config.max_position.to_f64()?;
        let target = self.config.inventory_target.to_f64().unwrap_or(0.0);
        let pos = self.net_position.to_f64()?;
        let inv_norm = if max_pos > 0.0 { (pos - target) / max_pos } else { 0.0 };
        let level_step_bps = self.config.order_level_spread_bps.to_f64()?;
        let levels = self.config.order_levels.max(1);

        Some(quote_ladder(
            mid_f,
            inv_norm,
            gamma,
            kappa,
            sigma,
            tau,
            SpreadBounds {
                min_half_spread_bps: self.config.min_half_spread_bps.to_f64()?,
                max_half_spread_bps: self.config.max_half_spread_bps.to_f64()?,
            },
            levels,
            level_step_bps,
        ))
    }

    /// Whether we should re-quote this tick. True if no quotes are live yet,
    /// or if `order_refresh_secs` has elapsed since the last re-quote.
    fn should_refresh(&self, now: Instant) -> bool {
        match self.last_quote_at {
            None => true,
            Some(last) => {
                now.duration_since(last)
                    >= Duration::from_secs(self.config.order_refresh_secs.max(1))
            }
        }
    }

    /// Cancel every live quote on the exchange. Clears local state
    /// optimistically; the next refresh will place a fresh ladder.
    async fn cancel_live_quotes(&mut self, ctx: &StrategyContext) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();
        let mut cancels: Vec<CancelOrder> = Vec::new();
        for slot in &self.slots {
            let (oid, cid) = (slot.order_id.clone(), slot.client_id.clone());
            if oid.is_none() && cid.is_none() {
                continue;
            }
            cancels.push(CancelOrder {
                symbol: symbol.clone(),
                order_id: oid.unwrap_or_default(),
                client_id: cid,
            });
        }
        if !cancels.is_empty() {
            ctx.cancel_orders(&self.exchange_name, &cancels).await?;
        }
        self.slots.clear();
        Ok(())
    }

    /// Per-level order size with optional progressive step.
    fn size_for_level(&self, level: usize) -> Decimal {
        let step = self.config.order_level_amount_step;
        if step.is_zero() {
            return self.config.order_size;
        }
        self.config.order_size * (Decimal::ONE + step * Decimal::from(level as u64))
    }

    /// Compute the fresh quote ladder and place it. Skips a side if doing so
    /// would push past `max_position`.
    async fn refresh_quotes(
        &mut self,
        ctx: &StrategyContext,
        mid: Decimal,
    ) -> Result<(), BotError> {
        let Some((inner, rungs)) = self.build_ladder(mid) else {
            tracing::debug!("Not enough volatility samples yet to quote");
            return Ok(());
        };

        let can_buy = self.net_position < self.config.max_position;
        let can_sell = self.net_position > -self.config.max_position;

        self.cancel_live_quotes(ctx).await?;

        let order_type = self.parse_order_type();
        let symbol = ctx.config.symbol.clone();

        let mut orders: Vec<NewOrder> = Vec::new();
        // Parallel to `orders`: (side, level, price, size, client_id).
        let mut intents: Vec<(Side, usize, Decimal, Decimal, String)> = Vec::new();

        for rung in &rungs {
            let size = self.size_for_level(rung.level);
            if can_buy {
                let Some(bid_price) = Decimal::from_f64(rung.bid) else {
                    return Err(BotError::strategy("Non-finite bid from A-S ladder"));
                };
                let cid = self.issue_client_id();
                orders.push(NewOrder {
                    symbol: symbol.clone(),
                    side: Side::Buy,
                    order_type,
                    price: bid_price,
                    quantity: size,
                    client_id: Some(cid.clone()),
                    reduce_only: false,
                });
                intents.push((Side::Buy, rung.level, bid_price, size, cid));
            }
            if can_sell {
                let Some(ask_price) = Decimal::from_f64(rung.ask) else {
                    return Err(BotError::strategy("Non-finite ask from A-S ladder"));
                };
                let cid = self.issue_client_id();
                orders.push(NewOrder {
                    symbol: symbol.clone(),
                    side: Side::Sell,
                    order_type,
                    price: ask_price,
                    quantity: size,
                    client_id: Some(cid.clone()),
                    reduce_only: false,
                });
                intents.push((Side::Sell, rung.level, ask_price, size, cid));
            }
        }

        if orders.is_empty() {
            tracing::warn!(
                net_pos = %self.net_position,
                max = %self.config.max_position,
                "At max position on both sides; skipping refresh"
            );
            return Ok(());
        }

        let results = ctx.place_orders(&self.exchange_name, &orders).await?;

        for ((side, level, price, size, cid), res) in intents.into_iter().zip(results.iter()) {
            if !res.success {
                tracing::warn!(
                    side = %side,
                    level,
                    price = %price,
                    error = res.error.as_deref().unwrap_or("unknown"),
                    "Failed to place A-S quote"
                );
                continue;
            }
            self.slots.push(QuoteSlot {
                side,
                level,
                price,
                size,
                client_id: Some(cid),
                order_id: if res.order_id.is_empty() {
                    None
                } else {
                    Some(res.order_id.clone())
                },
                placed_at: Some(std::time::SystemTime::now()),
            });
        }

        self.last_quote_at = Some(Instant::now());
        tracing::info!(
            levels = rungs.len(),
            inner_bid = %Decimal::from_f64(inner.bid).unwrap_or_default(),
            inner_ask = %Decimal::from_f64(inner.ask).unwrap_or_default(),
            reservation = %inner.reservation_price,
            inner_half_spread = %inner.half_spread,
            net_pos = %self.net_position,
            "A-S ladder refreshed"
        );
        Ok(())
    }

    /// Apply a fill: update position/PnL, clear the filled side, and trigger
    /// an immediate re-quote on the next tick by nulling `last_quote_at`.
    fn record_fill(
        &mut self,
        client_id: Option<&str>,
        order_id: &str,
        side: Side,
        price: Decimal,
        quantity: Decimal,
    ) {
        match side {
            Side::Buy => self.net_position += quantity,
            Side::Sell => self.net_position -= quantity,
        }
        // Rough realized PnL: use mid (last_mid) as anchor; without a proper
        // average-cost tracker this is an approximation, matching the grid
        // strategy's approach.
        let anchor = self.last_mid.unwrap_or(price);
        let profit = match side {
            Side::Buy => (anchor - price) * quantity,
            Side::Sell => (price - anchor) * quantity,
        };
        self.realized_pnl += profit;
        self.total_fills += 1;

        // Drop the filled rung; the next refresh will rebuild the full ladder
        // around the new reservation price.
        self.slots.retain(|slot| {
            let cid_match =
                client_id.map_or(false, |c| slot.client_id.as_deref() == Some(c));
            let oid_match = slot.order_id.as_deref() == Some(order_id);
            !(cid_match || oid_match)
        });

        // Force a re-quote on the next tick so both sides follow the shifted
        // reservation price.
        self.last_quote_at = None;
    }
}

#[async_trait]
impl Strategy for AvellanedaStoikovStrategy {
    fn name(&self) -> &str {
        "avellaneda-stoikov"
    }

    async fn on_start(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();
        let exchange = &self.exchange_name;

        self.check_fee_floor()?;

        tracing::info!("Cancelling stale orders");
        ctx.cancel_all_orders(exchange, &symbol).await?;

        let book = ctx.get_orderbook(exchange, &symbol, 20).await?;
        let mid = book.midpoint().ok_or_else(|| {
            BotError::strategy("No orderbook data available to compute mid price")
        })?;

        self.last_mid = Some(mid);
        if let Some(m) = mid.to_f64() {
            self.volatility.push(m, Instant::now());
        }

        tracing::info!(
            mid = %mid,
            gamma = %self.config.gamma,
            kappa = %self.config.kappa,
            tau = self.config.order_horizon_secs,
            "A-S strategy started — awaiting volatility samples before first quote"
        );
        // First quote is deferred until we have ≥2 vol samples; handled in
        // on_tick once the estimator is primed.
        Ok(())
    }

    async fn on_tick(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        let exchange = self.exchange_name.clone();

        let mid = match ctx.orderbook(&exchange).and_then(|b| b.midpoint()) {
            Some(m) => m,
            None => return Ok(()),
        };
        self.last_mid = Some(mid);
        if let Some(m) = mid.to_f64() {
            self.volatility.push(m, Instant::now());
        }

        let now = Instant::now();
        if self.should_refresh(now) {
            self.refresh_quotes(ctx, mid).await?;
        }

        let best_bid = self
            .slots
            .iter()
            .filter(|s| s.side == Side::Buy)
            .map(|s| s.price)
            .max();
        let best_ask = self
            .slots
            .iter()
            .filter(|s| s.side == Side::Sell)
            .map(|s| s.price)
            .min();
        tracing::info!(
            net_pos = %self.net_position,
            pnl = %self.realized_pnl,
            fills = self.total_fills,
            slots = self.slots.len(),
            best_bid = ?best_bid.map(|p| p.to_string()),
            best_ask = ?best_ask.map(|p| p.to_string()),
            vol_samples = self.volatility.sample_count(),
            "A-S tick"
        );

        Ok(())
    }

    async fn on_event(
        &mut self,
        _ctx: &mut StrategyContext,
        event: ExchangeEvent,
    ) -> Result<(), BotError> {
        match event {
            ExchangeEvent::Trade { order_id, client_id, side, price, quantity, .. } => {
                self.record_fill(client_id.as_deref(), &order_id, side, price, quantity);
                tracing::info!(
                    side = %side,
                    price = %price,
                    qty = %quantity,
                    net_pos = %self.net_position,
                    pnl = %self.realized_pnl,
                    "A-S fill"
                );
            }
            ExchangeEvent::OrderUpdate { order, .. } => {
                if order.status == OrderStatus::Filled {
                    self.record_fill(
                        order.client_id.as_deref(),
                        &order.id,
                        order.side,
                        order.price,
                        order.quantity,
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn on_stop(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();
        let exchange = &self.exchange_name;
        tracing::info!("Shutting down A-S strategy, cancelling all orders");
        ctx.cancel_all_orders(exchange, &symbol).await?;
        tracing::info!(
            net_pos = %self.net_position,
            pnl = %self.realized_pnl,
            fills = self.total_fills,
            "A-S final stats"
        );
        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "net_position": self.net_position.to_string(),
            "realized_pnl": self.realized_pnl.to_string(),
            "total_fills": self.total_fills,
            "slots": self.slots,
            "sigma": self.volatility.sigma(),
            "vol_samples": self.volatility.sample_count(),
        })
    }
}
