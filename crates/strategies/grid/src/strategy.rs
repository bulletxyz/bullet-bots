use std::time::Instant;

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::strategy::{Strategy, StrategyContext};
use bb_core::types::*;
use rust_decimal::Decimal;

use crate::config::{GridConfig, SpacingMode};
use crate::grid::{GridLevelStatus, GridState, OrderRef};

/// Price tolerance for `reconcile` matching, in percent. Tight: only keeps
/// existing orders whose price is effectively identical to the desired one.
/// 0.01% — tight enough that any meaningful drift forces a replace.
fn reconcile_match_tolerance_pct() -> Decimal {
    Decimal::new(1, 2)
}

/// Grid trading strategy.
///
/// Places buy and sell limit orders at fixed intervals around a reference price.
/// When an order fills, places a new order on the opposite side to capture the
/// spread. Tracks net position and pauses when limits are hit.
pub struct GridStrategy {
    config: GridConfig,
    state: GridState,
    exchange_name: String,
}

impl GridStrategy {
    pub fn new(config: GridConfig, exchange_name: String) -> Self {
        let state = match &config.trend_filter {
            Some(cfg) => GridState::new().with_trend_filter(cfg),
            None => GridState::new(),
        };
        Self { config, state, exchange_name }
    }

    /// Reject configs whose spacing can't cover round-trip maker fees. Spacing
    /// is interpreted in bps: geometric `grid_spacing` is already a percent
    /// (→ ×100 for bps); arithmetic spacing is absolute price and is compared
    /// against `mid_price × 2 × maker_bps × multiplier`.
    fn check_fee_floor(&self, mid_price: Decimal) -> Result<(), BotError> {
        let Some(fees) = &self.config.fees else {
            tracing::warn!(
                "No [strategy.grid.fees] configured — skipping fee-floor check. \
                 Consider setting maker_bps so startup can refuse uneconomic spacing."
            );
            return Ok(());
        };
        let round_trip_bps = Decimal::from(2) * fees.maker_bps;
        let required_bps = round_trip_bps * fees.min_spacing_fee_multiplier;

        let spacing_bps = match self.config.spacing_mode {
            SpacingMode::Geometric => self.config.grid_spacing * Decimal::from(100),
            SpacingMode::Arithmetic => {
                if mid_price.is_zero() {
                    return Err(BotError::strategy(
                        "Cannot check fee floor: mid_price is zero",
                    ));
                }
                self.config.grid_spacing / mid_price * Decimal::from(10_000)
            }
        };

        let break_even_win_rate = if round_trip_bps.is_zero() {
            Decimal::ZERO
        } else {
            round_trip_bps / spacing_bps
        };

        tracing::info!(
            spacing_bps = %spacing_bps,
            required_bps = %required_bps,
            maker_bps = %fees.maker_bps,
            break_even_win_rate = %break_even_win_rate,
            "Grid fee-floor check"
        );

        if spacing_bps < required_bps {
            return Err(BotError::strategy(format!(
                "Grid spacing {spacing_bps} bps below required {required_bps} bps \
                 ({} × round-trip maker fee). Widen grid_spacing or lower \
                 min_spacing_fee_multiplier.",
                fees.min_spacing_fee_multiplier
            )));
        }
        Ok(())
    }

    /// Place all pending grid orders. Assigns a fresh `client_id` to each
    /// level *before* submission so that subsequent fill events can be
    /// correlated even though the exchange doesn't return order_ids
    /// synchronously.
    async fn place_pending_orders(&mut self, ctx: &StrategyContext) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();
        let exchange = &self.exchange_name;
        let max_position = self.config.max_position;
        let order_type = self.parse_order_type();
        let order_size = self.config.order_size;

        let net_position = self.state.net_position;
        let mut orders_to_place: Vec<NewOrder> = Vec::new();
        let mut target_level_ids: Vec<String> = Vec::new();
        for idx in 0..self.state.levels.len() {
            let level_side;
            let level_price;
            {
                let level = &self.state.levels[idx];
                if level.status != GridLevelStatus::Pending {
                    continue;
                }
                let at_max = match level.side {
                    Side::Buy => net_position >= max_position,
                    Side::Sell => net_position <= -max_position,
                };
                if at_max {
                    continue;
                }
                level_side = level.side;
                level_price = level.price;
            }
            let cid = self.state.issue_client_id();
            self.state.levels[idx].client_id = Some(cid.clone());
            orders_to_place.push(NewOrder {
                symbol: symbol.clone(),
                side: level_side,
                order_type,
                price: level_price,
                quantity: order_size,
                client_id: Some(cid.clone()),
                reduce_only: false,
            });
            target_level_ids.push(cid);
        }

        if orders_to_place.is_empty() {
            return Ok(());
        }

        tracing::info!(count = orders_to_place.len(), "Placing grid orders");

        let results = ctx.place_orders(exchange, &orders_to_place).await?;

        let mut placed = 0;
        for (result, cid) in results.iter().zip(target_level_ids.iter()) {
            let Some(level) = self.state.levels.iter_mut().find(|l| l.client_id.as_ref() == Some(cid)) else {
                continue;
            };
            if result.success {
                level.status = GridLevelStatus::Active;
                if !result.order_id.is_empty() {
                    level.order_id = Some(result.order_id.clone());
                }
                placed += 1;
            } else {
                tracing::warn!(
                    error = result.error.as_deref().unwrap_or("unknown"),
                    side = %level.side,
                    price = %level.price,
                    "Failed to place grid order"
                );
                level.client_id = None;
            }
        }

        tracing::info!(placed, "Grid orders placed");
        Ok(())
    }

    /// Apply a reconcile diff: cancel drifted orders, then place newly-needed
    /// ones. Uses the same `client_id`-first correlation as `place_pending`.
    async fn apply_reconcile(
        &mut self,
        ctx: &StrategyContext,
        new_mid: Decimal,
        cancels: Vec<OrderRef>,
        places: Vec<(Side, Decimal)>,
    ) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();
        let exchange = &self.exchange_name;

        if !cancels.is_empty() {
            let cancel_orders: Vec<CancelOrder> = cancels
                .iter()
                .map(|r| match r {
                    OrderRef::Client(cid) => CancelOrder {
                        symbol: symbol.clone(),
                        order_id: String::new(),
                        client_id: Some(cid.clone()),
                    },
                    OrderRef::Exchange(oid) => CancelOrder {
                        symbol: symbol.clone(),
                        order_id: oid.clone(),
                        client_id: None,
                    },
                })
                .collect();
            tracing::info!(count = cancel_orders.len(), "Soft-rebalance: cancelling drifted orders");
            ctx.cancel_orders(exchange, &cancel_orders).await?;
        }

        // Update state: drop cancelled levels, append new pending, recenter.
        self.state.apply_reconcile(new_mid, &cancels, &places, Instant::now());

        if !places.is_empty() {
            self.place_pending_orders(ctx).await?;
        }
        Ok(())
    }

    /// Handle a fill: record position change and place opposite order.
    async fn handle_fill(
        &mut self,
        ctx: &StrategyContext,
        order_id: &str,
        client_id: Option<&str>,
        side: Side,
        price: Decimal,
        quantity: Decimal,
    ) -> Result<(), BotError> {
        self.state.record_fill(side, price, quantity);

        tracing::info!(
            side = %side,
            price = %price,
            qty = %quantity,
            net_pos = %self.state.net_position,
            pnl = %self.state.realized_pnl,
            "Grid order filled"
        );

        let Some(filled_level) = self.state.mark_filled(client_id, order_id) else {
            return Ok(());
        };
        let opposite_side = filled_level.side.opposite();

        if self.state.at_max_position(opposite_side, self.config.max_position) {
            tracing::warn!(
                side = %opposite_side,
                net_pos = %self.state.net_position,
                max = %self.config.max_position,
                "At max position, skipping opposite order"
            );
            return Ok(());
        }

        let new_cid = self.state.issue_client_id();
        let order = NewOrder {
            symbol: ctx.config.symbol.clone(),
            side: opposite_side,
            order_type: self.parse_order_type(),
            price: filled_level.price,
            quantity: self.config.order_size,
            client_id: Some(new_cid.clone()),
            reduce_only: false,
        };

        let results = ctx.place_orders(&self.exchange_name, &[order]).await?;

        if let Some(result) = results.first() {
            if result.success {
                let lookup = client_id.or(filled_level.client_id.as_deref());
                if let Some(level) = self
                    .state
                    .levels
                    .iter_mut()
                    .find(|l| lookup.is_some() && l.client_id.as_deref() == lookup)
                {
                    level.side = opposite_side;
                    level.status = GridLevelStatus::Active;
                    level.client_id = Some(new_cid);
                    if !result.order_id.is_empty() {
                        level.order_id = Some(result.order_id.clone());
                    } else {
                        level.order_id = None;
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_order_type(&self) -> OrderType {
        match self.config.order_type.as_str() {
            "PostOnly" | "post_only" => OrderType::PostOnly,
            "Market" | "market" => OrderType::Market,
            _ => OrderType::Limit,
        }
    }
}

#[async_trait]
impl Strategy for GridStrategy {
    fn name(&self) -> &str {
        "grid"
    }

    async fn on_start(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();
        let exchange = &self.exchange_name;

        tracing::info!("Cancelling stale orders");
        ctx.cancel_all_orders(exchange, &symbol).await?;

        let book = ctx.get_orderbook(exchange, &symbol, 20).await?;
        let mid = book.midpoint().ok_or_else(|| {
            BotError::strategy("No orderbook data available to compute mid price")
        })?;

        // Refuse to run if spacing can't cover fees.
        self.check_fee_floor(mid)?;

        tracing::info!(
            mid_price = %mid,
            num_levels = self.config.num_levels,
            spacing = %self.config.grid_spacing,
            "Initializing grid"
        );

        self.state.compute_levels(mid, &self.config);
        self.place_pending_orders(ctx).await?;

        Ok(())
    }

    async fn on_tick(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();
        let exchange = &self.exchange_name;

        // Snapshot the current mid from cached book state. Used for both the
        // trend filter and rebalance decisions below.
        let current_mid = ctx.orderbook(exchange).and_then(|b| b.midpoint());

        // Trend-filter gate: update EMAs and flip `paused` if divergence exceeds
        // threshold. When paused, cancel all live orders and bail on the rest
        // of the tick (re-placement happens on the next tick once slope relaxes).
        if let (Some(cfg), Some(mid)) = (self.config.trend_filter.clone(), current_mid) {
            let was_paused = self.state.paused;
            let (divergence_bps, paused) =
                self.state.update_trend_filter(mid, Instant::now(), &cfg);
            if paused && !was_paused {
                tracing::warn!(
                    divergence_bps = %format!("{divergence_bps:.1}"),
                    threshold_bps = %cfg.pause_divergence_bps,
                    "Trend filter tripped — pausing grid and cancelling live orders"
                );
                ctx.cancel_all_orders(exchange, &symbol).await?;
                for l in &mut self.state.levels {
                    l.status = GridLevelStatus::Pending;
                    l.order_id = None;
                    l.client_id = None;
                }
            } else if !paused && was_paused {
                tracing::info!(
                    divergence_bps = %format!("{divergence_bps:.1}"),
                    "Trend filter cleared — resuming grid"
                );
            }
            if paused {
                return Ok(());
            }
        }

        // Reconcile on drift. Only the delta is issued, and a cooldown
        // suppresses churn in whippy markets. `at_max_position` wind-down is
        // still handled by the per-level skip in place_pending_orders.
        if let Some(mid) = current_mid {
            let now = Instant::now();
            if self.state.needs_rebalance(mid, self.config.rebalance_threshold_pct)
                && self.state.rebalance_ready(now, self.config.rebalance_cooldown_secs)
            {
                let diff = self.state.reconcile(
                    mid,
                    &self.config,
                    reconcile_match_tolerance_pct(),
                );
                if diff.changed {
                    tracing::info!(
                        old_center = %self.state.center_price,
                        new_mid = %mid,
                        cancels = diff.cancels.len(),
                        places = diff.places.len(),
                        "Soft-rebalancing grid"
                    );
                    self.apply_reconcile(ctx, mid, diff.cancels, diff.places).await?;
                    return Ok(());
                }
            }
        }

        // Verify expected orders are still live. We match on client_id first
        // because Bullet's SubmitTxResponse doesn't return exchange order_ids
        // synchronously — levels often only have client_id set until an
        // OrderUpdate arrives, so an order_id-only check would silently skip
        // every level.
        let live_orders = ctx.get_open_orders(exchange, &symbol).await?;
        let live_order_ids: std::collections::HashSet<&str> =
            live_orders.iter().map(|o| o.id.as_str()).collect();
        let live_client_ids: std::collections::HashSet<&str> =
            live_orders.iter().filter_map(|o| o.client_id.as_deref()).collect();

        let mut missing = 0;
        for level in &mut self.state.levels {
            if level.status != GridLevelStatus::Active {
                continue;
            }
            let is_live = match (&level.client_id, &level.order_id) {
                (Some(cid), _) if live_client_ids.contains(cid.as_str()) => true,
                (_, Some(oid)) if live_order_ids.contains(oid.as_str()) => true,
                _ => false,
            };
            if !is_live {
                // Without a trade/fill event, we can't tell if this level was
                // filled (→ should flip to opposite side) or just cancelled
                // (→ should re-place same side). Conservative choice: mark
                // pending and re-place same side. `handle_fill` will correct
                // the side when the fill event eventually lands.
                level.status = GridLevelStatus::Pending;
                level.order_id = None;
                level.client_id = None;
                missing += 1;
            }
        }

        if missing > 0 {
            tracing::warn!(missing, "Detected missing grid orders, re-placing");
            self.place_pending_orders(ctx).await?;
        }

        tracing::info!(
            active = self.state.active_count(),
            net_pos = %self.state.net_position,
            pnl = %self.state.realized_pnl,
            fills = self.state.total_fills,
            center = %self.state.center_price,
            paused = self.state.paused,
            "Grid tick"
        );

        Ok(())
    }

    async fn on_event(
        &mut self,
        ctx: &mut StrategyContext,
        event: ExchangeEvent,
    ) -> Result<(), BotError> {
        match event {
            ExchangeEvent::Trade { order_id, client_id, side, price, quantity, .. } => {
                self.handle_fill(ctx, &order_id, client_id.as_deref(), side, price, quantity)
                    .await?;
            }
            ExchangeEvent::OrderUpdate { order, .. } => {
                if order.status == OrderStatus::Filled {
                    self.handle_fill(
                        ctx,
                        &order.id,
                        order.client_id.as_deref(),
                        order.side,
                        order.price,
                        order.quantity,
                    )
                    .await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn on_stop(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();
        let exchange = &self.exchange_name;

        tracing::info!("Shutting down grid strategy, cancelling all orders");
        ctx.cancel_all_orders(exchange, &symbol).await?;

        tracing::info!(
            net_pos = %self.state.net_position,
            pnl = %self.state.realized_pnl,
            fills = self.state.total_fills,
            "Grid strategy final stats"
        );

        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "center_price": self.state.center_price.to_string(),
            "active_levels": self.state.active_count(),
            "total_levels": self.state.levels.len(),
            "net_position": self.state.net_position.to_string(),
            "realized_pnl": self.state.realized_pnl.to_string(),
            "total_fills": self.state.total_fills,
            "paused": self.state.paused,
            "grid_levels": self.state.levels,
        })
    }
}
