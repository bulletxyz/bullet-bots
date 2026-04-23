use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::strategy::{Strategy, StrategyContext};
use bb_core::types::*;
use rust_decimal::Decimal;

use crate::config::GridConfig;
use crate::grid::{GridLevelStatus, GridState};

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
        Self { config, state: GridState::new(), exchange_name }
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
                // Clear the unused client_id so the level can be retried with
                // a fresh one next pass.
                level.client_id = None;
            }
        }

        tracing::info!(placed, "Grid orders placed");
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
                // Locate the just-filled level — prefer client_id from the fill
                // event, fall back to the filled level's prior client_id.
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

        // Cancel any stale orders
        tracing::info!("Cancelling stale orders");
        ctx.cancel_all_orders(exchange, &symbol).await?;

        // Fetch current orderbook to get mid price
        let book = ctx.get_orderbook(exchange, &symbol, 20).await?;
        let mid = book.midpoint().ok_or_else(|| {
            BotError::strategy("No orderbook data available to compute mid price")
        })?;

        tracing::info!(
            mid_price = %mid,
            num_levels = self.config.num_levels,
            spacing = %self.config.grid_spacing,
            "Initializing grid"
        );

        // Compute grid levels
        self.state.compute_levels(mid, &self.config);

        // Place all grid orders
        self.place_pending_orders(ctx).await?;

        Ok(())
    }

    async fn on_tick(&mut self, ctx: &mut StrategyContext) -> Result<(), BotError> {
        let symbol = ctx.config.symbol.clone();
        let exchange = &self.exchange_name;

        // Check for rebalance
        if let Some(book) = ctx.orderbook(exchange) {
            if let Some(mid) = book.midpoint() {
                if self.state.needs_rebalance(mid, self.config.rebalance_threshold_pct) {
                    tracing::info!(
                        old_center = %self.state.center_price,
                        new_mid = %mid,
                        "Rebalancing grid"
                    );
                    ctx.cancel_all_orders(exchange, &symbol).await?;
                    self.state.compute_levels(mid, &self.config);
                    self.place_pending_orders(ctx).await?;
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

        // Log status
        tracing::info!(
            active = self.state.active_count(),
            net_pos = %self.state.net_position,
            pnl = %self.state.realized_pnl,
            fills = self.state.total_fills,
            center = %self.state.center_price,
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
                // Handle fills from order updates (for exchanges that don't send
                // separate trade events)
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
            "grid_levels": self.state.levels,
        })
    }
}
