//! Grid strategy as an event-driven actor.
//!
//! Subscribed events:
//!   - `BookUpdate` — cache the book so tick handlers can read mid price.
//!   - `Trade` — the *only* event that updates inventory / PnL. Also places
//!     the opposite-side replacement order.
//!   - `OrderLifecycle` — learn the exchange-assigned `order_id` once the
//!     venue acknowledges placement, so reconcile can match on either id.
//!   - `Tick` — periodic reconcile, trend-filter evaluation, missing-order
//!     detection.
//!
//! Business logic (level geometry, trend filter, fee floor) is identical to
//! the pre-harness version. Position/PnL tracking and client-id issuance moved
//! to shared helpers so every strategy benefits from the same correct impls.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bb_core::broker::Broker;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, OrderLifecycle, Tick, Trade};
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::{ClientIdIssuer, InventoryTracker};
use bb_core::types::{CancelOrder, NewOrder, OrderBook, OrderStatus, Side};
use rust_decimal::Decimal;

use crate::config::{GridConfig, SpacingMode};
use crate::grid::{GridLevelStatus, GridState, OrderRef};

/// Price tolerance for reconcile matching, in percent. Tight enough that any
/// meaningful drift forces a replace.
fn reconcile_match_tolerance_pct() -> Decimal {
    Decimal::new(1, 2) // 0.01%
}

pub struct GridActor {
    config: GridConfig,
    exchange_name: String,
    state: GridState,
    inventory: InventoryTracker,
    client_ids: ClientIdIssuer,
    book: Option<OrderBook>,
}

impl GridActor {
    pub fn new(config: GridConfig, exchange_name: impl Into<String>) -> Self {
        let state = match &config.trend_filter {
            Some(cfg) => GridState::new().with_trend_filter(cfg),
            None => GridState::new(),
        };
        Self {
            config,
            exchange_name: exchange_name.into(),
            state,
            inventory: InventoryTracker::new(),
            client_ids: ClientIdIssuer::new(),
            book: None,
        }
    }

    fn symbol(&self) -> &str {
        &self.config.symbol
    }

    fn check_fee_floor(&self, mid_price: Decimal) -> Result<(), BotError> {
        let Some(fees) = &self.config.fees else {
            tracing::warn!(
                "No [strategy.grid.fees] configured — skipping fee-floor check."
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

    fn broker(&self, cx: &ActorContext) -> Result<Arc<dyn Broker>, BotError> {
        cx.brokers().require(&self.exchange_name).map(Arc::clone)
    }

    /// Place every `Pending` level, skipping sides at max position.
    async fn place_pending_orders(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        let broker = self.broker(cx)?;
        let order_type = self.config.order_type;
        let order_size = self.config.order_size;
        let max_position = self.config.max_position;
        let net_position = self.inventory.net_position;

        let mut orders: Vec<NewOrder> = Vec::new();
        let mut target_cids: Vec<String> = Vec::new();
        for idx in 0..self.state.levels.len() {
            let (level_side, level_price) = {
                let level = &self.state.levels[idx];
                if level.status != GridLevelStatus::Pending {
                    continue;
                }
                if GridState::at_max_position(net_position, level.side, max_position) {
                    continue;
                }
                (level.side, level.price)
            };
            let cid = self.client_ids.issue();
            self.state.levels[idx].client_id = Some(cid.clone());
            orders.push(NewOrder {
                symbol: self.symbol().to_string(),
                side: level_side,
                order_type,
                price: level_price,
                quantity: order_size,
                client_id: Some(cid.clone()),
                reduce_only: false,
            });
            target_cids.push(cid);
        }

        if orders.is_empty() {
            return Ok(());
        }
        tracing::info!(count = orders.len(), "Placing grid orders");

        let results = broker.place_orders(&orders).await?;
        let mut placed = 0;
        for (result, cid) in results.iter().zip(target_cids.iter()) {
            let Some(level) =
                self.state.levels.iter_mut().find(|l| l.client_id.as_deref() == Some(cid))
            else {
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

    async fn apply_reconcile(
        &mut self,
        cx: &ActorContext,
        new_mid: Decimal,
        cancels: Vec<OrderRef>,
        places: Vec<(Side, Decimal)>,
    ) -> Result<(), BotError> {
        let broker = self.broker(cx)?;

        if !cancels.is_empty() {
            let cancel_orders: Vec<CancelOrder> = cancels
                .iter()
                .map(|r| match r {
                    OrderRef::Client(cid) => CancelOrder {
                        symbol: self.symbol().to_string(),
                        order_id: String::new(),
                        client_id: Some(cid.clone()),
                    },
                    OrderRef::Exchange(oid) => CancelOrder {
                        symbol: self.symbol().to_string(),
                        order_id: oid.clone(),
                        client_id: None,
                    },
                })
                .collect();
            tracing::info!(count = cancel_orders.len(), "Soft-rebalance: cancelling drifted orders");
            broker.cancel_orders(&cancel_orders).await?;
        }

        self.state.apply_reconcile(new_mid, &cancels, &places, Instant::now());

        if !places.is_empty() {
            self.place_pending_orders(cx).await?;
        }
        Ok(())
    }

    /// Place the mirror-side replacement after a fill.
    async fn place_replacement_after_fill(
        &mut self,
        cx: &ActorContext,
        filled_side: Side,
        filled_price: Decimal,
    ) -> Result<(), BotError> {
        let opposite_side = filled_side.opposite();
        if GridState::at_max_position(
            self.inventory.net_position,
            opposite_side,
            self.config.max_position,
        ) {
            tracing::warn!(
                side = %opposite_side,
                net_pos = %self.inventory.net_position,
                max = %self.config.max_position,
                "At max position, skipping opposite order"
            );
            return Ok(());
        }

        let broker = self.broker(cx)?;
        let cid = self.client_ids.issue();
        let order = NewOrder {
            symbol: self.symbol().to_string(),
            side: opposite_side,
            order_type: self.config.order_type,
            price: filled_price,
            quantity: self.config.order_size,
            client_id: Some(cid.clone()),
            reduce_only: false,
        };
        let results = broker.place_orders(&[order]).await?;
        // Whatever the outcome, the `Filled` sentinel set by `mark_filled`
        // must not stick around — otherwise the tick reconcile (which only
        // looks at `Active` levels) would never re-place it and the slot
        // would leak permanently. Find it once, mutate in place based on
        // whether the submission succeeded.
        let Some(level) =
            self.state.levels.iter_mut().find(|l| l.status == GridLevelStatus::Filled)
        else {
            return Ok(());
        };

        match results.first() {
            Some(res) if res.success => {
                level.side = opposite_side;
                level.status = GridLevelStatus::Active;
                level.client_id = Some(cid);
                level.order_id =
                    if res.order_id.is_empty() { None } else { Some(res.order_id.clone()) };
            }
            Some(res) => {
                tracing::warn!(
                    side = %opposite_side,
                    price = %filled_price,
                    error = res.error.as_deref().unwrap_or("unknown"),
                    "Replacement placement failed; marking level Pending for retry"
                );
                level.side = opposite_side;
                level.status = GridLevelStatus::Pending;
                level.client_id = None;
                level.order_id = None;
            }
            None => {
                // Broker returned an empty result vector. Treat as retry.
                level.side = opposite_side;
                level.status = GridLevelStatus::Pending;
                level.client_id = None;
                level.order_id = None;
            }
        }
        Ok(())
    }

    /// Tick-time trend-filter evaluation. Cancels live orders and returns
    /// `true` if paused (caller should skip the rest of the tick).
    async fn evaluate_trend_filter(&mut self, cx: &ActorContext, mid: Decimal) -> Result<bool, BotError> {
        let Some(cfg) = self.config.trend_filter.clone() else {
            return Ok(false);
        };
        let was_paused = self.state.paused;
        let (divergence_bps, paused) =
            self.state.update_trend_filter(mid, Instant::now(), &cfg);
        if paused && !was_paused {
            tracing::warn!(
                divergence_bps = %format!("{divergence_bps:.1}"),
                threshold_bps = %cfg.pause_divergence_bps,
                "Trend filter tripped — pausing grid and cancelling live orders"
            );
            let broker = self.broker(cx)?;
            broker.cancel_all_orders(self.symbol()).await?;
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
        Ok(paused)
    }

    async fn handle_tick(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        let Some(mid) = self.book.as_ref().and_then(|b| b.midpoint()) else {
            return Ok(());
        };

        if self.evaluate_trend_filter(cx, mid).await? {
            return Ok(());
        }

        let now = Instant::now();
        if self.state.needs_rebalance(mid, self.config.rebalance_threshold_pct)
            && self.state.rebalance_ready(now, self.config.rebalance_cooldown_secs)
        {
            let diff = self.state.reconcile(
                mid,
                self.inventory.net_position,
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
                self.apply_reconcile(cx, mid, diff.cancels, diff.places).await?;
                return Ok(());
            }
        }

        // Verify expected orders are still live.
        let broker = self.broker(cx)?;
        let live_orders = broker.get_open_orders(self.symbol()).await?;
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
                level.status = GridLevelStatus::Pending;
                level.order_id = None;
                level.client_id = None;
                missing += 1;
            }
        }
        if missing > 0 {
            tracing::warn!(missing, "Detected missing grid orders, re-placing");
        }
        // Always run — covers missing orders AND any level left `Pending` by
        // a prior failed placement (e.g., a replacement that the broker
        // rejected). `place_pending_orders` is a no-op when nothing pends.
        self.place_pending_orders(cx).await?;

        tracing::info!(
            active = self.state.active_count(),
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            center = %self.state.center_price,
            paused = self.state.paused,
            "Grid tick"
        );
        Ok(())
    }
}

#[async_trait]
impl Actor for GridActor {
    async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        let broker = self.broker(cx)?;
        tracing::info!("Cancelling stale orders");
        broker.cancel_all_orders(self.symbol()).await?;

        let book = broker.get_orderbook(self.symbol(), 20).await?;
        let mid = book.midpoint().ok_or_else(|| {
            BotError::strategy("No orderbook data available to compute mid price")
        })?;
        self.check_fee_floor(mid)?;
        self.book = Some(book);

        tracing::info!(
            mid_price = %mid,
            num_levels = self.config.num_levels,
            spacing = %self.config.grid_spacing,
            "Initializing grid"
        );
        self.state.compute_levels(mid, self.inventory.net_position, &self.config);
        self.place_pending_orders(cx).await?;
        Ok(())
    }

    async fn wind_down(
        &mut self,
        _reason: &WindDownReason,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        let broker = self.broker(cx)?;
        tracing::info!("Shutting down grid — cancelling all orders");
        if let Err(e) = broker.cancel_all_orders(self.symbol()).await {
            tracing::warn!(error = %e, "Failed to cancel all on shutdown");
        }
        tracing::info!(
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            "Grid final stats"
        );
        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "center_price": self.state.center_price.to_string(),
            "active_levels": self.state.active_count(),
            "total_levels": self.state.levels.len(),
            "net_position": self.inventory.net_position.to_string(),
            "realized_pnl": self.inventory.realized_pnl.to_string(),
            "total_fills": self.inventory.total_fills,
            "paused": self.state.paused,
            "grid_levels": self.state.levels,
        })
    }
}

#[async_trait]
impl EventHandler<BookUpdate> for GridActor {
    async fn on_event(
        &mut self,
        event: BookUpdate,
        _cx: &ActorContext,
    ) -> Result<(), BotError> {
        if event.exchange == self.exchange_name && event.symbol == self.symbol() {
            self.book = Some(event.orderbook);
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Trade> for GridActor {
    async fn on_event(&mut self, event: Trade, cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange != self.exchange_name || event.symbol != self.symbol() {
            return Ok(());
        }

        // 1. Update inventory (canonical).
        self.inventory.record_fill(event.side, event.price, event.quantity);

        // 2. Mark the level filled and derive the replacement price.
        let filled = self
            .state
            .mark_filled(event.client_id.as_deref(), &event.order_id);
        tracing::info!(
            side = %event.side,
            price = %event.price,
            qty = %event.quantity,
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            "Grid fill"
        );
        let Some(filled_level) = filled else {
            return Ok(());
        };

        // 3. Place the opposite-side order at the filled level's price.
        self.place_replacement_after_fill(cx, filled_level.side, filled_level.price).await
    }
}

#[async_trait]
impl EventHandler<OrderLifecycle> for GridActor {
    async fn on_event(
        &mut self,
        event: OrderLifecycle,
        _cx: &ActorContext,
    ) -> Result<(), BotError> {
        if event.exchange != self.exchange_name || event.order.symbol != self.symbol() {
            return Ok(());
        }
        // Learn the exchange order_id once the venue acks our place/fill.
        // Needed so reconcile matching works after a `client_id`-free adapter
        // path (e.g., Hyperliquid currently doesn't propagate cloid).
        if let Some(cid) = event.order.client_id.as_deref() {
            if let Some(level) =
                self.state.levels.iter_mut().find(|l| l.client_id.as_deref() == Some(cid))
            {
                if level.order_id.is_none() && !event.order.id.is_empty() {
                    level.order_id = Some(event.order.id.clone());
                }
            }
        }
        // Drop cancelled/rejected levels from the active set — the tick
        // missing-order check will re-place them.
        if matches!(
            event.order.status,
            OrderStatus::Cancelled | OrderStatus::Rejected
        ) {
            if let Some(level) = self.state.levels.iter_mut().find(|l| {
                l.client_id.as_deref() == event.order.client_id.as_deref()
                    && event.order.client_id.is_some()
            }) {
                level.status = GridLevelStatus::Pending;
                level.client_id = None;
                level.order_id = None;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Tick> for GridActor {
    async fn on_event(&mut self, _event: Tick, cx: &ActorContext) -> Result<(), BotError> {
        self.handle_tick(cx).await
    }
}

#[cfg(test)]
mod integration_tests {
    //! End-to-end test of the grid actor running under the real harness,
    //! driven by `ScriptedFeed<Trade>` with a `NullBroker` standing in for
    //! the exchange. Exercises the "fill → place replacement → inventory
    //! update" loop without touching any network.

    use super::*;
    use bb_core::broker::Broker;
    use bb_core::events::Trade;
    use bb_core::harness::testing::{NullBroker, ScriptedFeed};
    use bb_core::harness::{ActorSpec, HarnessBuilder};
    use bb_core::types::OrderType;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn test_config() -> GridConfig {
        GridConfig {
            symbol: "BTC-USD".to_string(),
            num_levels: 2,
            spacing_mode: SpacingMode::Geometric,
            grid_spacing: dec("0.5"),
            order_size: dec("1"),
            order_type: OrderType::Limit,
            max_position: dec("5"),
            rebalance_threshold_pct: dec("3"),
            rebalance_cooldown_secs: 30,
            inventory_skew_k: Decimal::ZERO,
            fees: None,
            trend_filter: None,
        }
    }

    fn fill(cid: &str) -> Trade {
        Trade {
            exchange: "bullet".to_string(),
            symbol: "BTC-USD".to_string(),
            order_id: "42".to_string(),
            client_id: Some(cid.to_string()),
            side: Side::Buy,
            price: dec("99.5"),
            quantity: dec("1"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn trade_updates_inventory_and_does_not_place_without_init() {
        // Actor without init — state is empty, so a fill can't match a level.
        // The harness still processes it and the broker should see no calls.
        let broker = NullBroker::shared("bullet");
        let actor = GridActor::new(test_config(), "bullet");
        let feed = ScriptedFeed::new(vec![fill("unknown")]).named("trades");

        let harness = HarnessBuilder::new()
            .wire_broker("bullet", broker.clone() as Arc<dyn Broker>)
            .wire_feed_named("trades", feed)
            .wire_actor(ActorSpec::new("grid", actor).sub::<Trade>())
            .build()
            .unwrap();
        let _ = harness.run().await.unwrap();

        let hist = broker.history().await;
        // No matching level → no replacement. Shutdown may have called
        // `cancel_all_orders` — we allow that but disallow `place_orders`.
        assert!(
            !hist.iter().any(|c| c.method == "place_orders"),
            "unexpected place: {hist:?}"
        );
    }
}
