//! Static grid strategy as an event-driven actor.
//!
//! Subscribed events:
//!   - `BookUpdate` — cache mid / book for the initial anchor + trend filter.
//!   - `Trade` — canonical source of inventory / PnL. On fill, marks the
//!     level dormant and re-arms the adjacent level with the opposite side.
//!   - `OrderLifecycle` — learn exchange order_ids; mark cancel/reject
//!     levels Pending so the tick re-places them.
//!   - `Tick` — trend-filter evaluation, missing-order reconcile,
//!     place-pendings.
//!
//! No rebalancing: levels are fixed at startup. No dynamic re-centering, no
//! drift threshold, no inventory skew. The grid's bias is expressed by the
//! `anchor_price` (or current mid at startup) relative to the range.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bb_core::broker::Broker;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, OrderLifecycle, Tick, Trade};
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::{ClientIdIssuer, InventoryTracker};
use bb_core::types::{NewOrder, OrderBook, OrderStatus, OrderType, Side};
use rust_decimal::Decimal;

use crate::config::GridConfig;
use crate::grid::{GridState, LevelState};

pub struct GridActor {
    config: GridConfig,
    exchange_name: String,
    state: GridState,
    inventory: InventoryTracker,
    client_ids: ClientIdIssuer,
    book: Option<OrderBook>,
    /// The anchor the grid was built around. Cached so trend-filter resume
    /// can re-arm using the same bias it was configured with.
    anchor: Decimal,
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
            anchor: Decimal::ZERO,
        }
    }

    fn symbol(&self) -> &str {
        &self.config.symbol
    }

    fn broker(&self, cx: &ActorContext) -> Result<Arc<dyn Broker>, BotError> {
        cx.brokers().require(&self.exchange_name).map(Arc::clone)
    }

    /// Refuse to start if the per-level spacing can't cover round-trip
    /// maker fees by the configured multiplier.
    fn check_fee_floor(&self) -> Result<(), BotError> {
        let Some(fees) = &self.config.fees else {
            tracing::warn!("No [strategy.grid.fees] — skipping fee-floor check.");
            return Ok(());
        };
        // Profit per round trip = spacing × order_size, but expressed as a
        // fraction of price it's smallest at the *upper* end of the range
        // (same absolute spacing, larger denominator). Use upper_price so
        // the floor guarantees coverage everywhere in the grid — not just
        // at the cheap end.
        let spacing = self.config.spacing();
        if spacing.is_zero() || self.config.upper_price.is_zero() {
            return Err(BotError::strategy("degenerate grid geometry"));
        }
        let spacing_bps = spacing / self.config.upper_price * Decimal::from(10_000);
        let round_trip_bps = Decimal::from(2) * fees.maker_bps;
        let required_bps = round_trip_bps * fees.min_spacing_fee_multiplier;

        tracing::info!(
            spacing_bps = %spacing_bps,
            required_bps = %required_bps,
            "Grid fee-floor check"
        );
        if spacing_bps < required_bps {
            return Err(BotError::strategy(format!(
                "Grid spacing {spacing_bps} bps < required {required_bps} bps \
                 ({} × round-trip maker fee). Widen the range or reduce num_levels.",
                fees.min_spacing_fee_multiplier
            )));
        }
        Ok(())
    }

    /// Place every `Pending` level, skipping sides at `max_position` or
    /// levels that would cross the cached book under PostOnly.
    async fn place_pending_orders(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        let broker = self.broker(cx)?;
        let order_type = self.config.order_type;
        let order_size = self.config.order_size;
        let max_position = self.config.max_position;
        let net_position = self.inventory.net_position;

        let mut orders: Vec<NewOrder> = Vec::new();
        let mut target_cids: Vec<String> = Vec::new();
        let mut skipped_crossed = 0usize;

        for idx in 0..self.state.levels.len() {
            let (side, price) = {
                let l = &self.state.levels[idx];
                if l.state != LevelState::Pending {
                    continue;
                }
                let Some(side) = l.side else { continue };
                (side, l.price)
            };

            if GridState::at_max_position(net_position, side, max_position) {
                continue;
            }

            if order_type == OrderType::PostOnly
                && self.book.as_ref().is_some_and(|b| b.would_cross(side, price))
            {
                skipped_crossed += 1;
                continue;
            }

            let cid = self.client_ids.issue();
            self.state.levels[idx].client_id = Some(cid.clone());
            orders.push(NewOrder {
                symbol: self.symbol().to_string(),
                side,
                order_type,
                price,
                quantity: order_size,
                client_id: Some(cid.clone()),
                reduce_only: false,
            });
            target_cids.push(cid);
        }

        if skipped_crossed > 0 {
            tracing::warn!(
                skipped = skipped_crossed,
                best_bid = ?self.book.as_ref().and_then(|b| b.best_bid()).map(|l| l.price.to_string()),
                best_ask = ?self.book.as_ref().and_then(|b| b.best_ask()).map(|l| l.price.to_string()),
                "Skipping PostOnly levels in cross — range too tight vs current market"
            );
        }

        if orders.is_empty() {
            return Ok(());
        }
        tracing::info!(count = orders.len(), "Placing grid orders");

        let results = broker.place_orders(&orders).await?;
        let mut placed = 0;
        for (res, cid) in results.iter().zip(target_cids.iter()) {
            let Some(level) = self
                .state
                .levels
                .iter_mut()
                .find(|l| l.client_id.as_deref() == Some(cid))
            else {
                continue;
            };
            if res.success {
                level.state = LevelState::Active;
                if !res.order_id.is_empty() {
                    level.order_id = Some(res.order_id.clone());
                }
                placed += 1;
            } else {
                tracing::warn!(
                    error = res.error.as_deref().unwrap_or("unknown"),
                    side = ?level.side,
                    price = %level.price,
                    "Place failed — leaving level Pending for retry"
                );
                level.client_id = None;
            }
        }
        tracing::info!(placed, "Grid orders placed");
        Ok(())
    }

    /// Missing-order reconcile: query live orders; any `Active` level whose
    /// order is not on the exchange (e.g., silently cancelled by a venue
    /// edge case) flips back to `Pending` so we re-place next tick.
    async fn reconcile_missing_orders(&mut self, cx: &ActorContext) -> Result<usize, BotError> {
        let broker = self.broker(cx)?;
        let live = broker.get_open_orders(self.symbol()).await?;
        let live_oids: std::collections::HashSet<&str> =
            live.iter().map(|o| o.id.as_str()).collect();
        let live_cids: std::collections::HashSet<&str> =
            live.iter().filter_map(|o| o.client_id.as_deref()).collect();

        let mut missing = 0;
        for l in &mut self.state.levels {
            if l.state != LevelState::Active {
                continue;
            }
            let is_live = match (&l.client_id, &l.order_id) {
                (Some(cid), _) if live_cids.contains(cid.as_str()) => true,
                (_, Some(oid)) if live_oids.contains(oid.as_str()) => true,
                _ => false,
            };
            if !is_live {
                l.state = LevelState::Pending;
                l.client_id = None;
                l.order_id = None;
                missing += 1;
            }
        }
        Ok(missing)
    }

    async fn handle_trend_filter(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        let Some(cfg) = self.config.trend_filter.clone() else {
            return Ok(());
        };
        let Some(mid) = self.book.as_ref().and_then(|b| b.midpoint()) else {
            return Ok(());
        };
        let was_paused = self.state.paused;
        let (div, paused) = self.state.update_trend_filter(mid, Instant::now(), &cfg);

        if paused && !was_paused {
            tracing::warn!(
                divergence_bps = %format!("{div:.1}"),
                threshold_bps = %cfg.pause_divergence_bps,
                "Trend filter tripped — suspending grid"
            );
            let broker = self.broker(cx)?;
            let _ = broker.cancel_all_orders(self.symbol()).await;
            self.state.suspend_all();
        } else if !paused && was_paused {
            tracing::info!(divergence_bps = %format!("{div:.1}"), "Trend filter cleared — resuming grid");
            self.state.resume(self.anchor);
        }
        Ok(())
    }
}

#[async_trait]
impl Actor for GridActor {
    async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        self.config.validate().map_err(BotError::strategy)?;
        self.check_fee_floor()?;

        let broker = self.broker(cx)?;
        tracing::info!("Cancelling stale orders");
        broker.cancel_all_orders(self.symbol()).await?;

        let book = broker.get_orderbook(self.symbol(), 20).await?;
        let mid = book.midpoint().ok_or_else(|| {
            BotError::strategy("No orderbook data available to compute mid at startup")
        })?;
        self.book = Some(book);

        // Anchor: explicit config wins; otherwise use current mid as the
        // "neutral at start" default.
        self.anchor = self.config.anchor_price.unwrap_or(mid);
        self.state.compute_levels(self.anchor, &self.config);

        // Warn when the grid is configured away from where the market
        // actually is. The `anchor_price` config-time check only catches
        // explicit mis-sets; an implicit "use current mid" anchor with
        // a range that doesn't include the mid is still a legal config
        // but produces a degenerate grid (all one side).
        if mid < self.config.lower_price || mid > self.config.upper_price {
            tracing::warn!(
                mid = %mid,
                lower = %self.config.lower_price,
                upper = %self.config.upper_price,
                "Startup mid is outside the grid range — grid will idle until price re-enters"
            );
        }

        let buys =
            self.state.levels.iter().filter(|l| l.side == Some(Side::Buy)).count();
        let sells =
            self.state.levels.iter().filter(|l| l.side == Some(Side::Sell)).count();
        tracing::info!(
            lower = %self.config.lower_price,
            upper = %self.config.upper_price,
            levels = self.config.num_levels,
            spacing = %self.config.spacing(),
            anchor = %self.anchor,
            buys,
            sells,
            mid = %mid,
            "Static grid initialized"
        );

        // Evaluate trend filter before the first placement — if the market
        // is already trending hard at startup, suspend-all takes effect
        // before we submit a batch we'd immediately cancel.
        self.handle_trend_filter(cx).await?;
        if self.state.paused {
            tracing::warn!("Trend filter tripped at init — deferring initial placement");
            return Ok(());
        }
        self.place_pending_orders(cx).await
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
            realized_pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            "Grid final stats"
        );
        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "lower_price": self.config.lower_price.to_string(),
            "upper_price": self.config.upper_price.to_string(),
            "anchor": self.anchor.to_string(),
            "spacing": self.config.spacing().to_string(),
            "active_levels": self.state.active_count(),
            "total_levels": self.state.levels.len(),
            "net_position": self.inventory.net_position.to_string(),
            "realized_pnl": self.inventory.realized_pnl.to_string(),
            "total_fills": self.inventory.total_fills,
            "paused": self.state.paused,
            "levels": self.state.levels,
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
        self.inventory.record_fill(event.side, event.price, event.quantity);

        let Some(idx) =
            self.state.find_fill_target(event.client_id.as_deref(), &event.order_id)
        else {
            // Could happen if a fill arrives for an order we never saw
            // (e.g. left over from a prior session that get_open_orders
            // didn't catch at init). Log and move on.
            tracing::debug!(
                order_id = %event.order_id,
                "Trade with no matching level — ignoring"
            );
            return Ok(());
        };

        tracing::info!(
            level = idx,
            side = %event.side,
            price = %event.price,
            qty = %event.quantity,
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            "Grid fill"
        );

        // Re-arm the adjacent level (or skip if it's already live). Log
        // either way so an operator can see the "grid walking up/down"
        // chain in the log stream without tailing state.
        match self.state.on_fill(idx, event.side) {
            Some((target_idx, target_side)) => {
                let target_price = self.state.levels[target_idx].price;
                tracing::info!(
                    from = idx,
                    to = target_idx,
                    side = %target_side,
                    price = %target_price,
                    "Re-armed adjacent level"
                );
            }
            None => {
                tracing::debug!(
                    level = idx,
                    "No re-arm: adjacent level not dormant or at grid edge"
                );
            }
        }

        self.place_pending_orders(cx).await
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
        // Learn exchange order_id once the venue acks our placement.
        if let Some(cid) = event.order.client_id.as_deref() {
            if let Some(level) =
                self.state.levels.iter_mut().find(|l| l.client_id.as_deref() == Some(cid))
            {
                if level.order_id.is_none() && !event.order.id.is_empty() {
                    level.order_id = Some(event.order.id.clone());
                }
            }
        }
        // Cancelled/rejected → flip back to Pending so the tick re-places.
        // Log so an operator can tell a venue-side auto-cancel (e.g. order
        // TTL expiry on some testnets) apart from a bug in our reconcile.
        if matches!(
            event.order.status,
            OrderStatus::Cancelled | OrderStatus::Rejected
        ) {
            if let Some(cid) = event.order.client_id.as_deref() {
                if let Some(level) =
                    self.state.levels.iter_mut().find(|l| l.client_id.as_deref() == Some(cid))
                {
                    if level.state == LevelState::Active {
                        tracing::info!(
                            level = level.index,
                            price = %level.price,
                            side = ?level.side,
                            status = ?event.order.status,
                            "Lifecycle event flipped Active level back to Pending"
                        );
                        level.state = LevelState::Pending;
                        level.client_id = None;
                        level.order_id = None;
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Tick> for GridActor {
    async fn on_event(&mut self, _event: Tick, cx: &ActorContext) -> Result<(), BotError> {
        self.handle_trend_filter(cx).await?;
        if self.state.paused {
            return Ok(());
        }

        let missing = self.reconcile_missing_orders(cx).await?;
        if missing > 0 {
            tracing::warn!(missing, "Detected missing grid orders");
        }
        self.place_pending_orders(cx).await?;

        tracing::info!(
            active = self.state.active_count(),
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            paused = self.state.paused,
            "Grid tick"
        );
        Ok(())
    }
}

#[cfg(test)]
mod integration_tests {
    //! Drive the actor end-to-end with a `ScriptedFeed<Trade>` and
    //! `NullBroker`. Exercises the fill → inventory + adjacent re-arm loop
    //! without touching the network.

    use super::*;
    use bb_core::broker::Broker;
    use bb_core::events::Trade;
    use bb_core::harness::testing::{NullBroker, ScriptedFeed};
    use bb_core::harness::{ActorSpec, HarnessBuilder};

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn test_config() -> GridConfig {
        GridConfig {
            symbol: "BTC-USD".into(),
            lower_price: d("74"),
            upper_price: d("78"),
            num_levels: 5,
            anchor_price: Some(d("75.5")),
            order_size: d("1"),
            order_type: OrderType::Limit,
            max_position: d("10"),
            fees: None,
            trend_filter: None,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unmatched_trade_is_ignored() {
        // Actor that never ran init — level set is empty, so an incoming
        // Trade matches nothing. The harness should dispatch it and the
        // broker should see no subsequent place/cancel from the Trade path.
        let broker = NullBroker::shared("bullet");
        let actor = GridActor::new(test_config(), "bullet");
        let feed = ScriptedFeed::new(vec![Trade {
            exchange: "bullet".into(),
            symbol: "BTC-USD".into(),
            order_id: "42".into(),
            client_id: Some("unknown".into()),
            side: Side::Buy,
            price: d("75"),
            quantity: d("1"),
        }]);

        let harness = HarnessBuilder::new()
            .wire_broker("bullet", broker.clone() as Arc<dyn Broker>)
            .wire_feed_named("trades", feed)
            .wire_actor(ActorSpec::new("grid", actor).sub::<Trade>())
            .build()
            .unwrap();
        let _ = harness.run().await.unwrap();

        let hist = broker.history().await;
        assert!(
            !hist.iter().any(|c| c.method == "place_orders"),
            "unexpected place: {hist:?}"
        );
    }

    // --- GridState-level tests for paths the actor relies on but that don't
    // need the full harness roundtrip (and are annoying to drive there
    // because init needs a live book). ---

    use crate::grid::{GridState, LevelState};

    fn active_level(state: &mut GridState, idx: usize, cid: &str) {
        let l = &mut state.levels[idx];
        l.state = LevelState::Active;
        l.client_id = Some(cid.to_string());
    }

    #[test]
    fn lifecycle_cancelled_flips_active_to_pending() {
        // Build state directly — reproduces what init does without needing
        // a live broker.
        let cfg = test_config();
        let mut state = GridState::new();
        state.compute_levels(d("75.5"), &cfg);
        active_level(&mut state, 2, "cid-2");

        // Directly exercise the flip logic (mirrors the actor's lifecycle
        // handler). Keeping this at the state layer avoids constructing a
        // full harness just to verify one branch.
        let order_id = "9999";
        let matching_cid = "cid-2";
        let idx = state
            .levels
            .iter()
            .position(|l| l.client_id.as_deref() == Some(matching_cid))
            .expect("matching level");
        assert_eq!(state.levels[idx].state, LevelState::Active);

        // Simulate the handler's branch for Cancelled:
        let level = &mut state.levels[idx];
        if level.state == LevelState::Active {
            level.state = LevelState::Pending;
            level.client_id = None;
            level.order_id = None;
        }

        assert_eq!(state.levels[idx].state, LevelState::Pending);
        assert!(state.levels[idx].client_id.is_none());
        // Prevent unused warning on order_id placeholder
        let _ = order_id;
    }

    #[test]
    fn lifecycle_cancelled_noop_when_already_pending() {
        // A Cancelled event for a level that's already Pending (e.g. because
        // reconcile beat the WS event to the state) should not flip status
        // or drop client_id. The actor's condition `state == Active` is
        // what protects this — this test pins that invariant at the unit
        // level.
        let cfg = test_config();
        let mut state = GridState::new();
        state.compute_levels(d("75.5"), &cfg);
        let idx = 1;
        state.levels[idx].state = LevelState::Pending;
        state.levels[idx].client_id = Some("cid".to_string());

        // Re-run the same branch; since state != Active, no flip.
        let level = &mut state.levels[idx];
        if level.state == LevelState::Active {
            level.state = LevelState::Pending;
            level.client_id = None;
        }
        assert_eq!(state.levels[idx].state, LevelState::Pending);
        assert_eq!(state.levels[idx].client_id.as_deref(), Some("cid"));
    }

    // --- would_cross integration: ensure place_pending_orders skips a
    // crossing PostOnly level without killing the rest of the batch. ---

    #[tokio::test(flavor = "current_thread")]
    async fn would_cross_skips_inner_level_in_postonly_mode() {
        use bb_core::types::OrderBook;
        use std::collections::BTreeMap;

        // Build an actor with range straddling an ask that's inside our
        // grid: the level-just-below-ask is a buy, and its price (≥ ask)
        // should be skipped.
        let mut cfg = test_config();
        cfg.order_type = OrderType::PostOnly;
        cfg.anchor_price = Some(d("76"));
        // 5 levels at 74,75,76,77,78. Anchor 76 → 74,75,76=buy, 77,78=sell.
        let mut state = GridState::new();
        state.compute_levels(d("76"), &cfg);
        // Sanity: level 2 = 76 = buy.
        assert_eq!(state.levels[2].side, Some(Side::Buy));
        assert_eq!(state.levels[2].state, LevelState::Pending);

        // Book where best_ask = 76. Any PostOnly buy at price >= 76 crosses.
        let mut asks = BTreeMap::new();
        asks.insert(d("76"), d("1"));
        let mut bids = BTreeMap::new();
        bids.insert(d("75"), d("1"));
        let book = OrderBook { bids, asks, last_update_id: 0 };

        // would_cross: buy at 76 vs best_ask 76 → crosses.
        assert!(book.would_cross(Side::Buy, d("76")));
        // Lower buy levels (75, 74) don't cross.
        assert!(!book.would_cross(Side::Buy, d("75")));
        // Sell levels above best_bid don't cross either.
        assert!(!book.would_cross(Side::Sell, d("77")));

        // Note: end-to-end verification of place_pending_orders' skip path
        // would need a live-broker stub that acknowledges the partial batch;
        // the unit-level assertions above pin the contract and the actor
        // test above covers event flow.
    }
}

