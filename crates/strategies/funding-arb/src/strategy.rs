//! Funding rate arbitrage as an event-driven actor.
//!
//! Two-venue delta-neutral strategy. Subscribed events:
//!   - `MarkPriceUpdate` — cache mark + funding per venue; trigger entry.
//!   - `Trade` — canonical source of position changes *per venue*. One `InventoryTracker` per venue
//!     via a `HashMap<exchange, tracker>`. The `net_delta` across both trackers should tend to zero
//!     while Active.
//!   - `BookUpdate` — cache best bid/ask for aggressive pricing.
//!   - `Tick` — phase-timeout checks, delta-imbalance guard, exit condition.
//!
//! Key fix vs. pre-harness version: position updates come from `Trade` only,
//! so the "double-count via Trade + `OrderUpdate`" bug that used to happen on
//! HL is structurally impossible. The `order.symbol` vs `exchange` name
//! confusion is gone too — events are typed and carry explicit `exchange`.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, MarkPriceUpdate, Tick, Trade};
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::{ClientIdIssuer, InventoryTracker};
use bb_core::types::{NewOrder, OrderBook, OrderType, Side};
use rust_decimal::Decimal;

use crate::config::{FundingArbConfig, OrderMode};
use crate::state::{ArbLeg, ArbPhase, ArbState};

pub struct FundingArbActor {
    config: FundingArbConfig,
    state: ArbState,
    /// Per-venue inventory. Each Trade updates exactly one tracker.
    inventory: HashMap<String, InventoryTracker>,
    books: HashMap<String, OrderBook>,
    /// Monotonic id-issuer seeded by session epoch so a restart doesn't
    /// collide with cloids the previous session left dangling on the venue.
    client_ids: ClientIdIssuer,
    /// Set of `client_ids` the strategy has issued. Trade events whose
    /// `client_id` isn't in this set are ignored — they're external fills
    /// (e.g., from another bot or manual order placement on the same wallet)
    /// and would otherwise corrupt our `InventoryTracker` net positions.
    my_client_ids: HashSet<String>,
}

impl FundingArbActor {
    pub fn new(config: FundingArbConfig) -> Self {
        Self::with_client_ids(config, ClientIdIssuer::session_seeded())
    }

    /// Construct with an explicit `ClientIdIssuer`. Use `ClientIdIssuer::new()`
    /// (starts at 1) in tests to get deterministic `client_ids` so scripted fills
    /// can carry the expected id without knowing the session epoch.
    pub fn with_client_ids(config: FundingArbConfig, client_ids: ClientIdIssuer) -> Self {
        let mut inventory = HashMap::new();
        inventory.insert(config.exchange_a.clone(), InventoryTracker::new());
        inventory.insert(config.exchange_b.clone(), InventoryTracker::new());
        Self {
            config,
            state: ArbState::new(),
            inventory,
            books: HashMap::new(),
            client_ids,
            my_client_ids: HashSet::new(),
        }
    }

    /// Issue a fresh `client_id` and remember it so the Trade handler can
    /// distinguish our fills from external ones on the same wallet.
    fn issue_client_id(&mut self) -> String {
        let cid = self.client_ids.issue();
        self.my_client_ids.insert(cid.clone());
        cid
    }

    fn symbol(&self) -> &str {
        &self.config.symbol
    }

    fn net_position(&self, exchange: &str) -> Decimal {
        self.inventory.get(exchange).map_or(Decimal::ZERO, |i| i.net_position)
    }

    /// Net delta across both legs — should be near zero when Active.
    fn net_delta(&self) -> Decimal {
        self.net_position(&self.config.exchange_a) + self.net_position(&self.config.exchange_b)
    }

    fn rates_sane(&self) -> bool {
        self.state.rate_a.abs() < self.config.max_funding_rate
            && self.state.rate_b.abs() < self.config.max_funding_rate
    }

    /// (`short_exchange`, `long_exchange`) — short the leg with the higher funding.
    fn pick_sides(&self) -> (String, String) {
        if self.state.rate_a > self.state.rate_b {
            (self.config.exchange_a.clone(), self.config.exchange_b.clone())
        } else {
            (self.config.exchange_b.clone(), self.config.exchange_a.clone())
        }
    }

    fn aggressive_price(&self, exchange: &str, side: Side) -> Decimal {
        let book_price = self.books.get(exchange).and_then(|b| match side {
            Side::Buy => b.best_ask().map(|l| l.price),
            Side::Sell => b.best_bid().map(|l| l.price),
        });
        let base = book_price.unwrap_or_else(|| {
            if exchange == self.config.exchange_a { self.state.mark_a } else { self.state.mark_b }
        });
        match side {
            Side::Buy => base * (Decimal::ONE + self.config.slippage),
            Side::Sell => base * (Decimal::ONE - self.config.slippage),
        }
    }

    fn order_type(&self) -> OrderType {
        match self.config.order_mode {
            OrderMode::Passive => OrderType::PostOnly,
            OrderMode::Aggressive => OrderType::Market,
        }
    }

    fn make_order(
        &mut self,
        exchange: &str,
        side: Side,
        qty: Decimal,
        reduce_only: bool,
    ) -> NewOrder {
        NewOrder {
            symbol: self.symbol().to_string(),
            side,
            order_type: self.order_type(),
            price: self.aggressive_price(exchange, side),
            quantity: qty,
            client_id: Some(self.issue_client_id()),
            reduce_only,
        }
    }

    async fn enter(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        let (short_ex, long_ex) = self.pick_sides();
        tracing::info!(
            spread = %self.state.abs_rate_spread(),
            short = %short_ex,
            long = %long_ex,
            "Entering arb position"
        );

        self.state.leg_a = Some(ArbLeg {
            exchange: short_ex.clone(),
            entry_side: Side::Sell,
            target_size: self.config.order_size,
        });
        self.state.leg_b = Some(ArbLeg {
            exchange: long_ex.clone(),
            entry_side: Side::Buy,
            target_size: self.config.order_size,
        });
        self.state.transition(ArbPhase::Entering);

        let short_orders = [self.make_order(&short_ex, Side::Sell, self.config.order_size, false)];
        let long_orders = [self.make_order(&long_ex, Side::Buy, self.config.order_size, false)];

        let (short_b, long_b) = (cx.broker(&short_ex)?, cx.broker(&long_ex)?);
        let (short_res, long_res) =
            tokio::join!(short_b.place_orders(&short_orders), long_b.place_orders(&long_orders));

        let short_ok = short_res
            .as_ref()
            .ok()
            .and_then(|r| r.first())
            .is_some_and(|r| r.success);
        let long_ok =
            long_res.as_ref().ok().and_then(|r| r.first()).is_some_and(|r| r.success);

        if let Err(e) = &short_res {
            tracing::error!(exchange = %short_ex, error = %e, "Short leg placement failed");
        } else if !short_ok && let Ok(results) = short_res {
            let err = results.into_iter().next().and_then(|r| r.error).unwrap_or_default();
            tracing::error!(exchange = %short_ex, error = %err, "Short leg order rejected");
        }
        if let Err(e) = &long_res {
            tracing::error!(exchange = %long_ex, error = %e, "Long leg placement failed");
        } else if !long_ok && let Ok(results) = long_res {
            let err = results.into_iter().next().and_then(|r| r.error).unwrap_or_default();
            tracing::error!(exchange = %long_ex, error = %err, "Long leg order rejected");
        }

        if !short_ok || !long_ok {
            // At least one leg didn't land. Cancel any orders that may have
            // been accepted on either venue to avoid leaving an unhedged leg.
            let _ = cx.broker(&short_ex)?.cancel_all_orders(self.symbol()).await;
            let _ = cx.broker(&long_ex)?.cancel_all_orders(self.symbol()).await;
            tracing::warn!("Entry incomplete — cancelling all orders on both venues, returning to Flat");
            self.state.go_flat();
        }
        Ok(())
    }

    async fn exit(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        tracing::info!(spread = %self.state.abs_rate_spread(), "Exiting arb position");
        self.state.transition(ArbPhase::Exiting);

        // For each leg, close whatever position we actually hold (handles
        // partial-fill asymmetry — close exactly what exists on that venue).
        for leg in [self.state.leg_a.clone(), self.state.leg_b.clone()].into_iter().flatten() {
            let pos = self.net_position(&leg.exchange);
            if pos.is_zero() {
                continue;
            }
            let (close_side, qty) =
                if pos.is_sign_positive() { (Side::Sell, pos) } else { (Side::Buy, -pos) };
            let order = self.make_order(&leg.exchange, close_side, qty, true);
            let broker = cx.broker(&leg.exchange)?;
            if let Err(e) = broker.place_orders(&[order]).await {
                tracing::error!(exchange = %leg.exchange, error = %e, "Close order failed");
            }
        }
        Ok(())
    }

    async fn emergency_flatten(&mut self, cx: &ActorContext, reason: &str) -> Result<(), BotError> {
        tracing::warn!(reason, "Emergency flatten");
        let exchanges = [self.config.exchange_a.clone(), self.config.exchange_b.clone()];
        let mut all_ok = true;
        for ex in &exchanges {
            let broker = cx.broker(ex)?;
            let _ = broker.cancel_all_orders(self.symbol()).await;
            let pos = self.net_position(ex);
            if pos.is_zero() {
                continue;
            }
            let (close_side, qty) =
                if pos.is_sign_positive() { (Side::Sell, pos) } else { (Side::Buy, -pos) };
            let mut order = self.make_order(ex, close_side, qty, true);
            order.order_type = OrderType::Market; // force IoC regardless of config
            match broker.place_orders(&[order]).await {
                Ok(results) if results.first().is_some_and(|r| r.success) => {}
                Ok(results) => {
                    let err = results
                        .first()
                        .and_then(|r| r.error.as_deref())
                        .unwrap_or("order rejected");
                    tracing::error!(exchange = %ex, error = %err, "Emergency flatten order rejected — MANUAL INTERVENTION REQUIRED");
                    all_ok = false;
                }
                Err(e) => {
                    tracing::error!(exchange = %ex, error = %e, "Emergency flatten order failed — MANUAL INTERVENTION REQUIRED");
                    all_ok = false;
                }
            }
        }
        if !all_ok {
            // Flatten order didn't land. Request shutdown so the operator
            // can see the incomplete flatten in the harness exit log and
            // close the position manually.
            cx.request_shutdown();
        }
        // Transition state to Flat regardless — staying in Entering/Exiting
        // would cause more entry attempts on top of an open position.
        self.state.go_flat();
        Ok(())
    }

    fn both_legs_fully_entered(&self) -> bool {
        let at_target = |leg: &ArbLeg| {
            let pos = self.net_position(&leg.exchange);
            match leg.entry_side {
                Side::Buy => pos >= leg.target_size,
                Side::Sell => pos <= -leg.target_size,
            }
        };
        matches!(
            (&self.state.leg_a, &self.state.leg_b),
            (Some(a), Some(b)) if at_target(a) && at_target(b)
        )
    }

    fn both_legs_flat(&self) -> bool {
        self.net_position(&self.config.exchange_a).is_zero()
            && self.net_position(&self.config.exchange_b).is_zero()
    }
}

#[async_trait]
impl Actor for FundingArbActor {
    async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        self.config
            .validate()
            .map_err(|e| BotError::config(format!("funding-arb config invalid: {e}")))?;
        tracing::info!(
            exchange_a = %self.config.exchange_a,
            exchange_b = %self.config.exchange_b,
            entry = %self.config.entry_threshold,
            exit = %self.config.exit_threshold,
            size = %self.config.order_size,
            "Funding arb actor started"
        );
        // Cancel stale orders on both venues.
        for ex in [&self.config.exchange_a, &self.config.exchange_b] {
            let _ = cx.broker(ex)?.cancel_all_orders(self.symbol()).await;
        }
        Ok(())
    }

    async fn wind_down(
        &mut self,
        _reason: &WindDownReason,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        // WindDownReason intentionally ignored: position-or-not is the right
        // discriminant here. If we hold a hedge, emergency_flatten is correct
        // regardless of why we're shutting down.
        if self.state.phase == ArbPhase::Flat {
            for ex in [&self.config.exchange_a, &self.config.exchange_b] {
                let _ = cx.broker(ex)?.cancel_all_orders(self.symbol()).await;
            }
        } else {
            let _ = self.emergency_flatten(cx, "shutdown").await;
        }
        tracing::info!(
            cycles = self.state.cycles_completed,
            net_delta = %self.net_delta(),
            "Funding arb final stats"
        );
        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        let inv_a = self.inventory.get(&self.config.exchange_a).cloned().unwrap_or_default();
        let inv_b = self.inventory.get(&self.config.exchange_b).cloned().unwrap_or_default();
        serde_json::json!({
            "phase": format!("{:?}", self.state.phase),
            "rate_a": self.state.rate_a.to_string(),
            "rate_b": self.state.rate_b.to_string(),
            "spread": self.state.abs_rate_spread().to_string(),
            "net_delta": self.net_delta().to_string(),
            "mark_a": self.state.mark_a.to_string(),
            "mark_b": self.state.mark_b.to_string(),
            "inventory_a": inv_a,
            "inventory_b": inv_b,
            "cycles_completed": self.state.cycles_completed,
        })
    }
}

#[async_trait]
impl EventHandler<MarkPriceUpdate> for FundingArbActor {
    async fn on_event(
        &mut self,
        event: MarkPriceUpdate,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        if event.symbol != self.symbol() {
            return Ok(());
        }
        if event.exchange == self.config.exchange_a {
            self.state.mark_a = event.mark_price;
            // Only update rate when the venue reported one — None means the
            // update carried no funding data (e.g. HL AllMids), not zero rate.
            if let Some(rate) = event.funding_rate {
                self.state.rate_a = rate;
                self.state.has_rate_a = true;
            }
        } else if event.exchange == self.config.exchange_b {
            self.state.mark_b = event.mark_price;
            if let Some(rate) = event.funding_rate {
                self.state.rate_b = rate;
                self.state.has_rate_b = true;
            }
        } else {
            return Ok(());
        }

        // Entry trigger: only when flat, both marks valid, both venues have
        // reported at least one real funding rate (not just the default zero),
        // rates are sane, spread is wide, and the flat-hold window has elapsed.
        let hold_elapsed = self
            .state
            .phase_entered_at
            .map_or(true, |t| t.elapsed().as_secs() >= self.config.min_flat_hold_secs);

        if self.state.phase == ArbPhase::Flat
            && hold_elapsed
            && self.state.has_rate_a
            && self.state.has_rate_b
            && self.state.abs_rate_spread() > self.config.entry_threshold
            && self.rates_sane()
            && !self.state.mark_a.is_zero()
            && !self.state.mark_b.is_zero()
        {
            self.enter(cx).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Trade> for FundingArbActor {
    async fn on_event(&mut self, event: Trade, _cx: &ActorContext) -> Result<(), BotError> {
        if event.symbol != self.symbol() {
            return Ok(());
        }
        // Same wallet may carry orders from other strategies / manual placements.
        // Only fold fills back into our inventory if we own the client_id.
        let is_ours =
            event.client_id.as_deref().is_some_and(|cid| self.my_client_ids.contains(cid));
        if !is_ours {
            tracing::debug!(
                exchange = %event.exchange,
                cid = ?event.client_id,
                "ignoring foreign fill (not in my_client_ids)"
            );
            return Ok(());
        }
        let Some(inv) = self.inventory.get_mut(&event.exchange) else {
            return Ok(());
        };
        inv.record_fill(event.side, event.price, event.quantity);
        tracing::info!(
            exchange = %event.exchange,
            side = %event.side,
            qty = %event.quantity,
            price = %event.price,
            net = %inv.net_position,
            "arb fill"
        );
        Ok(())
    }
}

#[async_trait]
impl EventHandler<BookUpdate> for FundingArbActor {
    async fn on_event(&mut self, event: BookUpdate, _cx: &ActorContext) -> Result<(), BotError> {
        if event.symbol != self.symbol() {
            return Ok(());
        }
        self.books.insert(event.exchange, event.orderbook);
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Tick> for FundingArbActor {
    async fn on_event(&mut self, _event: Tick, cx: &ActorContext) -> Result<(), BotError> {
        // Connection-health checks first — if either broker has lost its WS,
        // we'd be running blind on stale state. Request shutdown so the
        // harness can wind down (cancel orders, log final stats) cleanly.
        for ex in [&self.config.exchange_a, &self.config.exchange_b] {
            if let Ok(broker) = cx.broker(ex)
                && broker.is_disconnected()
            {
                tracing::error!(
                    exchange = %ex,
                    "broker reports permanent disconnect — requesting harness shutdown"
                );
                cx.request_shutdown();
                return Ok(());
            }
        }
        // On a transparent reconnect (or message-stream gap) the broker
        // raises a one-shot reconcile signal. Sync our `InventoryTracker`
        // against actual on-venue positions for any leg that flagged.
        for ex in [self.config.exchange_a.clone(), self.config.exchange_b.clone()] {
            let Ok(broker) = cx.broker(&ex) else { continue };
            if !broker.take_reconcile_signal() {
                continue;
            }
            tracing::warn!(exchange = %ex, "WS reconnect detected — reconciling positions");
            match broker.get_positions().await {
                Ok(positions) => {
                    let venue_pos = positions
                        .iter()
                        .find(|p| p.symbol == self.symbol())
                        .map_or(Decimal::ZERO, |p| match p.side {
                            Some(Side::Buy) => p.size,
                            Some(Side::Sell) => -p.size,
                            None => Decimal::ZERO,
                        });
                    let our_pos = self.net_position(&ex);
                    if venue_pos != our_pos {
                        tracing::warn!(
                            exchange = %ex,
                            tracked = %our_pos,
                            actual = %venue_pos,
                            "position drift after reconnect — resetting tracker"
                        );
                        if let Some(inv) = self.inventory.get_mut(&ex) {
                            inv.net_position = venue_pos;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(exchange = %ex, error = %e, "reconcile get_positions failed");
                }
            }
        }
        match self.state.phase {
            ArbPhase::Flat => {
                tracing::info!(
                    rate_a = %self.state.rate_a,
                    rate_b = %self.state.rate_b,
                    spread = %self.state.abs_rate_spread(),
                    "Monitoring funding rates"
                );
            }
            ArbPhase::Entering => {
                if self.state.phase_timed_out(self.config.phase_timeout_secs) {
                    self.emergency_flatten(cx, "entering phase timed out").await?;
                    return Ok(());
                }
                if self.both_legs_fully_entered() {
                    tracing::info!("Both legs filled, position active");
                    self.state.transition(ArbPhase::Active);
                }
            }
            ArbPhase::Active => {
                let delta = self.net_delta().abs();
                if delta > self.config.max_delta_imbalance {
                    self.emergency_flatten(cx, "delta imbalance exceeded").await?;
                    return Ok(());
                }
                if self.state.abs_rate_spread() < self.config.exit_threshold {
                    self.exit(cx).await?;
                    return Ok(());
                }
                tracing::info!(
                    spread = %self.state.abs_rate_spread(),
                    delta = %self.net_delta(),
                    "Active arb position"
                );
            }
            ArbPhase::Exiting => {
                if self.state.phase_timed_out(self.config.phase_timeout_secs) {
                    self.emergency_flatten(cx, "exiting phase timed out").await?;
                    return Ok(());
                }
                if self.both_legs_flat() {
                    self.state.cycles_completed += 1;
                    tracing::info!(cycles = self.state.cycles_completed, "Arb cycle completed");
                    self.state.go_flat();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bb_core::broker::Broker;
    use bb_core::events::{MarkPriceUpdate, Tick, Trade};
    use bb_core::harness::testing::{MockBroker, ScriptedFeed};
    use bb_core::harness::{ActorSpec, HarnessBuilder};
    use bb_core::helpers::ClientIdIssuer;
    use bb_core::types::Side;
    use rust_decimal::Decimal;

    use super::*;
    use crate::config::{FundingArbConfig, OrderMode};

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn test_config() -> FundingArbConfig {
        FundingArbConfig {
            symbol: "BTC-PERP".into(),
            exchange_a: "bullet".into(),
            exchange_b: "hl".into(),
            entry_threshold: d("0.001"),
            exit_threshold: d("0.0001"),
            order_size: d("1"),
            max_delta_imbalance: d("10"),
            max_funding_rate: d("0.01"),
            order_mode: OrderMode::Aggressive,
            phase_timeout_secs: 3600,
            slippage: d("0"),
            min_flat_hold_secs: 0,
        }
    }

    fn mark(exchange: &str, mark_price: &str, rate: Option<&str>) -> MarkPriceUpdate {
        MarkPriceUpdate {
            exchange: exchange.into(),
            symbol: "BTC-PERP".into(),
            mark_price: d(mark_price),
            funding_rate: rate.map(d),
        }
    }

    fn fill(exchange: &str, side: Side, cid: &str) -> Trade {
        Trade {
            exchange: exchange.into(),
            symbol: "BTC-PERP".into(),
            order_id: String::new(),
            trade_id: None,
            client_id: Some(cid.into()),
            side,
            price: d("100"),
            quantity: d("1"),
            timestamp: None,
        }
    }

    fn foreign_trade() -> Trade {
        Trade {
            exchange: "other".into(),
            symbol: "BTC-PERP".into(),
            order_id: String::new(),
            trade_id: None,
            client_id: None,
            side: Side::Buy,
            price: d("100"),
            quantity: d("1"),
            timestamp: None,
        }
    }

    // -------------------------------------------------------------------------
    // Zero-rate guard: no entry until both venues have reported a real rate
    // -------------------------------------------------------------------------

    /// Exchange B sends a mark price but no funding rate. Entry must be blocked
    /// even when the spread between the default zero and a real rate looks wide.
    #[tokio::test(flavor = "current_thread")]
    async fn no_entry_until_both_rates_received() {
        let bullet = MockBroker::shared("bullet");
        let hl = MockBroker::shared("hl");

        // rate_a = 0.002, but exchange B only sends mark_price (no funding rate).
        // has_rate_b stays false → entry guard must block.
        let marks = ScriptedFeed::new(vec![
            mark("bullet", "100", Some("0.002")), // has_rate_a = true
            MarkPriceUpdate {
                exchange: "hl".into(),
                symbol: "BTC-PERP".into(),
                mark_price: d("100"),
                funding_rate: None, // has_rate_b stays false
            },
        ]);
        let actor = FundingArbActor::with_client_ids(test_config(), ClientIdIssuer::new());
        let harness = HarnessBuilder::new()
            .wire_broker("bullet", bullet.clone() as Arc<dyn bb_core::broker::Broker>)
            .wire_broker("hl", hl.clone() as Arc<dyn bb_core::broker::Broker>)
            .wire_feed_named("marks", marks)
            .wire_actor(
                ActorSpec::new("funding-arb", actor).sub::<MarkPriceUpdate>().sub::<Tick>(),
            )
            .build()
            .unwrap();
        harness.run().await.unwrap();
        assert_eq!(bullet.placed_count().await, 0, "should not enter without both rates");
        assert_eq!(hl.placed_count().await, 0, "should not enter without both rates");
    }

    // -------------------------------------------------------------------------
    // enter() failure: both brokers reject → state goes back to Flat
    // -------------------------------------------------------------------------

    /// When entry order placements fail, the actor must cancel any potentially
    /// placed orders and return to Flat — not stay stuck in Entering.
    #[tokio::test(flavor = "current_thread")]
    async fn enter_failure_returns_to_flat_and_cancels() {
        let bullet = MockBroker::shared("bullet");
        let hl = MockBroker::shared("hl");

        // Reject both legs.
        bullet
            .queue_place_response(Err(BotError::exchange("venue error", false)))
            .await;
        hl.queue_place_response(Err(BotError::exchange("venue error", false))).await;

        // Wide spread triggers entry on the second mark event.
        let marks = ScriptedFeed::new(vec![
            mark("bullet", "100", Some("0.002")),
            mark("hl", "100", Some("0.0005")), // spread = 0.0015 > 0.001 → entry
        ]);

        let actor = FundingArbActor::with_client_ids(test_config(), ClientIdIssuer::new());
        let harness = HarnessBuilder::new()
            .wire_broker("bullet", bullet.clone() as Arc<dyn bb_core::broker::Broker>)
            .wire_broker("hl", hl.clone() as Arc<dyn bb_core::broker::Broker>)
            .wire_feed_named("marks", marks)
            .wire_actor(
                ActorSpec::new("funding-arb", actor).sub::<MarkPriceUpdate>().sub::<Tick>(),
            )
            .build()
            .unwrap();
        harness.run().await.unwrap();

        // Entry was attempted (place_orders called), then failed.
        assert_eq!(bullet.placed_count().await, 1, "one entry attempt");
        // After the failure, cancel_all_orders should have been called on both
        // venues to clean up any orders that might have landed on one side.
        let bullet_hist = bullet.history().await;
        let hl_hist = hl.history().await;
        let bullet_cancels = bullet_hist.iter().filter(|c| c.method == "cancel_all_orders").count();
        let hl_cancels = hl_hist.iter().filter(|c| c.method == "cancel_all_orders").count();
        assert!(bullet_cancels >= 1, "cancel_all_orders should be called on bullet after failure");
        assert!(hl_cancels >= 1, "cancel_all_orders should be called on hl after failure");
    }

    // -------------------------------------------------------------------------
    // Full `Flat → Entering → Active → Exiting → Flat` cycle driven by
    /// `ScriptedFeed`s and `MockBroker`.
    ///
    /// Three parallel feeds run in interleaved fashion (`current_thread` +
    /// `yield_now` between events). Fills are placed 3 rounds after the entry
    /// trigger so they're guaranteed to arrive after `enter()` has issued the
    /// `client_ids` they carry.
    ///
    /// Round-by-round:
    ///   0: `rate_a` set, skip, tick (Flat — `mark_b` still 0)
    ///   1: `rate_b` set → ENTRY (cids "1","2"), skip, tick (Entering 0/2)
    ///   2: narrow `rate_a`, skip, tick (Entering 0/2)
    ///   3: narrow `rate_b`, fill cid=1 (bullet), tick (Entering 1/2)
    ///   4: padding, fill cid=2 (hl), tick (Entering → Active)
    ///   5: padding, skip, tick (Active → EXIT, cids "3","4" issued)
    ///   6: padding, close cid=3 (bullet), tick (Exiting 1/2)
    ///   7: padding, close cid=4 (hl), tick (Exiting → Flat, cycles=1)
    ///   8: padding, skip, tick (Flat, no-op)
    #[tokio::test(flavor = "current_thread")]
    async fn full_arb_cycle_flat_to_entering_to_active_to_exiting_to_flat() {
        let bullet = MockBroker::shared("bullet");
        let hl = MockBroker::shared("hl");

        // rate_a > rate_b → bullet is the short leg (cid="1"), hl is the long leg (cid="2")
        let marks = ScriptedFeed::new(vec![
            mark("bullet", "100", Some("0.002")),   // [0] rate_a wide
            mark("hl", "100", Some("0.0005")),      // [1] rate_b wide → ENTRY
            mark("bullet", "100", Some("0.00005")), // [2] narrow rate_a
            mark("hl", "100", Some("0.00003")),     // [3] narrow rate_b (spread 0.00002 < 0.0001)
            mark("bullet", "100", Some("0.00005")), // [4] padding
            mark("hl", "100", Some("0.00003")),     // [5] padding
            mark("bullet", "100", Some("0.00005")), // [6] padding
            mark("hl", "100", Some("0.00003")),     // [7] padding
            mark("bullet", "100", Some("0.00005")), // [8] padding
        ]);
        let trades = ScriptedFeed::new(vec![
            foreign_trade(),                 // [0] skip
            foreign_trade(),                 /* [1] skip (entry fires in this round — fills not
                                              * yet) */
            foreign_trade(),                 // [2] skip
            fill("bullet", Side::Sell, "1"), // [3] short leg filled (cid="1" guaranteed in my_ids)
            fill("hl", Side::Buy, "2"),      // [4] long leg filled
            foreign_trade(),                 // [5] skip
            fill("bullet", Side::Buy, "3"),  // [6] close short (cid="3" issued by exit in round 5)
            fill("hl", Side::Sell, "4"),     // [7] close long
            foreign_trade(),                 // [8] skip
        ]);
        let ticks = ScriptedFeed::new(vec![
            Tick::now(), // [0] Flat, no-op
            Tick::now(), // [1] Entering, 0/2 filled
            Tick::now(), // [2] Entering, 0/2 filled
            Tick::now(), // [3] Entering, 1/2 filled (only bullet)
            Tick::now(), // [4] Entering → both filled → Active
            Tick::now(), // [5] Active → spread narrow → EXIT (cids "3","4" issued)
            Tick::now(), // [6] Exiting, 1/2 closed
            Tick::now(), // [7] Exiting → both flat → cycles=1 → Flat
            Tick::now(), // [8] Flat, no-op
        ]);

        let actor = FundingArbActor::with_client_ids(test_config(), ClientIdIssuer::new());
        let harness = HarnessBuilder::new()
            .wire_broker("bullet", bullet.clone() as Arc<dyn Broker>)
            .wire_broker("hl", hl.clone() as Arc<dyn Broker>)
            .wire_feed_named("marks", marks)
            .wire_feed_named("trades", trades)
            .wire_feed_named("ticks", ticks)
            .wire_actor(
                ActorSpec::new("funding-arb", actor)
                    .sub::<MarkPriceUpdate>()
                    .sub_critical::<Trade>()
                    .sub::<Tick>(),
            )
            .build()
            .unwrap();

        harness.run().await.unwrap();

        // Both brokers should have received exactly 2 place_orders calls:
        // one for entry and one for exit.
        assert_eq!(bullet.placed_count().await, 2, "bullet: expected entry + exit");
        assert_eq!(hl.placed_count().await, 2, "hl: expected entry + exit");

        // Verify entry orders: bullet=short (Sell), hl=long (Buy)
        let bullet_hist = bullet.history().await;
        let place_calls: Vec<_> =
            bullet_hist.iter().filter(|c| c.method == "place_orders").collect();
        assert_eq!(place_calls[0].orders[0].side, Side::Sell, "bullet entry is Sell");
        assert_eq!(place_calls[1].orders[0].side, Side::Buy, "bullet close is Buy");

        let hl_hist = hl.history().await;
        let hl_place: Vec<_> = hl_hist.iter().filter(|c| c.method == "place_orders").collect();
        assert_eq!(hl_place[0].orders[0].side, Side::Buy, "hl entry is Buy");
        assert_eq!(hl_place[1].orders[0].side, Side::Sell, "hl close is Sell");
    }
}
