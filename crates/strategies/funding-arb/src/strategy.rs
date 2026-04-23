//! Funding rate arbitrage as an event-driven actor.
//!
//! Two-venue delta-neutral strategy. Subscribed events:
//!   - `MarkPriceUpdate` — cache mark + funding per venue; trigger entry.
//!   - `Trade` — canonical source of position changes *per venue*. One
//!     `InventoryTracker` per venue via a `HashMap<exchange, tracker>`.
//!     The net_delta across both trackers should tend to zero while Active.
//!   - `BookUpdate` — cache best bid/ask for aggressive pricing.
//!   - `Tick` — phase-timeout checks, delta-imbalance guard, exit condition.
//!
//! Key fix vs. pre-harness version: position updates come from `Trade` only,
//! so the "double-count via Trade + OrderUpdate" bug that used to happen on
//! HL is structurally impossible. The `order.symbol` vs `exchange` name
//! confusion is gone too — events are typed and carry explicit `exchange`.

use std::collections::HashMap;

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, MarkPriceUpdate, Tick, Trade};
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::InventoryTracker;
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
}

impl FundingArbActor {
    pub fn new(config: FundingArbConfig) -> Self {
        let mut inventory = HashMap::new();
        inventory.insert(config.exchange_a.clone(), InventoryTracker::new());
        inventory.insert(config.exchange_b.clone(), InventoryTracker::new());
        Self {
            config,
            state: ArbState::new(),
            inventory,
            books: HashMap::new(),
        }
    }

    fn symbol(&self) -> &str {
        &self.config.symbol
    }

    fn net_position(&self, exchange: &str) -> Decimal {
        self.inventory.get(exchange).map(|i| i.net_position).unwrap_or(Decimal::ZERO)
    }

    /// Net delta across both legs — should be near zero when Active.
    fn net_delta(&self) -> Decimal {
        self.net_position(&self.config.exchange_a) + self.net_position(&self.config.exchange_b)
    }

    fn rates_sane(&self) -> bool {
        self.state.rate_a.abs() < self.config.max_funding_rate
            && self.state.rate_b.abs() < self.config.max_funding_rate
    }

    /// (short_exchange, long_exchange) — short the leg with the higher funding.
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
            if exchange == self.config.exchange_a {
                self.state.mark_a
            } else {
                self.state.mark_b
            }
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

    fn make_order(&self, exchange: &str, side: Side, size: Decimal, reduce_only: bool) -> NewOrder {
        NewOrder {
            symbol: self.symbol().to_string(),
            side,
            order_type: self.order_type(),
            price: self.aggressive_price(exchange, side),
            quantity: size,
            client_id: None,
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
        if let Err(e) = &short_res {
            tracing::error!(exchange = %short_ex, error = %e, "Short leg placement failed");
        }
        if let Err(e) = &long_res {
            tracing::error!(exchange = %long_ex, error = %e, "Long leg placement failed");
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
            let (close_side, qty) = if pos.is_sign_positive() {
                (Side::Sell, pos)
            } else {
                (Side::Buy, -pos)
            };
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
        for ex in [&self.config.exchange_a, &self.config.exchange_b] {
            let broker = cx.broker(ex)?;
            let _ = broker.cancel_all_orders(self.symbol()).await;
            let pos = self.net_position(ex);
            if pos.is_zero() {
                continue;
            }
            let (close_side, qty) = if pos.is_sign_positive() {
                (Side::Sell, pos)
            } else {
                (Side::Buy, -pos)
            };
            let mut order = self.make_order(ex, close_side, qty, true);
            order.order_type = OrderType::Market; // force IoC regardless of config
            let _ = broker.place_orders(&[order]).await;
        }
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
        if self.state.phase != ArbPhase::Flat {
            let _ = self.emergency_flatten(cx, "shutdown").await;
        } else {
            for ex in [&self.config.exchange_a, &self.config.exchange_b] {
                let _ = cx.broker(ex)?.cancel_all_orders(self.symbol()).await;
            }
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
            self.state.rate_a = event.funding_rate;
        } else if event.exchange == self.config.exchange_b {
            self.state.mark_b = event.mark_price;
            self.state.rate_b = event.funding_rate;
        } else {
            return Ok(());
        }

        // Entry trigger: only when flat, both marks valid, rates sane, spread
        // wide, and we've been flat at least `min_flat_hold_secs`. The hold
        // window guards against spread wobble and post-flatten re-entry.
        let hold_elapsed = self
            .state
            .phase_entered_at
            .map(|t| t.elapsed().as_secs() >= self.config.min_flat_hold_secs)
            .unwrap_or(true);

        if self.state.phase == ArbPhase::Flat
            && hold_elapsed
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
                    tracing::info!(
                        cycles = self.state.cycles_completed,
                        "Arb cycle completed"
                    );
                    self.state.go_flat();
                }
            }
        }
        Ok(())
    }
}
