//! Avellaneda-Stoikov market maker as an event-driven actor.
//!
//! Subscribed events:
//!   - `BookUpdate` — update mid price + volatility estimator.
//!   - `Trade` — canonical source of inventory / realized PnL. Nulls
//!     `last_quote_at` so the next tick re-builds the ladder around the new
//!     reservation price.
//!   - `OrderLifecycle` — purely observational; logged for debugging. Not
//!     used for position updates (that's the canonical-source invariant).
//!   - `Tick` — refresh the quote ladder when `order_refresh_secs` elapsed.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, OrderLifecycle, Tick, Trade};
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::{ClientIdIssuer, InventoryTracker};
use bb_core::types::{CancelOrder, NewOrder, OrderBook, Side};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::Serialize;

use crate::config::AvellanedaStoikovConfig;
use crate::model::{LadderRung, Quote, SpreadBounds, quote_ladder};
use crate::volatility::Volatility;

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

pub struct AvellanedaStoikovActor {
    config: AvellanedaStoikovConfig,
    slots: Vec<QuoteSlot>,
    inventory: InventoryTracker,
    client_ids: ClientIdIssuer,
    volatility: Volatility,
    book: Option<OrderBook>,
    last_mid: Option<Decimal>,
    last_quote_at: Option<Instant>,
}

impl AvellanedaStoikovActor {
    pub fn new(config: AvellanedaStoikovConfig) -> Self {
        let volatility = Volatility::new(config.vol_window_secs);
        Self {
            config,
            slots: Vec::new(),
            inventory: InventoryTracker::new(),
            client_ids: ClientIdIssuer::new(),
            volatility,
            book: None,
            last_mid: None,
            last_quote_at: None,
        }
    }

    fn exchange(&self) -> &str {
        &self.config.exchange
    }

    fn symbol(&self) -> &str {
        &self.config.symbol
    }

    fn check_fee_floor(&self) -> Result<(), BotError> {
        let Some(fees) = &self.config.fees else {
            tracing::warn!(
                "No [strategy.avellaneda-stoikov.fees] configured — skipping fee-floor check."
            );
            return Ok(());
        };
        let round_trip_bps = Decimal::from(2) * fees.maker_bps;
        let required_bps = round_trip_bps * fees.min_spread_fee_multiplier;
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

    fn build_ladder(&self, mid: Decimal) -> Option<(Quote, Vec<LadderRung>)> {
        let sigma = self.volatility.sigma()?;
        let mid_f = mid.to_f64()?;
        let gamma = self.config.gamma.to_f64()?;
        let kappa = self.config.kappa.to_f64()?;
        let tau = self.config.order_horizon_secs as f64;
        let max_pos = self.config.max_position.to_f64()?;
        let target = self.config.inventory_target.to_f64().unwrap_or(0.0);
        let pos = self.inventory.net_position.to_f64()?;
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

    fn should_refresh(&self, now: Instant) -> bool {
        match self.last_quote_at {
            None => true,
            Some(last) => {
                now.duration_since(last)
                    >= Duration::from_secs(self.config.order_refresh_secs.max(1))
            }
        }
    }

    async fn cancel_live_quotes(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        let mut cancels: Vec<CancelOrder> = Vec::new();
        for slot in &self.slots {
            let (oid, cid) = (slot.order_id.clone(), slot.client_id.clone());
            if oid.is_none() && cid.is_none() {
                continue;
            }
            cancels.push(CancelOrder {
                symbol: self.symbol().to_string(),
                order_id: oid.unwrap_or_default(),
                client_id: cid,
            });
        }
        if !cancels.is_empty() {
            cx.broker(self.exchange())?.cancel_orders(&cancels).await?;
        }
        self.slots.clear();
        Ok(())
    }

    fn size_for_level(&self, level: usize) -> Decimal {
        let step = self.config.order_level_amount_step;
        if step.is_zero() {
            return self.config.order_size;
        }
        self.config.order_size * (Decimal::ONE + step * Decimal::from(level as u64))
    }

    async fn refresh_quotes(
        &mut self,
        cx: &ActorContext,
        mid: Decimal,
    ) -> Result<(), BotError> {
        let Some((inner, rungs)) = self.build_ladder(mid) else {
            tracing::debug!("Not enough volatility samples yet to quote");
            return Ok(());
        };

        let can_buy = self.inventory.net_position < self.config.max_position;
        let can_sell = self.inventory.net_position > -self.config.max_position;

        self.cancel_live_quotes(cx).await?;
        let order_type = self.config.order_type;

        let mut orders: Vec<NewOrder> = Vec::new();
        let mut intents: Vec<(Side, usize, Decimal, Decimal, String)> = Vec::new();

        for rung in &rungs {
            let size = self.size_for_level(rung.level);
            if can_buy {
                let Some(bid_price) = Decimal::from_f64(rung.bid) else {
                    return Err(BotError::strategy("Non-finite bid from A-S ladder"));
                };
                let cid = self.client_ids.issue();
                orders.push(NewOrder {
                    symbol: self.symbol().to_string(),
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
                let cid = self.client_ids.issue();
                orders.push(NewOrder {
                    symbol: self.symbol().to_string(),
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
                net_pos = %self.inventory.net_position,
                max = %self.config.max_position,
                "At max position on both sides; skipping refresh"
            );
            return Ok(());
        }

        let results = cx.broker(self.exchange())?.place_orders(&orders).await?;
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
            net_pos = %self.inventory.net_position,
            "A-S ladder refreshed"
        );
        Ok(())
    }
}

#[async_trait]
impl Actor for AvellanedaStoikovActor {
    async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        self.check_fee_floor()?;
        let broker = cx.broker(self.exchange())?;
        broker.cancel_all_orders(self.symbol()).await?;
        let book = broker.get_orderbook(self.symbol(), 20).await?;
        let mid = book.midpoint().ok_or_else(|| {
            BotError::strategy("No orderbook data available to compute mid price")
        })?;
        self.last_mid = Some(mid);
        if let Some(m) = mid.to_f64() {
            self.volatility.push(m, Instant::now());
        }
        self.book = Some(book);

        tracing::info!(
            mid = %mid,
            gamma = %self.config.gamma,
            kappa = %self.config.kappa,
            tau = self.config.order_horizon_secs,
            "A-S actor started — awaiting volatility samples before first quote"
        );
        Ok(())
    }

    async fn wind_down(
        &mut self,
        _reason: &WindDownReason,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        let broker = cx.broker(self.exchange())?;
        tracing::info!("Shutting down A-S actor — cancelling all orders");
        let _ = broker.cancel_all_orders(self.symbol()).await;
        tracing::info!(
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            "A-S final stats"
        );
        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "net_position": self.inventory.net_position.to_string(),
            "realized_pnl": self.inventory.realized_pnl.to_string(),
            "total_fills": self.inventory.total_fills,
            "slots": self.slots,
            "sigma": self.volatility.sigma(),
            "vol_samples": self.volatility.sample_count(),
        })
    }
}

#[async_trait]
impl EventHandler<BookUpdate> for AvellanedaStoikovActor {
    async fn on_event(&mut self, event: BookUpdate, _cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange != self.exchange() || event.symbol != self.symbol() {
            return Ok(());
        }
        if let Some(mid) = event.orderbook.midpoint() {
            self.last_mid = Some(mid);
            if let Some(m) = mid.to_f64() {
                self.volatility.push(m, Instant::now());
            }
        }
        self.book = Some(event.orderbook);
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Trade> for AvellanedaStoikovActor {
    async fn on_event(&mut self, event: Trade, _cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange != self.exchange() || event.symbol != self.symbol() {
            return Ok(());
        }
        self.inventory.record_fill(event.side, event.price, event.quantity);
        // Drop the filled slot from our local view.
        // Match on (Some(cid) == Some(cid)) or (Some(oid) == Some(oid)).
        // The explicit `Some` gates prevent the naive `a == b` from matching
        // `None == None` — which would otherwise wipe every slot that hasn't
        // yet received its exchange order_id the moment a fill arrives
        // without a client_id (possible on HL for orders placed without cloid).
        let event_cid = event.client_id.as_deref();
        let event_oid = Some(event.order_id.as_str()).filter(|s| !s.is_empty());
        self.slots.retain(|slot| {
            let cid_match =
                event_cid.is_some() && slot.client_id.as_deref() == event_cid;
            let oid_match =
                event_oid.is_some() && slot.order_id.as_deref() == event_oid;
            !(cid_match || oid_match)
        });
        // Force a re-quote on the next tick so both sides follow the new
        // reservation price.
        self.last_quote_at = None;

        tracing::info!(
            side = %event.side,
            price = %event.price,
            qty = %event.quantity,
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            "A-S fill"
        );
        Ok(())
    }
}

#[async_trait]
impl EventHandler<OrderLifecycle> for AvellanedaStoikovActor {
    async fn on_event(
        &mut self,
        event: OrderLifecycle,
        _cx: &ActorContext,
    ) -> Result<(), BotError> {
        if event.exchange != self.exchange() || event.order.symbol != self.symbol() {
            return Ok(());
        }
        // Learn exchange-assigned order_id once the venue acks the place.
        if let Some(cid) = event.order.client_id.as_deref() {
            if let Some(slot) =
                self.slots.iter_mut().find(|s| s.client_id.as_deref() == Some(cid))
            {
                if slot.order_id.is_none() && !event.order.id.is_empty() {
                    slot.order_id = Some(event.order.id.clone());
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Tick> for AvellanedaStoikovActor {
    async fn on_event(&mut self, _event: Tick, cx: &ActorContext) -> Result<(), BotError> {
        let Some(mid) = self.last_mid else {
            return Ok(());
        };
        let now = Instant::now();
        if self.should_refresh(now) {
            self.refresh_quotes(cx, mid).await?;
        }

        let best_bid = self.slots.iter().filter(|s| s.side == Side::Buy).map(|s| s.price).max();
        let best_ask = self.slots.iter().filter(|s| s.side == Side::Sell).map(|s| s.price).min();
        tracing::info!(
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            slots = self.slots.len(),
            best_bid = ?best_bid.map(|p| p.to_string()),
            best_ask = ?best_ask.map(|p| p.to_string()),
            vol_samples = self.volatility.sample_count(),
            "A-S tick"
        );
        Ok(())
    }
}
