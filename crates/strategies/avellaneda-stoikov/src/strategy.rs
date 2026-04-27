//! Avellaneda-Stoikov market maker as an event-driven actor.
//!
//! Two modes, selected by config:
//!   - **Single-venue (default)**: `s` in the A-S formula comes from the
//!     trading venue's BookUpdate mid. Faithful to the paper.
//!   - **Fair-value MM**: when `reference_exchange` is set, `s` comes from
//!     `ReferencePriceUpdate` (e.g. Binance). The local book is still used
//!     for inventory tracking and would-cross checks but no longer drives
//!     `last_mid` or the volatility estimator. The textbook A-S inventory
//!     skew (`r = s − q·γ·σ²·τ`) is unchanged — only the source of `s`.
//!
//! Subscribed events:
//!   - `BookUpdate` — local book; drives `last_mid`/volatility only when no
//!     reference is configured. Always triggers a refresh attempt so we can
//!     react to inventory-cap toggles even without a reference.
//!   - `ReferencePriceUpdate` — when a reference is configured, this is
//!     the canonical source of `s` and the volatility estimator's input.
//!   - `Trade` — canonical source of inventory / realized PnL. Nulls
//!     `last_quote_at` so the next tick re-builds the ladder around the new
//!     reservation price.
//!   - `OrderLifecycle` — purely observational; logged for debugging. Not
//!     used for position updates (that's the canonical-source invariant).
//!   - `Tick` — fallback refresh when `order_refresh_secs` elapses, so we
//!     still re-quote in calm markets where book updates are sparse.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, OrderLifecycle, Tick, Trade};
use bb_exchange_binance::ReferencePriceUpdate;
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::{ClientIdIssuer, InventoryTracker};
use bb_core::types::{AmendOrder, CancelOrder, NewOrder, OrderBook, OrderStatus, Side};
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
    /// Set when `reference_exchange` is configured. When `Some`, `last_mid`
    /// and the volatility estimator are driven exclusively by the reference
    /// feed; local BookUpdate is used only for the local book itself.
    reference_last_seen: Option<Instant>,
    last_reconcile_at: Option<Instant>,
}

impl AvellanedaStoikovActor {
    pub fn new(config: AvellanedaStoikovConfig) -> Self {
        let volatility = Volatility::new(config.vol_window_secs);
        Self {
            config,
            slots: Vec::new(),
            inventory: InventoryTracker::new(),
            client_ids: ClientIdIssuer::session_seeded(),
            volatility,
            book: None,
            last_mid: None,
            last_quote_at: None,
            reference_last_seen: None,
            last_reconcile_at: None,
        }
    }

    fn uses_reference(&self) -> bool {
        self.config.reference_exchange.is_some()
    }

    fn reference_is_stale(&self, now: Instant) -> bool {
        match self.reference_last_seen {
            None => true,
            Some(t) => {
                now.duration_since(t)
                    > Duration::from_secs(self.config.reference_stale_secs.max(1))
            }
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

    fn should_force_refresh(&self, now: Instant) -> bool {
        match self.last_quote_at {
            None => true,
            Some(last) => {
                now.duration_since(last)
                    >= Duration::from_secs(self.config.order_refresh_secs.max(1))
            }
        }
    }

    fn size_for_level(&self, level: usize) -> Decimal {
        let step = self.config.order_level_amount_step;
        if step.is_zero() {
            return self.config.order_size;
        }
        self.config.order_size * (Decimal::ONE + step * Decimal::from(level as u64))
    }

    /// Diff the desired ladder against `self.slots` and reconcile via the
    /// minimal set of API calls:
    /// - slots whose target price is within `amend_threshold_bps` are kept
    ///   in place (no round-trip),
    /// - slots whose target moved are batch-amended atomically,
    /// - missing rungs are placed,
    /// - unmatched slots (e.g. when inventory caps disable a side) are
    ///   cancelled.
    /// The function is cheap when nothing has moved, so it's safe to call
    /// from `BookUpdate` for sub-second responsiveness.
    async fn refresh_quotes(
        &mut self,
        cx: &ActorContext,
        mid: Decimal,
    ) -> Result<(), BotError> {
        // Throttle: when ref feeds fire faster than Bullet REST can settle,
        // keep the actor draining its input channel by skipping refreshes
        // that arrive within the cooldown window. The next event after the
        // window will pick up the latest mid.
        let min_interval = Duration::from_millis(self.config.min_refresh_interval_ms);
        if min_interval > Duration::ZERO {
            if let Some(last) = self.last_quote_at {
                if Instant::now().duration_since(last) < min_interval {
                    return Ok(());
                }
            }
        }
        if self.uses_reference() && self.reference_is_stale(Instant::now()) {
            tracing::warn!(
                stale_secs = self.config.reference_stale_secs,
                "Reference feed stale — skipping refresh"
            );
            return Ok(());
        }
        let Some((inner, rungs)) = self.build_ladder(mid) else {
            tracing::debug!("Not enough volatility samples yet to quote");
            return Ok(());
        };

        let can_buy = self.inventory.net_position < self.config.max_position;
        let can_sell = self.inventory.net_position > -self.config.max_position;
        let order_type = self.config.order_type;
        let symbol = self.symbol().to_string();

        // Build target intents.
        let mut intents: Vec<(Side, usize, Decimal, Decimal)> = Vec::new();
        for rung in &rungs {
            let size = self.size_for_level(rung.level);
            if can_buy {
                let Some(bid_price) = Decimal::from_f64(rung.bid) else {
                    return Err(BotError::strategy("Non-finite bid from A-S ladder"));
                };
                intents.push((Side::Buy, rung.level, bid_price, size));
            }
            if can_sell {
                let Some(ask_price) = Decimal::from_f64(rung.ask) else {
                    return Err(BotError::strategy("Non-finite ask from A-S ladder"));
                };
                intents.push((Side::Sell, rung.level, ask_price, size));
            }
        }

        // PostOnly orders that would cross the local Bullet book are rejected
        // by the venue, and the atomic-batch amend means one bad rung tanks
        // the whole batch. Pre-empt that here. Only applies when the strategy
        // is using PostOnly — Limit/Market are allowed to cross.
        let post_only = matches!(order_type, bb_core::types::OrderType::PostOnly);

        // Reconciliation buckets.
        let mut matched = vec![false; self.slots.len()];
        let mut kept_slots: Vec<QuoteSlot> = Vec::new();
        let mut to_amend: Vec<AmendOrder> = Vec::new();
        let mut amend_intents: Vec<(usize, Side, usize, Decimal, Decimal, String)> = Vec::new();
        let mut to_place: Vec<NewOrder> = Vec::new();
        let mut place_intents: Vec<(Side, usize, Decimal, Decimal, String)> = Vec::new();

        for (side, level, price, size) in &intents {
            // Per-level threshold: outer rungs widen by `step × level` so we
            // don't burn round-trips chasing fair value on rungs that rarely
            // fill.
            let level_bps = self.config.amend_threshold_bps
                + self.config.amend_threshold_step_bps * Decimal::from(*level as u64);
            let threshold = mid * level_bps / Decimal::from(10_000);

            // Skip rungs that would cross the local book — PostOnly would
            // reject and revert the whole batch.
            if post_only
                && self
                    .book
                    .as_ref()
                    .is_some_and(|b| b.would_cross(*side, *price))
            {
                tracing::debug!(side = %side, level, price = %price, "skipping would-cross rung");
                // If we already have a slot at this (side, level), preserve
                // it untouched — the venue will keep our existing quote.
                if let Some(idx) = self
                    .slots
                    .iter()
                    .enumerate()
                    .find(|(i, s)| !matched[*i] && s.side == *side && s.level == *level)
                    .map(|(i, _)| i)
                {
                    matched[idx] = true;
                    kept_slots.push(self.slots[idx].clone());
                }
                continue;
            }

            let existing = self
                .slots
                .iter()
                .enumerate()
                .find(|(i, s)| !matched[*i] && s.side == *side && s.level == *level)
                .map(|(i, _)| i);

            match existing {
                Some(idx) => {
                    matched[idx] = true;
                    let slot = &self.slots[idx];
                    if (slot.price - *price).abs() < threshold {
                        kept_slots.push(slot.clone());
                        continue;
                    }
                    // Need an identifier to amend; if neither is set yet
                    // (place ack hasn't landed), keep the slot — we'll catch
                    // it on the next refresh once OrderLifecycle fires.
                    if slot.order_id.is_none() && slot.client_id.is_none() {
                        kept_slots.push(slot.clone());
                        continue;
                    }
                    let new_cid = self.client_ids.issue();
                    to_amend.push(AmendOrder {
                        cancel: CancelOrder {
                            symbol: symbol.clone(),
                            order_id: slot.order_id.clone().unwrap_or_default(),
                            client_id: slot.client_id.clone(),
                        },
                        new_order: NewOrder {
                            symbol: symbol.clone(),
                            side: *side,
                            order_type,
                            price: *price,
                            quantity: *size,
                            client_id: Some(new_cid.clone()),
                            reduce_only: false,
                        },
                    });
                    amend_intents.push((idx, *side, *level, *price, *size, new_cid));
                }
                None => {
                    let new_cid = self.client_ids.issue();
                    to_place.push(NewOrder {
                        symbol: symbol.clone(),
                        side: *side,
                        order_type,
                        price: *price,
                        quantity: *size,
                        client_id: Some(new_cid.clone()),
                        reduce_only: false,
                    });
                    place_intents.push((*side, *level, *price, *size, new_cid));
                }
            }
        }

        // Unmatched slots → cancel (e.g. inventory cap disabled this side).
        // Track slot indices so we can re-track any slot whose cancel ends
        // up reverting — otherwise we silently lose tracking of a still-live
        // order on Bullet, leaking it as an orphan.
        let mut to_cancel: Vec<CancelOrder> = Vec::new();
        let mut cancel_slot_indices: Vec<usize> = Vec::new();
        for (i, slot) in self.slots.iter().enumerate() {
            if matched[i] {
                continue;
            }
            if slot.order_id.is_none() && slot.client_id.is_none() {
                continue;
            }
            to_cancel.push(CancelOrder {
                symbol: symbol.clone(),
                order_id: slot.order_id.clone().unwrap_or_default(),
                client_id: slot.client_id.clone(),
            });
            cancel_slot_indices.push(i);
        }

        let n_cancels = to_cancel.len();
        let n_amends = to_amend.len();
        let n_places = to_place.len();

        if n_cancels == 0 && n_amends == 0 && n_places == 0 {
            // Everything within threshold and no add/remove churn — skip
            // updating last_quote_at so the Tick fallback can still force a
            // refresh if `order_refresh_secs` elapses.
            return Ok(());
        }

        let broker = cx.broker(self.exchange())?.clone();
        let mut any_failure = false;

        if !to_cancel.is_empty() {
            let results = broker.cancel_orders(&to_cancel).await?;
            for (slot_idx, res) in cancel_slot_indices.iter().zip(results.iter()) {
                if !res.success {
                    any_failure = true;
                    let slot = &self.slots[*slot_idx];
                    tracing::warn!(
                        side = %slot.side,
                        level = slot.level,
                        error = res.error.as_deref().unwrap_or("unknown"),
                        "unmatched-slot cancel reverted — retaining tracking to retry"
                    );
                    kept_slots.push(slot.clone());
                }
            }
        }

        if !to_amend.is_empty() {
            let results = broker.amend_orders(&to_amend).await?;
            for ((slot_idx, side, level, price, size, cid), res) in
                amend_intents.into_iter().zip(results.iter())
            {
                if !res.success {
                    any_failure = true;
                    tracing::warn!(
                        side = %side,
                        level,
                        price = %price,
                        error = res.error.as_deref().unwrap_or("unknown"),
                        "Failed to amend A-S quote — retaining old slot"
                    );
                    kept_slots.push(self.slots[slot_idx].clone());
                    continue;
                }
                kept_slots.push(QuoteSlot {
                    side,
                    level,
                    price,
                    size,
                    client_id: Some(cid),
                    order_id: res.order_id.clone(),
                    placed_at: Some(std::time::SystemTime::now()),
                });
            }
        }

        if !to_place.is_empty() {
            let results = broker.place_orders(&to_place).await?;
            for ((side, level, price, size, cid), res) in
                place_intents.into_iter().zip(results.iter())
            {
                if !res.success {
                    any_failure = true;
                    tracing::warn!(
                        side = %side,
                        level,
                        price = %price,
                        error = res.error.as_deref().unwrap_or("unknown"),
                        "Failed to place A-S quote"
                    );
                    continue;
                }
                kept_slots.push(QuoteSlot {
                    side,
                    level,
                    price,
                    size,
                    client_id: Some(cid),
                    order_id: res.order_id.clone(),
                    placed_at: Some(std::time::SystemTime::now()),
                });
            }
        }

        // Bidirectional reconciliation against Bullet's actual open orders.
        // Triggered on any batch failure (immediate recovery from phantom
        // slot loops) or on a periodic timer (catches orphan orders that
        // accumulated quietly during success runs — e.g. from non-atomic
        // amend edge cases or place-tx-committed-but-response-errored).
        //
        // Both directions:
        //   - Phantom: slot in `kept_slots` whose `order_id` isn't in the
        //     live list → drop the slot. Next refresh re-places it.
        //   - Orphan: live order on Bullet not matching any tracked slot's
        //     `order_id` or `client_id` → cancel it.
        //
        // Slots whose `order_id` is None (place ack pending) are matched by
        // `client_id` instead, so we don't drop legitimate in-flight slots.
        let now = Instant::now();
        let due_for_periodic = self.config.reconcile_interval_secs > 0
            && match self.last_reconcile_at {
                None => true,
                Some(t) => {
                    now.duration_since(t)
                        >= Duration::from_secs(self.config.reconcile_interval_secs)
                }
            };
        // WS user-data is the authoritative source for slot state, so
        // failure-triggered REST queries are unnecessary at sub-second
        // cadence. The connection layer flips a one-shot reconcile signal
        // every WS reconnect (since events during the disconnect window are
        // lost), and the periodic sweep is the safety net for any dropped
        // frames that the reconnect signal didn't catch.
        let _ = any_failure;
        let on_reconnect = broker.take_reconcile_signal();
        if on_reconnect {
            tracing::info!("WS reconnect detected — forcing immediate reconciliation");
        }
        if due_for_periodic || on_reconnect {
            match broker.get_open_orders(self.symbol()).await {
                Ok(open) => {
                    use std::collections::HashSet;
                    let live_oids: HashSet<&str> =
                        open.iter().map(|o| o.id.as_str()).collect();
                    let live_cids: HashSet<&str> =
                        open.iter().filter_map(|o| o.client_id.as_deref()).collect();
                    let before = kept_slots.len();
                    kept_slots.retain(|s| match s.order_id.as_deref() {
                        Some(oid) => {
                            live_oids.contains(oid)
                                || s.client_id
                                    .as_deref()
                                    .is_some_and(|c| live_cids.contains(c))
                        }
                        None => true,
                    });
                    let dropped = before - kept_slots.len();

                    // Now find orders alive on Bullet that we don't track —
                    // match in either direction (oid OR cid) so a slot whose
                    // place ack hasn't landed (cid only) still claims its
                    // order.
                    let tracked_oids: HashSet<&str> = kept_slots
                        .iter()
                        .filter_map(|s| s.order_id.as_deref())
                        .collect();
                    let tracked_cids: HashSet<&str> = kept_slots
                        .iter()
                        .filter_map(|s| s.client_id.as_deref())
                        .collect();
                    let orphans: Vec<CancelOrder> = open
                        .iter()
                        .filter(|o| {
                            let oid_tracked = tracked_oids.contains(o.id.as_str());
                            let cid_tracked = o
                                .client_id
                                .as_deref()
                                .is_some_and(|c| tracked_cids.contains(c));
                            !oid_tracked && !cid_tracked
                        })
                        .map(|o| CancelOrder {
                            symbol: symbol.clone(),
                            order_id: o.id.clone(),
                            client_id: o.client_id.clone(),
                        })
                        .collect();
                    let orphan_count = orphans.len();
                    let orphan_summary: Vec<String> = open
                        .iter()
                        .filter(|o| {
                            let oid_tracked = tracked_oids.contains(o.id.as_str());
                            let cid_tracked = o
                                .client_id
                                .as_deref()
                                .is_some_and(|c| tracked_cids.contains(c));
                            !oid_tracked && !cid_tracked
                        })
                        .map(|o| {
                            format!(
                                "{}@{}/{}",
                                o.id,
                                o.client_id.as_deref().unwrap_or("-"),
                                o.side
                            )
                        })
                        .collect();
                    let mut orphans_actually_cancelled = 0usize;
                    if !orphans.is_empty() {
                        match broker.cancel_orders(&orphans).await {
                            Ok(results) => {
                                orphans_actually_cancelled =
                                    results.iter().filter(|r| r.success).count();
                                let failed = results.len() - orphans_actually_cancelled;
                                if failed > 0 {
                                    tracing::warn!(
                                        failed,
                                        total = results.len(),
                                        "orphan cancel batch reverted — orphans persist"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "orphan cancel batch errored");
                            }
                        }
                    }

                    if dropped > 0 || orphan_count > 0 {
                        tracing::info!(
                            phantom_dropped = dropped,
                            orphans_found = orphan_count,
                            orphans_cancelled = orphans_actually_cancelled,
                            kept = kept_slots.len(),
                            live = open.len(),
                            orphans = ?orphan_summary,
                            "reconciled with Bullet open orders"
                        );
                    }
                    self.last_reconcile_at = Some(now);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "reconciliation get_open_orders failed");
                }
            }
        }

        self.slots = kept_slots;
        self.last_quote_at = Some(Instant::now());

        tracing::info!(
            levels = rungs.len(),
            cancels = n_cancels,
            amends = n_amends,
            places = n_places,
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
        // Seed local book unconditionally; seed mid/vol only in single-venue
        // mode (in fair-value mode the reference feed owns those).
        if !self.uses_reference() {
            let mid = book.midpoint().ok_or_else(|| {
                BotError::strategy("No orderbook data available to compute mid price")
            })?;
            self.last_mid = Some(mid);
            if let Some(m) = mid.to_f64() {
                self.volatility.push(m, Instant::now());
            }
        }
        self.book = Some(book);

        tracing::info!(
            mode = if self.uses_reference() { "fair-value" } else { "single-venue" },
            reference = ?self.config.reference_exchange,
            gamma = %self.config.gamma,
            kappa = %self.config.kappa,
            tau = self.config.order_horizon_secs,
            "A-S actor started — awaiting price samples before first quote"
        );
        Ok(())
    }

    async fn wind_down(
        &mut self,
        _reason: &WindDownReason,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        // WindDownReason intentionally ignored: market-making never wants to
        // take taker fees at shutdown, so cancel-only is correct for every
        // reason including FeedFailed.
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
    async fn on_event(&mut self, event: BookUpdate, cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange != self.exchange() || event.symbol != self.symbol() {
            return Ok(());
        }
        // In single-venue mode the local book is the price source. In
        // fair-value mode the reference feed owns `last_mid` / volatility,
        // so we only retain the book itself for would-cross checks.
        if !self.uses_reference() {
            if let Some(m) = event.orderbook.midpoint() {
                self.last_mid = Some(m);
                if let Some(f) = m.to_f64() {
                    self.volatility.push(f, Instant::now());
                }
            }
        }
        self.book = Some(event.orderbook);
        // Refresh on every book update; the function self-throttles via
        // `amend_threshold_bps`, so calls where nothing has moved are free.
        if let Some(m) = self.last_mid {
            self.refresh_quotes(cx, m).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<ReferencePriceUpdate> for AvellanedaStoikovActor {
    async fn on_event(
        &mut self,
        event: ReferencePriceUpdate,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        let Some(ref_symbol) = self.config.reference_symbol.as_deref() else {
            return Ok(());
        };
        if !event.symbol.eq_ignore_ascii_case(ref_symbol) {
            return Ok(());
        }
        self.reference_last_seen = Some(event.received_at);
        let prev = self.last_mid;
        if prev != Some(event.mid) {
            tracing::debug!(
                prev = ?prev.map(|p| p.to_string()),
                new = %event.mid,
                "ref mid changed"
            );
        }
        self.last_mid = Some(event.mid);
        if let Some(f) = event.mid.to_f64() {
            self.volatility.push(f, Instant::now());
        }
        self.refresh_quotes(cx, event.mid).await?;
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
        let cid = event.order.client_id.as_deref();
        let oid = event.order.id.as_str();

        match event.order.status {
            // Terminal: order is gone from Bullet's book. Drop our tracking
            // so we don't try to amend it. The Filled case is also handled
            // by the Trade handler — both dropping is idempotent.
            OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Filled => {
                self.slots.retain(|s| {
                    let cid_match = cid.is_some() && s.client_id.as_deref() == cid;
                    let oid_match = !oid.is_empty() && s.order_id.as_deref() == Some(oid);
                    !(cid_match || oid_match)
                });
            }
            // Place ack: learn the exchange-assigned order_id.
            OrderStatus::Open | OrderStatus::PartiallyFilled => {
                if let Some(cid) = cid {
                    if let Some(slot) =
                        self.slots.iter_mut().find(|s| s.client_id.as_deref() == Some(cid))
                    {
                        if slot.order_id.is_none() && !oid.is_empty() {
                            slot.order_id = Some(oid.to_string());
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Tick> for AvellanedaStoikovActor {
    async fn on_event(&mut self, _event: Tick, cx: &ActorContext) -> Result<(), BotError> {
        // If the broker has flagged a permanent WS disconnect, the strategy
        // is running blind. Request a clean harness shutdown so wind_down
        // can cancel outstanding orders before the process exits.
        if let Ok(broker) = cx.broker(self.exchange()) {
            if broker.is_disconnected() {
                tracing::error!(
                    "broker reports permanent disconnect — requesting harness shutdown"
                );
                cx.request_shutdown();
                return Ok(());
            }
        }
        let Some(mid) = self.last_mid else {
            return Ok(());
        };
        let now = Instant::now();
        if self.should_force_refresh(now) {
            self.refresh_quotes(cx, mid).await?;
        }

        let best_bid = self.slots.iter().filter(|s| s.side == Side::Buy).map(|s| s.price).max();
        let best_ask = self.slots.iter().filter(|s| s.side == Side::Sell).map(|s| s.price).min();
        tracing::info!(
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            slots = self.slots.len(),
            mid = ?self.last_mid.map(|p| p.to_string()),
            best_bid = ?best_bid.map(|p| p.to_string()),
            best_ask = ?best_ask.map(|p| p.to_string()),
            vol_samples = self.volatility.sample_count(),
            "A-S tick"
        );
        Ok(())
    }
}
