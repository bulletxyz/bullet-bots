use std::time::{Duration, Instant};

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, OrderLifecycle, Tick, Trade};
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::{ClientIdIssuer, InventoryTracker};
use bb_core::types::{CancelOrder, NewOrder, OrderBook, OrderStatus, Side};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::config::SimpleMmConfig;

const BPS_SCALE: i64 = 10_000;

#[derive(Debug, Clone, Serialize)]
struct QuoteSlot {
    side: Side,
    price: Decimal,
    client_id: String,
    order_id: Option<String>,
}

pub struct SimpleMmActor {
    config: SimpleMmConfig,
    inventory: InventoryTracker,
    client_ids: ClientIdIssuer,
    book: Option<OrderBook>,
    bid: Option<QuoteSlot>,
    ask: Option<QuoteSlot>,
    last_refresh_at: Option<Instant>,
}

impl SimpleMmActor {
    pub fn new(config: SimpleMmConfig) -> Self {
        Self {
            config,
            inventory: InventoryTracker::new(),
            client_ids: ClientIdIssuer::session_seeded(),
            book: None,
            bid: None,
            ask: None,
            last_refresh_at: None,
        }
    }

    fn exchange(&self) -> &str {
        &self.config.exchange
    }

    fn symbol(&self) -> &str {
        &self.config.symbol
    }

    fn mid(&self) -> Option<Decimal> {
        self.book.as_ref().and_then(OrderBook::midpoint)
    }

    fn desired_price(&self, side: Side, mid: Decimal) -> Decimal {
        let bps = match side {
            Side::Buy => self.config.bid_spread_bps,
            Side::Sell => self.config.ask_spread_bps,
        };
        let spread = bps / Decimal::from(BPS_SCALE);
        match side {
            Side::Buy => mid * (Decimal::ONE - spread),
            Side::Sell => mid * (Decimal::ONE + spread),
        }
    }

    fn slot(&self, side: Side) -> Option<&QuoteSlot> {
        match side {
            Side::Buy => self.bid.as_ref(),
            Side::Sell => self.ask.as_ref(),
        }
    }

    fn set_slot(&mut self, side: Side, slot: Option<QuoteSlot>) {
        match side {
            Side::Buy => self.bid = slot,
            Side::Sell => self.ask = slot,
        }
    }

    fn can_quote(&self, side: Side) -> bool {
        match side {
            Side::Buy => {
                self.inventory.net_position + self.config.order_size <= self.config.max_position
            }
            Side::Sell => {
                self.inventory.net_position - self.config.order_size >= -self.config.max_position
            }
        }
    }

    fn needs_refresh(&self, side: Side, desired: Decimal) -> bool {
        let Some(slot) = self.slot(side) else {
            return true;
        };
        if desired.is_zero() {
            return false;
        }
        let drift_bps = (slot.price - desired).abs() / desired * Decimal::from(BPS_SCALE);
        drift_bps >= self.config.refresh_threshold_bps
    }

    fn cancel_for(slot: &QuoteSlot, symbol: &str) -> CancelOrder {
        CancelOrder {
            symbol: symbol.to_string(),
            order_id: slot.order_id.clone().unwrap_or_default(),
            client_id: Some(slot.client_id.clone()),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn refresh_quotes(&mut self, cx: &ActorContext, force: bool) -> Result<(), BotError> {
        let Some(mid) = self.mid() else {
            return Ok(());
        };
        let due = force
            || self
                .last_refresh_at
                .is_none_or(|last| last.elapsed() >= Duration::from_secs(self.config.refresh_secs));
        if !due {
            return Ok(());
        }

        let symbol = self.symbol().to_string();
        let book = self.book.as_ref();
        let mut cancels = Vec::new();
        let mut cancel_sides = Vec::new(); // parallel to `cancels` — which side each cancel belongs to
        let mut placements = Vec::new();
        let mut new_slots = Vec::new();

        for side in [Side::Buy, Side::Sell] {
            let desired = self.desired_price(side, mid);
            let blocked_by_inventory = !self.can_quote(side);
            let would_cross = self.config.order_type == bb_core::types::OrderType::PostOnly
                && book.is_some_and(|b| b.would_cross(side, desired));
            let should_have_quote = !blocked_by_inventory && !would_cross;
            let needs_refresh = should_have_quote && self.needs_refresh(side, desired);

            if let Some(slot) = self.slot(side)
                && (!should_have_quote || needs_refresh)
            {
                cancels.push(Self::cancel_for(slot, &symbol));
                cancel_sides.push(side);
            }

            if should_have_quote && needs_refresh {
                let client_id = self.client_ids.issue();
                placements.push(NewOrder {
                    symbol: symbol.clone(),
                    side,
                    order_type: self.config.order_type,
                    price: desired,
                    quantity: self.config.order_size,
                    client_id: Some(client_id.clone()),
                    reduce_only: false,
                });
                new_slots.push(QuoteSlot { side, price: desired, client_id, order_id: None });
            } else if blocked_by_inventory {
                tracing::debug!(
                    side = %side,
                    net_position = %self.inventory.net_position,
                    max_position = %self.config.max_position,
                    "simple-mm: side blocked by inventory cap"
                );
            } else if would_cross {
                tracing::debug!(side = %side, price = %desired, "simple-mm: quote would cross");
            }
        }

        if self.config.dry_run {
            for cancel in &cancels {
                if self
                    .bid
                    .as_ref()
                    .is_some_and(|s| s.client_id == cancel.client_id.clone().unwrap_or_default())
                {
                    self.bid = None;
                }
                if self
                    .ask
                    .as_ref()
                    .is_some_and(|s| s.client_id == cancel.client_id.clone().unwrap_or_default())
                {
                    self.ask = None;
                }
            }
            for slot in new_slots {
                self.set_slot(slot.side, Some(slot));
            }
            self.last_refresh_at = Some(Instant::now());
            tracing::info!(
                mid = %mid,
                dry_run = true,
                bid = ?self.bid.as_ref().map(|s| s.price.to_string()),
                ask = ?self.ask.as_ref().map(|s| s.price.to_string()),
                "simple-mm: paper refresh"
            );
            return Ok(());
        }

        let broker = cx.broker(self.exchange())?;
        let mut failed_cancel_sides = std::collections::HashSet::new();
        if !cancels.is_empty() {
            let results = broker.cancel_orders(&cancels).await?;
            for (result, &side) in results.iter().zip(cancel_sides.iter()) {
                if result.success {
                    self.set_slot(side, None);
                } else {
                    tracing::warn!(
                        ?side,
                        error = result.error.as_deref().unwrap_or("unknown"),
                        "simple-mm: cancel rejected — skipping replacement"
                    );
                    failed_cancel_sides.insert(side);
                }
            }
        }

        // Drop placements for any side whose cancel failed — placing without
        // cancelling would orphan the old order.
        let (placements, new_slots): (Vec<_>, Vec<_>) = placements
            .into_iter()
            .zip(new_slots)
            .filter(|(_, slot)| !failed_cancel_sides.contains(&slot.side))
            .unzip();

        if !placements.is_empty() {
            let results = broker.place_orders(&placements).await?;
            for (slot, result) in new_slots.into_iter().zip(results.iter()) {
                if result.success {
                    let mut slot = slot;
                    slot.order_id.clone_from(&result.order_id);
                    self.set_slot(slot.side, Some(slot));
                } else {
                    tracing::warn!(
                        side = %slot.side,
                        price = %slot.price,
                        error = result.error.as_deref().unwrap_or("unknown"),
                        "simple-mm: place rejected"
                    );
                }
            }
        }

        self.last_refresh_at = Some(Instant::now());
        tracing::info!(
            mid = %mid,
            bid = ?self.bid.as_ref().map(|s| s.price.to_string()),
            ask = ?self.ask.as_ref().map(|s| s.price.to_string()),
            net_position = %self.inventory.net_position,
            "simple-mm: quotes refreshed"
        );
        Ok(())
    }
}

#[async_trait]
impl Actor for SimpleMmActor {
    async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        self.config
            .validate()
            .map_err(|e| BotError::config(format!("simple-mm config invalid: {e}")))?;
        if !self.config.dry_run {
            let broker = cx.broker(self.exchange())?;
            broker.cancel_all_orders(self.symbol()).await?;
            let positions = broker.get_positions().await?;
            if let Some(position) = positions.iter().find(|p| p.symbol == self.symbol()) {
                self.inventory.seed_from_position(position);
                tracing::warn!(
                    net_pos = %self.inventory.net_position,
                    entry = %self.inventory.avg_entry_price,
                    "simple-mm: seeded inventory from existing position"
                );
            }
            self.book = Some(broker.get_orderbook(self.symbol(), 20).await?);
        }
        tracing::info!(
            symbol = %self.config.symbol,
            bid_spread_bps = %self.config.bid_spread_bps,
            ask_spread_bps = %self.config.ask_spread_bps,
            dry_run = self.config.dry_run,
            "simple-mm started"
        );
        Ok(())
    }

    async fn wind_down(
        &mut self,
        _reason: &WindDownReason,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        if !self.config.dry_run {
            let broker = cx.broker(self.exchange())?;
            let _ = broker.cancel_all_orders(self.symbol()).await;
        }
        tracing::info!(
            net_pos = %self.inventory.net_position,
            realized_pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            "simple-mm final stats"
        );
        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "symbol": self.config.symbol,
            "dry_run": self.config.dry_run,
            "net_position": self.inventory.net_position.to_string(),
            "realized_pnl": self.inventory.realized_pnl.to_string(),
            "total_fills": self.inventory.total_fills,
            "mid": self.mid().map(|d| d.to_string()),
            "bid": self.bid,
            "ask": self.ask,
        })
    }
}

#[async_trait]
impl EventHandler<BookUpdate> for SimpleMmActor {
    async fn on_event(&mut self, event: BookUpdate, cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange != self.exchange() || event.symbol != self.symbol() {
            return Ok(());
        }
        self.book = Some(event.orderbook);
        self.refresh_quotes(cx, false).await
    }
}

#[async_trait]
impl EventHandler<Trade> for SimpleMmActor {
    async fn on_event(&mut self, event: Trade, _cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange != self.exchange() || event.symbol != self.symbol() {
            return Ok(());
        }
        self.inventory.record_fill(event.side, event.price, event.quantity);
        // Do NOT clear the slot here — a Trade may be a partial fill and the
        // order remains resting. Slot cleanup is handled by OrderLifecycle
        // (Filled / Cancelled / Rejected), which only fires on terminal states.
        tracing::info!(
            side = %event.side,
            price = %event.price,
            qty = %event.quantity,
            net_position = %self.inventory.net_position,
            "simple-mm fill"
        );
        Ok(())
    }
}

#[async_trait]
impl EventHandler<OrderLifecycle> for SimpleMmActor {
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
        for slot in [&mut self.bid, &mut self.ask].into_iter().flatten() {
            if cid.is_some_and(|c| slot.client_id == c)
                && slot.order_id.is_none()
                && !oid.is_empty()
            {
                slot.order_id = Some(oid.to_string());
            }
        }
        if matches!(
            event.order.status,
            OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Filled
        ) {
            if self.bid.as_ref().is_some_and(|s| {
                cid.is_some_and(|c| s.client_id == c)
                    || (!oid.is_empty() && s.order_id.as_deref() == Some(oid))
            }) {
                self.bid = None;
            }
            if self.ask.as_ref().is_some_and(|s| {
                cid.is_some_and(|c| s.client_id == c)
                    || (!oid.is_empty() && s.order_id.as_deref() == Some(oid))
            }) {
                self.ask = None;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Tick> for SimpleMmActor {
    async fn on_event(&mut self, _event: Tick, cx: &ActorContext) -> Result<(), BotError> {
        if let Ok(broker) = cx.broker(self.exchange())
            && broker.is_disconnected()
        {
            tracing::error!("simple-mm broker disconnected — requesting shutdown");
            cx.request_shutdown();
            return Ok(());
        }
        self.refresh_quotes(cx, false).await?;
        tracing::info!(
            net_position = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            bid = ?self.bid.as_ref().map(|s| s.price.to_string()),
            ask = ?self.ask.as_ref().map(|s| s.price.to_string()),
            "simple-mm tick"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use bb_core::broker::Broker;
    use bb_core::events::BookUpdate;
    use bb_core::harness::testing::{MockBroker, ScriptedFeed};
    use bb_core::harness::{ActorSpec, HarnessBuilder};

    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn config() -> SimpleMmConfig {
        SimpleMmConfig {
            exchange: "bullet".into(),
            symbol: "BTC-USD".into(),
            bid_spread_bps: d("10"),
            ask_spread_bps: d("10"),
            order_size: d("0.001"),
            max_position: d("0.005"),
            refresh_secs: 5,
            refresh_threshold_bps: Decimal::ZERO,
            order_type: bb_core::types::OrderType::PostOnly,
            dry_run: true,
            fees: None,
        }
    }

    fn book_update(bid: &str, ask: &str) -> BookUpdate {
        let mut bids = BTreeMap::new();
        bids.insert(d(bid), d("1"));
        let mut asks = BTreeMap::new();
        asks.insert(d(ask), d("1"));
        BookUpdate {
            exchange: "bullet".into(),
            symbol: "BTC-USD".into(),
            orderbook: OrderBook { bids, asks, last_update_id: 0 },
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dry_run_does_not_call_broker() {
        let broker = MockBroker::shared("bullet");
        let actor = SimpleMmActor::new(config());
        let feed = ScriptedFeed::new(vec![book_update("99", "101")]);

        let harness = HarnessBuilder::new()
            .wire_broker("bullet", broker.clone() as Arc<dyn Broker>)
            .wire_feed_named("book", feed)
            .wire_actor(ActorSpec::new("simple-mm", actor).sub::<BookUpdate>())
            .build()
            .unwrap();

        harness.run().await.unwrap();
        assert_eq!(broker.placed_count().await, 0);
    }
}
