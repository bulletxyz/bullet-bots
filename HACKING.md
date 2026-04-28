# Writing a strategy

This guide walks through adding a new strategy in the harness model using a
toy example: a "buy-the-dip" bot that places a bid when the book drops more
than `X%` below a rolling reference price, and closes when it bounces back.

You'll end up with one Rust file of business logic that reads like pseudocode,
because the framework handles event dispatch, lifecycle, reconnection,
shutdown, and shared bookkeeping (position, PnL, client IDs).

## 1. Create the crate

```sh
mkdir -p crates/strategies/dip-buyer/src
```

`crates/strategies/dip-buyer/Cargo.toml`:

```toml
[package]
name = "bb-strategy-dip-buyer"
version.workspace = true
edition.workspace = true

[dependencies]
bb-core = { workspace = true }
async-trait = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
rust_decimal = { workspace = true }
tracing = { workspace = true }
```

Register it in the workspace root `Cargo.toml` and in `bb-bot/Cargo.toml`.

## 2. Config

`crates/strategies/dip-buyer/src/config.rs`:

```rust
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DipBuyerConfig {
    /// Broker name — matches the one passed to `HarnessBuilder::wire_broker`.
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub symbol: String,
    /// Bid this far (in %) below the rolling reference price.
    pub dip_pct: Decimal,
    /// Close when mark price recovers to this %-above our entry.
    pub exit_pct: Decimal,
    pub order_size: Decimal,
    pub max_position: Decimal,
}

fn default_exchange() -> String { "bullet".to_string() }
```

## 3. The actor

`crates/strategies/dip-buyer/src/lib.rs`:

```rust
pub mod config;

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, Trade, Tick};
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::{ClientIdIssuer, InventoryTracker};
use bb_core::types::{NewOrder, OrderType, Side};
use rust_decimal::Decimal;

use config::DipBuyerConfig;

pub struct DipBuyerActor {
    config: DipBuyerConfig,
    inventory: InventoryTracker,
    client_ids: ClientIdIssuer,
    reference_price: Option<Decimal>,
    last_mid: Option<Decimal>,
}

impl DipBuyerActor {
    pub fn new(config: DipBuyerConfig) -> Self {
        Self {
            config,
            inventory: InventoryTracker::new(),
            client_ids: ClientIdIssuer::new(),
            reference_price: None,
            last_mid: None,
        }
    }

    fn exchange(&self) -> &str { &self.config.exchange }
    fn symbol(&self) -> &str { &self.config.symbol }

    async fn try_enter(&mut self, cx: &ActorContext, mid: Decimal) -> Result<(), BotError> {
        let Some(reference) = self.reference_price else { return Ok(()) };
        // Only buy if not already long.
        if !self.inventory.is_flat() { return Ok(()) }
        let drop_pct = (reference - mid) / reference * Decimal::from(100);
        if drop_pct < self.config.dip_pct { return Ok(()) }

        let broker = cx.broker(self.exchange())?;
        let cid = self.client_ids.issue();
        broker.place_orders(&[NewOrder {
            symbol: self.symbol().to_string(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: mid,
            quantity: self.config.order_size,
            client_id: Some(cid),
            reduce_only: false,
        }]).await?;
        tracing::info!(%drop_pct, %mid, "dip-buyer: submitted buy");
        Ok(())
    }

    async fn try_exit(&mut self, cx: &ActorContext, mid: Decimal) -> Result<(), BotError> {
        if self.inventory.is_flat() { return Ok(()) }
        let entry = self.inventory.avg_entry_price;
        if entry.is_zero() { return Ok(()) }
        let bounce_pct = (mid - entry) / entry * Decimal::from(100);
        if bounce_pct < self.config.exit_pct { return Ok(()) }

        let broker = cx.broker(self.exchange())?;
        let cid = self.client_ids.issue();
        broker.place_orders(&[NewOrder {
            symbol: self.symbol().to_string(),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: mid,
            quantity: self.inventory.net_position,
            client_id: Some(cid),
            reduce_only: true,
        }]).await?;
        tracing::info!(%bounce_pct, %mid, "dip-buyer: submitted close");
        Ok(())
    }
}

#[async_trait]
impl Actor for DipBuyerActor {
    async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        let broker = cx.broker(self.exchange())?;
        broker.cancel_all_orders(self.symbol()).await?;
        Ok(())
    }

    async fn wind_down(&mut self, _r: &WindDownReason, cx: &ActorContext) -> Result<(), BotError> {
        let broker = cx.broker(self.exchange())?;
        let _ = broker.cancel_all_orders(self.symbol()).await;
        tracing::info!(
            net_pos = %self.inventory.net_position,
            pnl = %self.inventory.realized_pnl,
            fills = self.inventory.total_fills,
            "dip-buyer: final stats"
        );
        Ok(())
    }

    fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "net_position": self.inventory.net_position.to_string(),
            "realized_pnl": self.inventory.realized_pnl.to_string(),
            "reference_price": self.reference_price.map(|d| d.to_string()),
        })
    }
}

#[async_trait]
impl EventHandler<BookUpdate> for DipBuyerActor {
    async fn on_event(&mut self, event: BookUpdate, _cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange == self.exchange() && event.symbol == self.symbol() {
            self.last_mid = event.orderbook.midpoint();
            // EMA-lite: if no reference, seed from the first mid.
            if self.reference_price.is_none() {
                self.reference_price = self.last_mid;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Trade> for DipBuyerActor {
    async fn on_event(&mut self, event: Trade, _cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange == self.exchange() && event.symbol == self.symbol() {
            // Canonical: Trade is the only source of position changes.
            self.inventory.record_fill(event.side, event.price, event.quantity);
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Tick> for DipBuyerActor {
    async fn on_event(&mut self, _t: Tick, cx: &ActorContext) -> Result<(), BotError> {
        let Some(mid) = self.last_mid else { return Ok(()) };
        // Slowly drift the reference price toward the current mid.
        if let Some(r) = self.reference_price.as_mut() {
            *r = (*r * Decimal::from(95) + mid * Decimal::from(5)) / Decimal::from(100);
        }
        self.try_exit(cx, mid).await?;
        self.try_enter(cx, mid).await?;
        Ok(())
    }
}
```

## 4. Wire it into `bb-bot`

In `bb-bot/src/main.rs`, add:

```rust
use bb_strategy_dip_buyer::{DipBuyerActor, config::DipBuyerConfig};
```

In the run-harness dispatch, add a match arm that parses the sub-config and
builds the actor:

```rust
async fn run_harness_dip_buyer(
    engine: EngineConfig,
    exchanges: HashMap<String, ExchangeEntry>,
    strategy: StrategyEntry,
) -> Result<(), Box<dyn std::error::Error>> {
    let bullet_cfg = bullet_config(&exchanges)?;
    let sub = strategy.config.get("dip-buyer").cloned().unwrap_or(strategy.config.clone());
    let dip_cfg: DipBuyerConfig = sub.try_into()
        .map_err(|e: toml::de::Error| format!("Invalid dip-buyer config: {e}"))?;

    let (broker, feeds) = connect_bullet(&bullet_cfg, &dip_cfg.symbol).await?;
    let broker: Arc<dyn bb_core::broker::Broker> = Arc::new(broker);
    let actor = DipBuyerActor::new(dip_cfg);
    let tick = TickFeed::every_ms(engine.tick_interval_ms);

    let harness = HarnessBuilder::new()
        .enable_signal_shutdown()
        .wire_broker("bullet", broker)
        .wire_feed_named("bullet-trades", feeds.trade)
        .wire_feed_named("bullet-book",   feeds.book)
        .wire_feed_named("ticks",         tick)
        .wire_actor(
            ActorSpec::new("dip-buyer", actor)
                .sub::<bb_core::events::BookUpdate>()
                .sub::<bb_core::events::Trade>()
                .sub::<bb_core::events::Tick>(),
        )
        .build()?;
    harness.run().await?;
    Ok(())
}
```

## 5. Config file

`config/dip-buyer-example.toml`:

```toml
[engine]
tick_interval_ms = 2000
status_port = 3030

[exchanges.bullet]
type = "bullet"
network = "testnet"
private_key_hex = ""

[strategy]
type = "dip-buyer"

[strategy.dip-buyer]
symbol = "BTC-USD"
# exchange defaults to "bullet" — set if you wire multiple brokers.
dip_pct  = "1.5"
exit_pct = "0.8"
order_size = "0.001"
max_position = "0.01"
```

Run:

```sh
export BB_BULLET_PRIVATE_KEY_HEX="0x..."
cargo run --bin bb-bot -- run --config config/dip-buyer-example.toml
```

## What you did *not* have to write

All the framework plumbing:

- WebSocket reconnection — inherited from the Bullet adapter's
  `ManagedWebsocket`.
- Event multiplexing — each feed publishes independently; the harness routes
  them into your handlers by type.
- Position / PnL tracking — `InventoryTracker`.
- Client order ID issuance — `ClientIdIssuer`.
- Periodic heartbeat — `TickFeed`.
- Graceful shutdown on Ctrl-C — `.enable_signal_shutdown()`.
- Per-actor serialized handler execution — harness-provided mutex.
- Feed-failure / actor-failure routing — `WindDownReason` covers the cases.

If you had written this the old way you would have needed ~300 lines of event
loop, reconnection, and bookkeeping around the ~80 lines of actual strategy
logic.

## Testing

Pure business logic (e.g., the reference-price drift computation) should live
in small helper functions so you can unit-test them directly:

```rust
#[test]
fn drift_moves_toward_mid() {
    let drifted = drift(Decimal::from(100), Decimal::from(110));
    assert!(drifted > Decimal::from(100) && drifted < Decimal::from(110));
}
```

For end-to-end tests of the actor itself, wire it into a `HarnessBuilder` with
`ScriptedFeed` and `MockBroker` from `bb-core::harness::testing`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn entry_fires_on_dip() {
    use bb_core::harness::{ActorSpec, HarnessBuilder, MockBroker, ScriptedFeed};
    use bb_core::events::{BookUpdate, Trade};

    let broker = MockBroker::shared("bullet");
    let actor = DipBuyerActor::new(/* config with dip_pct = 1.5 */);

    // Build a scripted book feed: price drops enough to trigger entry.
    let books = ScriptedFeed::new(vec![
        book_at(100_00), // seed reference price
        book_at(98_400), // -1.6% → triggers entry
        book_at(98_400), // padding so harness drains
    ]);

    let harness = HarnessBuilder::new()
        .wire_broker("bullet", Arc::clone(&broker) as Arc<dyn bb_core::broker::Broker>)
        .wire_feed_named("books", books)
        .wire_actor(ActorSpec::new("dip", actor).sub::<BookUpdate>().sub::<Trade>())
        .build()
        .unwrap();

    harness.run().await.unwrap();

    assert_eq!(broker.placed_count().await, 1);
    let orders = broker.last_placed_orders().await;
    assert_eq!(orders[0].side, Side::Buy);
}
```

`ScriptedFeed` yields between each event so handler tasks are scheduled before
the next event arrives — without this, tests that assert on intermediate state
would be structurally flaky. See the reference-arb and funding-arb strategy
tests for more complex multi-feed / multi-state-transition examples.

## Canonical-source rule (important)

A strategy's **position** and **realized PnL** are updated only from `Trade`
events. Never from `OrderLifecycle::Filled`. The reason: adapters may emit
both (e.g., HL's `UserFills` and `OrderUpdates` channels describe the same
execution from different angles). Crediting both would double-count.

`OrderLifecycle` is for reconcile only — "is my order still resting?", "did
my cancel go through?", "what's the exchange-assigned `order_id` for my
`client_id`?"

The framework can't enforce this at compile time (both are just event types),
but following the rule keeps your strategy correct against every adapter.
