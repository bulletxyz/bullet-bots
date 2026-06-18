# Adding a new exchange adapter

This guide walks through adding a new venue (e.g. Drift, dYdX) to
bullet-bots. The Bullet and Hyperliquid adapters are the reference
implementations; follow their layout exactly so contributors can navigate
any adapter without context-switching.

## Directory layout

```
crates/exchanges/<name>/
├── Cargo.toml
└── src/
    ├── lib.rs          re-exports
    ├── config.rs       TOML config struct
    ├── connection.rs   WS muxer → typed mpsc channels
    ├── broker.rs       REST → bb_core::broker::Broker impl
    └── convert.rs      all venue-type ↔ bb_core-type conversions
```

`connection.rs` and `broker.rs` are peers — they share connection health
flags (`Arc<ConnectionHealth>`) but otherwise don't call each other.

## Required public surface

```rust
// lib.rs
pub use config::MyConfig;
pub use connection::{MyFeeds, connect as connect_my_venue};
pub use broker::MyBroker;
```

`connect_my_venue(config: &MyConfig, symbol: &str) -> Result<(MyBroker, MyFeeds), BotError>`
is the single entry point. It sets up the WS, subscribes, spawns the muxer
task, and returns both handles.

`MyFeeds` holds typed feed handles — one per event type:

```rust
pub struct MyFeeds {
    pub trade:      MpscFeed<Trade>,
    pub book:       MpscFeed<BookUpdate>,
    pub lifecycle:  MpscFeed<OrderLifecycle>,
    pub mark_price: MpscFeed<MarkPriceUpdate>,
}
```

`MpscFeed<E>` (from `bb_core::harness`) is the generic feed backed by an
`mpsc::UnboundedReceiver<E>`. The muxer task writes to the sender side;
`connect_my_venue` wraps each receiver with `MpscFeed::new(rx)` and puts
it in `MyFeeds`.

## The canonical-source invariant (critical)

`Trade` is the **only** source of position/PnL changes. `OrderLifecycle`
is for reconcile and client_id → oid mapping only.

If your venue emits the same fill through two streams (common: both a
trade-stream and an order-update stream), emit it as `Trade` **once** from
the authoritative stream, and as `OrderLifecycle` **once** for the status
transition. Never emit `Trade` from the order-update stream AND the
trade-fill stream — the strategy will double-count inventory.

See the module doc in `crates/bb-core/src/harness/mod.rs` for the
full explanation.

## Symbol mapping

Use a pair of helpers in `convert.rs`:

```rust
pub fn to_bb_symbol(venue_coin: &str) -> String { format!("{venue_coin}-USD") }
pub fn to_venue_symbol(bb_symbol: &str) -> String {
    bb_symbol.strip_suffix("-USD").unwrap_or(bb_symbol).to_string()
}
```

Adjust for venues that use a different suffix convention.

## Auth / key material

- Never put private keys in configs. Read from env vars or a keystore file.
- Wrap raw key strings in `secrecy::SecretString` in your config struct.
- Use `secrecy::ExposeSecret::expose_secret(&config.private_key_hex)` only
  at the point of actual key use, not earlier.

Bullet API docs are machine-readable: see
<https://tradingapi.bullet.xyz/llms.txt> for an index, the raw OpenAPI spec at
<https://tradingapi.bullet.xyz/docs/rest/openapi.json>, and the
[delegate accounts](https://tradingapi.bullet.xyz/docs/delegate-accounts.md)
guide (delegate keys 404 on account reads — resolve the master via
`/api/v1/delegateOf` first).

## Reconnect patterns

Two supported patterns — pick the one your SDK offers:

**SDK-managed reconnect** (Bullet pattern): the SDK's `ManagedWebsocket`
reconnects internally. Your muxer loop receives `WsEvent::Reconnecting`
before re-subscribing. On reconnect, flip `ConnectionHealth::reconcile_pending`
so strategies re-query open orders.

**Gap-timeout reconnect** (Hyperliquid pattern): the SDK reconnects
transparently, giving you no explicit reconnect event. Infer reconnects from
message-stream gaps — wrap your `recv()` in `tokio::time::timeout(THRESHOLD)`.
On timeout, flip `reconcile_pending` and continue. Set `THRESHOLD` to a value
safely below your fastest publish interval (10s for HL, which publishes AllMids
every ~250ms).

Both patterns use `Arc<ConnectionHealth>` to share state with the broker:

```rust
#[derive(Default)]
pub(crate) struct ConnectionHealth {
    pub reconcile_pending: AtomicBool,
    pub disconnected:      AtomicBool,
}
```

## `place_orders` return convention

`OrderResult.order_id` is `Option<String>`:
- `Some(id)` — venue confirmed the order synchronously (HL does this for
  `Resting` / `Filled` statuses).
- `None` — outcome unknown; strategy should listen on the `OrderLifecycle`
  stream for confirmation (Bullet blockchain txns are not confirmed
  synchronously).

Document which case applies in your adapter's module doc.

## `OrderResult` error semantics

- `Err(BotError)` — transport/system failure; the whole batch failed before
  reaching the venue.
- `Ok(results)` with `result.success = false` — venue-level rejection of a
  specific order; other orders in the batch may have succeeded.

## convert.rs is the boundary

All venue-type → bb_core-type conversions live in `convert.rs`. `connection.rs`
calls `convert::*_to_event(...)` — it never constructs `bb_core::events::*`
directly. This makes the conversion layer independently testable.

## Wiring into bb-bot

In `crates/bb-bot/src/main.rs`, add a match arm to the exchange dispatch:

```rust
"my_venue" => {
    let config = MyConfig::from_env_and_toml(&cfg)?;
    let (broker, feeds) = connect_my_venue(&config, &symbol).await?;
    builder = builder
        .wire_broker("my_venue", Arc::new(broker))
        .wire_feed_named("my_venue-trades",    feeds.trade)
        .wire_feed_named("my_venue-book",      feeds.book)
        .wire_feed_named("my_venue-lifecycle", feeds.lifecycle)
        .wire_feed_named("my_venue-mark",      feeds.mark_price);
}
```

Then add a config struct variant under `[exchanges.my_venue]` in your
example TOML.
