# bullet-bots

Event-driven trading bot framework in Rust. Typed event bus, typed feeds per
event kind, strategies implemented as actors. Pluggable exchange adapters and
strategies connect through a shared harness that handles lifecycle, shutdown,
and multi-source event dispatch.

## Build & Test

```sh
cargo build                # full workspace
cargo nextest run          # unit + integration tests (default runner)
cargo test --doc           # doctests only (nextest doesn't run these)
cargo clippy               # lints (pedantic enabled)
cargo +nightly fmt         # format (nightly required for import grouping)
```

First-time setup:

```sh
cargo install cargo-nextest --locked
```

The nextest config lives at `.config/nextest.toml`. CI should use the `ci`
profile (`cargo nextest run --profile ci`) which retries once and doesn't
fail-fast.

Validate a config without connecting:

```sh
cargo run --bin bb-bot -- validate --config config/simple-mm-example.toml
```

Generate a keypair (first time):

```sh
cargo run --bin bb-bot -- keygen --network testnet
# → writes ~/.config/bullet/id.json (0600), prints address + faucet curl
```

Fund and onboard the account (first time). The faucet credits the on-chain
wallet (**testnet only** — on mainnet you fund the wallet with real
bridged/deposited assets instead); `deposit` then moves funds into the perp
margin account and initializes the trading account. Skipping the deposit makes
order placement fail with `user_variants not found`:

```sh
# Run the faucet curl printed by keygen, then:
cargo run --bin bb-bot -- deposit --network testnet --asset USDC --amount 5000
```

Run a bot (default: reads `~/.config/bullet/id.json`):

```sh
cargo run --bin bb-bot -- run --config config/simple-mm-example.toml
```

Or point at an explicit keystore / use hex for CI:

```sh
export BB_BULLET_KEY_FILE="/path/to/keystore.json"   # preferred
# OR
export BB_BULLET_PRIVATE_KEY_HEX="0x..."             # fallback
```

## Architecture — the harness, feeds, and actors

The framework is organized around three typed primitives:

### Events

An **event** is any `Clone + Debug + Send + 'static` value. One Rust type per
kind of world change. The five canonical events live in
[`bb-core/src/events.rs`](crates/bb-core/src/events.rs):

| Event              | Semantics                                                                |
|--------------------|--------------------------------------------------------------------------|
| `Trade`            | An execution against our account. One per fill. **Canonical** source of position/PnL changes. |
| `OrderLifecycle`   | An order's state transition (Open → PartiallyFilled → Filled / Cancelled / Rejected). Used for reconcile — never for position updates. |
| `BookUpdate`       | Orderbook snapshot/update.                                               |
| `MarkPriceUpdate`  | Mark price and/or funding rate.                                          |
| `Tick`             | Periodic heartbeat, produced by the framework-provided `TickFeed`.       |

Splitting fills (`Trade`) from lifecycle (`OrderLifecycle`) is the key
invariant: strategies only update inventory from `Trade`, so the double-count
failure mode where a fill is credited twice (once as trade, once as filled
order) is structurally impossible.

### Feeds

A **feed** publishes events of a single type. Feeds own their upstream —
typically a WebSocket — and handle their own reconnection. Feeds implement
[`EventFeed<E>`](crates/bb-core/src/harness/feed.rs):

```rust
#[async_trait]
pub trait EventFeed<E: Event>: Send + 'static {
    async fn run(self: Box<Self>, tx: EventTx<E>, cx: FeedContext) -> Result<(), BotError>;
}
```

Exchanges expose one feed per event type they can produce. The Bullet adapter
(see [`exchanges/bullet/src/connection.rs`](crates/exchanges/bullet/src/connection.rs))
owns a single `ManagedWebsocket` and demultiplexes its messages into four
feeds: `BulletTradeFeed`, `BulletBookFeed`, `BulletOrderLifecycleFeed`,
`BulletMarkPriceFeed`.

`TickFeed` ([`bb-core/src/helpers/tick_feed.rs`](crates/bb-core/src/helpers/tick_feed.rs))
is a framework-provided feed that emits `Tick` events on a fixed interval, so
periodic strategy work (rebalance checks, logging) flows through the same
model as everything else.

### Actors

An **actor** is a stateful event consumer. Every strategy is one actor. An
actor implements [`Actor`](crates/bb-core/src/harness/actor.rs) for lifecycle
(`init` / `wind_down` / `status`) plus [`EventHandler<E>`](crates/bb-core/src/harness/actor.rs)
once per event type it cares about.

```rust
#[async_trait]
impl Actor for GridActor {
    async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> { ... }
    async fn wind_down(&mut self, reason: &WindDownReason, cx: &ActorContext) -> Result<(), BotError> { ... }
    fn status(&self) -> serde_json::Value { ... }
}

#[async_trait]
impl EventHandler<Trade> for GridActor {
    async fn on_event(&mut self, event: Trade, cx: &ActorContext) -> Result<(), BotError> { ... }
}

#[async_trait]
impl EventHandler<Tick> for GridActor {
    async fn on_event(&mut self, event: Tick, cx: &ActorContext) -> Result<(), BotError> { ... }
}
```

The harness guards each actor instance with a mutex so handler calls never
overlap — event handling is serialized per actor, matching actor-model
expectations for single-threaded internal state.

### Brokers

Streaming (feeds) and request-response (order placement, queries) are split.
The [`Broker`](crates/bb-core/src/broker.rs) trait is the REST side. Actors
look up brokers by name in their handlers:

```rust
async fn on_event(&mut self, event: Trade, cx: &ActorContext) -> Result<(), BotError> {
    let broker = cx.broker("bullet")?;
    broker.place_orders(&[replacement_order]).await?;
    Ok(())
}
```

### Harness

[`HarnessBuilder`](crates/bb-core/src/harness/builder.rs) wires everything
together:

```rust
let harness = HarnessBuilder::new()
    .enable_signal_shutdown()
    .wire_broker("bullet", Arc::new(broker))
    .wire_feed_named("bullet-trades", feeds.trade)
    .wire_feed_named("bullet-book",   feeds.book)
    .wire_feed_named("bullet-lifecycle", feeds.lifecycle)
    .wire_feed_named("bullet-mark",   feeds.mark_price)
    .wire_feed_named("ticks",         TickFeed::every_ms(5000))
    .wire_actor(
        ActorSpec::new("grid", grid_actor)
            .sub::<Trade>()
            .sub::<BookUpdate>()
            .sub::<OrderLifecycle>()
            .sub::<Tick>(),
    )
    .build()?;

harness.run().await?;
```

`.sub::<E>()` on an actor spec is checked at compile time against the actor's
`EventHandler<E>` impl — you can't subscribe to an event the actor can't
handle.

### Shutdown

Shutdown is coordinated. The harness returns a
[`WindDownReason`](crates/bb-core/src/harness/actor.rs):

- `Signal` — Ctrl-C or an actor called `cx.request_shutdown()`
- `InputsClosed` — every feed finished on its own
- `FeedFailed { feed, error }` — a feed returned a fatal error
- `ActorFailed { actor, error }` — an actor's `init` or handler returned a fatal error

Each actor's `wind_down` is called with the reason so the actor can decide
cleanup: cancel working orders, flatten positions, log final stats.

## Shared helpers

Strategies pull building blocks from [`bb-core/src/helpers/`](crates/bb-core/src/helpers/):

- [`InventoryTracker`](crates/bb-core/src/helpers/inventory.rs) — net position,
  weighted-average entry price, realized PnL. Call `record_fill(side, price, qty)`
  in your `EventHandler<Trade>` impl.
- [`ClientIdIssuer`](crates/bb-core/src/helpers/client_id.rs) — monotonic u64
  client order IDs encoded as strings. Compatible with Bullet's on-chain
  `ClientOrderId` and HL's `cloid`.
- [`TickFeed`](crates/bb-core/src/helpers/tick_feed.rs) — the heartbeat feed.

These are plain types with no framework magic. Drop them into any actor.

## Workspace Layout

```
crates/
  bb-core/
    src/
      harness/        Harness + traits: Event, EventFeed, Actor, EventHandler;
                      status.rs (HTTP API), testing.rs (ScriptedFeed, MockBroker,
                      MarketDataReplayFeed)
      helpers/        InventoryTracker, ClientIdIssuer, TickFeed
      broker.rs       Broker trait + BrokerRegistry (REST side)
      events.rs       Canonical event types
      types.rs        Shared value types (Order, OrderBook, Side, ...)
      error.rs        BotError
      clock.rs        Clock trait (SystemClock + TestClock for deterministic tests)
  bb-bot/             CLI binary
  exchanges/
    bullet/           Bullet DEX adapter
      src/
        connection.rs   WebSocket demux + typed feed structs
        broker.rs       BulletBroker (Broker trait impl)
        convert.rs      Value conversions
    hyperliquid/      Hyperliquid adapter — same shape as Bullet; subscribes
                      to ActiveAssetCtx so `MarkPriceUpdate.funding_rate` is
                      real (not hardcoded zero)
    binance/          Binance reference-price feed (read-only, no broker)
  strategies/
    simple-mm/           Starter one-bid/one-ask market maker
    grid/                Static grid — fixed-range, anchor-biased
    avellaneda-stoikov/  A-S market maker — actor
    funding-arb/         Cross-venue funding arb — actor
    reference-arb/       Reference-price arb vs Binance perp microprice
config/
  simple-mm-example.toml
  grid-example.toml
  avellaneda-stoikov-example.toml
  funding-arb-example.toml
  reference-arb-example.toml
docs/
  ARCHITECTURE.md              Component diagram + event flow
  CONTRIBUTING-EXCHANGES.md   How to add a new adapter
  strategies/
    grid-design-notes.md
    grid-future-work.md
```

## Adding a Strategy

See [HACKING.md](HACKING.md) for a walkthrough.

## Adding an Exchange

For the harness path:

1. Create `crates/exchanges/<name>/` with:
   - `connection.rs` — owns the venue's WebSocket, demultiplexes into typed
     `mpsc::UnboundedReceiver<E>` channels, exposes `<Name>Feeds` bundle.
   - `broker.rs` — implements `bb_core::broker::Broker` for REST.
   - `convert.rs` — value conversions.
   - `config.rs` — adapter-specific config (TOML-derived).
2. Implement each `EventFeed<E>` by returning an `MpscFeed<E>` from
   `connect_<name>`. See `crates/exchanges/bullet/src/connection.rs` for the
   reference pattern — the muxer task writes to `mpsc::Sender<E>` channels
   and `MpscFeed::new(rx)` wraps each receiver.
3. In `bb-bot/src/main.rs`, add a match arm that calls
   `connect_<name>(&config, &symbol)` and wires the broker + feeds into the
   harness.

The Bullet adapter is the reference. Each piece has a single responsibility:
subscribe, broker, convert, config. See `docs/CONTRIBUTING-EXCHANGES.md` for
the full walkthrough including reconnect patterns and the `Trade` /
`OrderLifecycle` canonical-source invariant.

## Exchange-Specific Notes

**Bullet**: `bullet-rust-sdk` from the public Bullet GitHub repo. Ed25519 keys.
`ManagedWebsocket` provides internal reconnection — the harness only sees
`Disconnected` on terminal give-up. Symbol format: `"BTC-USD"`.

**Hyperliquid**: `hyperliquid_rust_sdk` 0.6 + `ethers` 2. secp256k1 keys.
`InfoClient::with_reconnect` handles reconnection. Symbol mapping: Bullet
`"BTC-USD"` ↔ HL `"BTC"`. `ActiveAssetCtx` provides real funding rates;
`AllMids` remains a mark-price fallback when no funding field is present.

## Config Format

TOML. Top-level sections: `[engine]`, `[exchanges.<name>]`, `[strategy]`,
`[strategy.<type>]`, `[logging]`.

- `[engine]` — `tick_interval_ms`, `status_port` (optional), or
  `status_bind = "host:port"` for explicit bind. `symbol` lives inside each
  `[strategy.<name>]` section so multi-symbol setups are explicit.
- Exchange configs: `type = "<name>"` + adapter-specific fields. Bullet
  resolves key material in this order (explicit config wins; env fills a
  field the config omits, so an ambient env var can't silently switch
  wallets): `key_file` (in config) → env `BB_BULLET_KEY_FILE` →
  `private_key_hex` (in config) → env `BB_BULLET_PRIVATE_KEY_HEX`. File-based
  keystore is preferred — see `bb-bot keygen`. Hyperliquid keys via
  `BB_HYPERLIQUID_PRIVATE_KEY_HEX`. (Standalone `deposit`/`flatten`/`observe`
  take no config, so there env is the source: `BB_BULLET_KEY_FILE` → env hex
  → default keystore.)
- Strategy configs: `type = "<name>"` with sub-table `[strategy.<name>]`.

## Code Style

- Edition 2024, MSRV 1.85
- `cargo +nightly fmt` (imports grouped: std, external, crate)
- Clippy pedantic with overrides for trading-code noise (see `[workspace.lints.clippy]`)
- `unsafe_code = "deny"`
- Errors: `BotError` enum with `is_retryable()` / `is_fatal()`
- `rust_decimal::Decimal` for all money/size fields in core types; adapters
  convert at the boundary
- No panics in adapters — return `BotError` and let the harness decide

## Status API

Opt in via `HarnessBuilder::with_status_port(port)`; each example config sets a
distinct `engine.status_port` (3030–3034) so several bots can run in parallel
without colliding:

- `GET /health` — liveness check.
- `GET /status` — uptime plus each actor's JSON snapshot keyed by name.

Snapshots are produced by `Actor::status()`; the server uses `try_lock` so a
slow handler never blocks the endpoint.

## Testing strategies

`bb-core::harness::testing` provides:

- `ScriptedFeed<E>` — emits a preset list of events, yielding between each so
  subscription tasks have a chance to process before the next event arrives.
  Then exits, causing the harness to shut down cleanly.
- `MarketDataReplayFeed<E>` — like `ScriptedFeed` but each event carries a
  `unix_ms` timestamp; the feed advances a `TestClock` before each send so
  strategies calling `cx.clock().unix_ms()` see deterministic event-driven time.
- `TimedFeed<E>` — emits `(delay, event)` pairs, sleeping `delay` before each
  send. Paired with `tokio::time::pause()` it controls ordering across multiple
  feeds (e.g. a trade arriving between two book updates) without real delays.
- `MockBroker` — records all broker calls and returns pre-queued responses.
  Inspect calls via `history()`, `placed_count()`, `last_placed_orders()`;
  queue responses via `queue_place_response(Ok(()))` / `queue_place_response(Err(e))`.
  `NullBroker` is kept as a type alias for backward compatibility.

`TestClock` (in `bb-core::clock`) pairs with `MarketDataReplayFeed` and can
be injected via `HarnessBuilder::with_clock(Arc::new(clock))`.

See `crates/strategies/grid/src/strategy.rs` (`integration_tests` module) and
`crates/strategies/reference-arb/src/strategy.rs` (`tests` module) for worked
examples that drive the full state machine using `ScriptedFeed` + `MockBroker`.
