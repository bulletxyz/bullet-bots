# bullet-bots

Open-source event-driven trading bot framework for the [Bullet](https://bullet.xyz)
perpetual futures DEX and other exchanges.

## What's in the box

- **bb-core** — Harness, event bus, `Actor` / `EventFeed` / `Broker` traits,
  shared helpers (`InventoryTracker`, `ClientIdIssuer`, `TickFeed`)
- **bb-exchange-bullet** — Bullet DEX adapter — typed feeds + REST broker
- **bb-exchange-hyperliquid** — Hyperliquid adapter — typed feeds + REST broker
- **bb-strategy-grid** — Static grid bot (fixed price range, anchor-biased)
- **bb-strategy-avellaneda-stoikov** — A-S market maker actor
- **bb-strategy-funding-arb** — Cross-venue funding arb actor

## Architecture at a glance

Strategies are **actors** that consume typed **events** (`Trade`, `BookUpdate`,
`OrderLifecycle`, `MarkPriceUpdate`, `Tick`) published by **feeds** owned by
exchange adapters. A shared **harness** wires everything together and handles
lifecycle, reconnection, and shutdown. See [AGENTS.md](AGENTS.md) for the
full architecture tour and [HACKING.md](HACKING.md) for a walkthrough of
adding your own strategy.

Key invariant: `Trade` is the only canonical source of position/PnL changes,
so double-count bugs from adapters that emit both trade and order-update
events are structurally impossible.

## Quick start

```sh
# Build
cargo build

# Run tests (57 passing)
cargo test

# Validate a config
cargo run --bin bb-bot -- validate --config config/grid-example.toml

# Run (set keys via env vars, never in config files)
export BB_BULLET_PRIVATE_KEY_HEX="0x..."
cargo run --bin bb-bot -- run --config config/grid-example.toml
```

## Strategies

### Grid

Static grid: N uniformly-spaced levels across a fixed `[lower_price,
upper_price]` range. Initial bias is set by `anchor_price` — levels below
the anchor start as buys, levels above start as sells. When a buy at level
`N` fills, the actor re-arms level `N+1` as a sell (and vice versa), so
every completed round trip harvests `spacing × order_size`. Levels never
move; price leaving the range = grid idles until it returns.

```sh
cargo run --bin bb-bot -- run --config config/grid-example.toml
```

### Avellaneda-Stoikov market maker

Closed-form reservation-price quoting with inventory skew, plus a
multi-level ladder. Runs as an actor on the harness.

```sh
cargo run --bin bb-bot -- run --config config/avellaneda-stoikov-example.toml
```

### Funding rate arbitrage

Monitors funding rates on two venues and opens a delta-neutral position when
the differential exceeds a threshold. Per-venue inventory tracking, phase
state machine (Flat / Entering / Active / Exiting), emergency-flatten on
timeout or delta imbalance. Runs as an actor on the harness.

```sh
export BB_BULLET_PRIVATE_KEY_HEX="0x..."
export BB_HYPERLIQUID_PRIVATE_KEY_HEX="0x..."
cargo run --bin bb-bot -- run --config config/funding-arb-example.toml
```

## Writing your own strategy

1. Create a crate in `crates/strategies/<name>/`.
2. Implement `Actor` + `EventHandler<E>` for each event type you care about.
3. Register in `bb-bot/src/main.rs` with `HarnessBuilder::wire_actor`.
4. Add an example config.

Full walkthrough: [HACKING.md](HACKING.md).

## Status API

While running, the bot exposes an HTTP status endpoint on `engine.status_port`
(default 3030):

- `GET /health` — liveness check
- `GET /status` — uptime plus every actor's JSON snapshot keyed by name

## Requirements

- Rust 1.85+ (edition 2024)
- `cargo +nightly fmt` for formatting (optional)

## License

MIT
