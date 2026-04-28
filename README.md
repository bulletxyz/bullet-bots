<p align="center">
  <img src="docs/banner.svg" alt="bullet-bots" width="900"/>
</p>

<p align="center">
  <a href="https://github.com/0xtristan/bullet-bots/actions/workflows/ci.yml"><img src="https://github.com/0xtristan/bullet-bots/actions/workflows/ci.yml/badge.svg" alt="CI"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"/></a>
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/rust-1.85+-orange.svg" alt="Rust 1.85+"/></a>
</p>

Production-grade, open-source trading bot framework for [Bullet](https://bullet.xyz) perpetuals and other exchanges — written in Rust, built for latency-sensitive environments.

Ships with exchange adapters for Bullet, Hyperliquid, and Binance, plus four ready-to-run strategies you can use out of the box or extend with your own logic.

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

For an annotated component diagram, event-flow walkthrough, adapter layout
rules, and the broker contract, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Quick start

```sh
# Build
cargo build

# Run tests
cargo nextest run

# Validate a config (no keys needed)
cargo run --bin bb-bot -- validate --config config/grid-example.toml

# Run
cargo run --bin bb-bot -- run --config config/grid-example.toml
```

## Key management

Keys are passed via environment variables, never in config files. Two options:

- **Key file (recommended):** generate once with `cargo run --bin bb-bot -- keygen`, then set `BB_BULLET_KEY_FILE` / `BB_HYPERLIQUID_KEY_FILE`.
- **Hex key:** set `BB_BULLET_PRIVATE_KEY_HEX` / `BB_HYPERLIQUID_PRIVATE_KEY_HEX`, e.g. via a `.env` file (already gitignored).

## Strategies

| Strategy | Description | Config example |
|---|---|---|
| [Grid](crates/strategies/grid/README.md) | Fixed-range level grid with anchor bias and trend filter | `config/grid-example.toml` |
| [Avellaneda-Stoikov](crates/strategies/avellaneda-stoikov/README.md) | Reservation-price market maker with inventory skew and multi-level ladder | `config/avellaneda-stoikov-example.toml` |
| [Funding arb](crates/strategies/funding-arb/README.md) | Cross-venue delta-neutral funding rate arb | `config/funding-arb-example.toml` |
| [Reference arb](crates/strategies/reference-arb/README.md) | Spread arb between Bullet and Binance perpetuals | `config/reference-arb-example.toml` |

Each README covers what the strategy does, its state machine, key design decisions, config reference, and future work.

## Writing your own strategy

1. Create a crate in `crates/strategies/<name>/`.
2. Implement `Actor` + `EventHandler<E>` for each event type you care about.
3. Register in `bb-bot/src/main.rs` with `HarnessBuilder::wire_actor`.
4. Add an example config.

Full walkthrough: [HACKING.md](HACKING.md).

## Status API

While running, the bot exposes an HTTP status endpoint on `engine.status_port`
(default 3030, bound to `127.0.0.1`):

- `GET /health` — liveness check
- `GET /status` — uptime plus every actor's JSON snapshot keyed by name

To expose on a non-loopback address (e.g. for remote monitoring), set
`engine.status_bind = "0.0.0.0:3030"` — note that the endpoint exposes
positions and PnL, so firewall accordingly.

## Intentionally out of scope

The following are explicit non-goals for v1 — listing them reduces issues
and clarifies where to build extensions:

- **Backtest / replay harness** — The framework ships `Clock` / `MockBroker` /
  `ScriptedFeed` test primitives; a full fill-simulation engine is not provided.
- **Persistence / crash recovery / journal** — No event log or replay on restart.
- **Prometheus metrics** — No `/metrics` endpoint; `/status` is JSON-only.
- **Rate limiting** — Each broker manages its own rate limit; the framework has no
  built-in token-bucket or request queue.
- **Instrument validation** — No tick-size / lot-size / min-notional rounding;
  strategies post raw prices and the venue rejects bad ones.
- **Extended `OrderType`** — `Limit`, `PostOnly`, `Market` only. No IOC, FOK, GTD,
  or `time_in_force` plumbing beyond what the adapters already need.

## Requirements

- Rust 1.85+ (edition 2024)
- `cargo +nightly fmt` for formatting (optional)

## License

MIT
