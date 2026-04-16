# bullet-bots

Open-source trading bot framework for the [Bullet](https://bullet.xyz) perpetual futures DEX and other exchanges.

## What's in the box

- **bb-core** — Engine, traits (`Exchange`, `Strategy`), types, error handling, status API
- **bb-exchange-bullet** — Bullet DEX adapter (REST + auto-reconnecting WebSocket)
- **bb-exchange-hyperliquid** — Hyperliquid adapter (REST + WS bridge)
- **bb-strategy-grid** — Grid trading strategy (single exchange)
- **bb-strategy-funding-arb** — Funding rate arbitrage across two exchanges

## Quick start

```sh
# Build
cargo build

# Run tests
cargo test

# Validate config
cargo run --bin bb-bot -- validate --config config/grid-example.toml

# Run (set keys via env vars, never in config files)
export BB_BULLET_PRIVATE_KEY_HEX="0x..."
cargo run --bin bb-bot -- run --config config/grid-example.toml
```

## Strategies

### Grid

Places limit orders at fixed intervals around a reference price. When one fills, places a new order on the opposite side. Tracks net position and pauses when limits are hit.

```sh
cargo run --bin bb-bot -- run --config config/grid-example.toml
```

### Funding Rate Arbitrage

Monitors funding rates on Bullet and Hyperliquid. When the differential exceeds a threshold, opens a delta-neutral position (short the higher-rate exchange, long the lower-rate exchange). Exits when the spread narrows.

```sh
export BB_BULLET_PRIVATE_KEY_HEX="0x..."
export BB_HYPERLIQUID_PRIVATE_KEY_HEX="0x..."
cargo run --bin bb-bot -- run --config config/funding-arb-example.toml
```

## Adding your own strategy

1. Create a crate in `crates/strategies/<name>/`
2. Implement the `Strategy` trait from `bb-core`
3. Wire it into `bb-bot/src/main.rs`
4. Add an example config

See `crates/strategies/grid/` for a minimal example.

## Status API

While running, the bot exposes an HTTP status endpoint:

- `GET /health` — liveness check
- `GET /status` — strategy state, uptime, symbol

Default port: 3030 (configurable via `engine.status_port`).

## Requirements

- Rust 1.85+ (edition 2024)
- `cargo +nightly fmt` for formatting (optional)

## License

MIT
