# bullet-bots

Modular trading bot framework in Rust. Pluggable exchange adapters and strategies connected by a trait-based engine.

## Build & Test

```sh
cargo build                # full workspace
cargo test                 # all unit tests (22 currently)
cargo clippy               # lints (pedantic enabled, see Cargo.toml overrides)
cargo +nightly fmt         # format (nightly required for import grouping)
```

Validate a config without connecting:

```sh
cargo run --bin bb-bot -- validate --config config/grid-example.toml
```

Run a bot:

```sh
export BB_BULLET_PRIVATE_KEY_HEX="0x..."
export BB_HYPERLIQUID_PRIVATE_KEY_HEX="0x..."
cargo run --bin bb-bot -- run --config config/funding-arb-example.toml
```

## Workspace Layout

```
crates/
  bb-core/           Core traits + engine (Exchange, Strategy, Engine, types, error)
  bb-bot/            CLI binary — config loading, exchange/strategy dispatch
  exchanges/
    bullet/          Bullet DEX adapter (bullet-rust-sdk, Ed25519 keys)
    hyperliquid/     Hyperliquid adapter (hyperliquid_rust_sdk, secp256k1/ethers keys)
  strategies/
    grid/            Grid trading (single exchange)
    funding-arb/     Funding rate arb (two exchanges, delta-neutral)
config/
  grid-example.toml
  funding-arb-example.toml
```

## Key Traits

**`Exchange`** (`bb-core/src/exchange.rs`) — connect, subscribe, place/cancel orders, recv WS events. Each adapter owns its REST client + WS connection.

**`Strategy`** (`bb-core/src/strategy.rs`) — `on_start`, `on_tick`, `on_event`, `on_stop`, `status`. Receives a `StrategyContext` with access to all exchanges (by name) and cached state.

**`StrategyContext`** — wraps `HashMap<String, Box<dyn Exchange>>`. Call `ctx.place_orders("bullet", &orders)` etc. Cached orderbooks/positions updated automatically from events via `apply_event`.

## Adding an Exchange

1. Create `crates/exchanges/<name>/` with `config.rs`, `convert.rs`, `adapter.rs`, `lib.rs`
2. Implement `Exchange` trait — follow the Bullet adapter pattern
3. Add workspace dep in root `Cargo.toml`, dep in `bb-bot/Cargo.toml`
4. Add match arm in `bb-bot/src/main.rs` `build_exchanges()` + env var override in `load_config()`

## Adding a Strategy

1. Create `crates/strategies/<name>/` with `config.rs`, `strategy.rs`, `lib.rs`
2. Implement `Strategy` trait
3. Add workspace dep, bb-bot dep, match arm in `build_strategy()`
4. Add example config in `config/`

## Config Format

TOML. Top-level sections: `[engine]`, `[exchanges.<name>]`, `[strategy]`, `[strategy.<type>]`, `[logging]`.

- `[engine]` — `symbol`, `tick_interval_ms`, `reconnect_max_delay_ms`, `status_port`
- Exchange configs use `type = "<name>"` + adapter-specific fields. Private keys via env vars.
- Strategy configs use `type = "<name>"` with sub-table `[strategy.<name>]`

## Exchange-Specific Notes

**Bullet**: Uses `bullet-rust-sdk` (local path dep). Ed25519 keys. `ManagedWebsocket` handles reconnection. Symbol format: `"BTC-USD"`.

**Hyperliquid**: Uses `hyperliquid_rust_sdk` 0.6 + `ethers` 2. secp256k1 keys (Ethereum wallet). WS bridge pattern: HL SDK pushes `Message` into an `mpsc::UnboundedSender`, background task converts to `ExchangeEvent`. Symbol mapping: Bullet `"BTC-USD"` ↔ HL `"BTC"`. AllMids subscription provides mark prices; funding rates currently emitted as zero (needs `ActiveAssetCtx` subscription or REST poll — TODO).

## Engine Event Loop

`Engine::run()` in `bb-core/src/engine.rs`:
1. Connect all exchanges (with backoff)
2. Subscribe all exchanges to `config.symbol`
3. Spawn HTTP status API (axum, `/health` + `/status`)
4. Call `strategy.on_start()`
5. `tokio::select!` over: tick interval → `on_tick`, any exchange event → `apply_event` + `on_event`, ctrl-c → shutdown
6. On disconnect: reconnect with exponential backoff, re-subscribe, refresh state

## Status API

Runs on `config.engine.status_port` (default 3030).

- `GET /health` → `{"status": "ok"}`
- `GET /status` → `{"strategy": "...", "symbol": "...", "uptime_secs": N, "strategy_status": {...}}`

## Code Style

- Edition 2024, MSRV 1.85
- `cargo +nightly fmt` (imports grouped: std, external, crate)
- Clippy pedantic with overrides for trading code noise (see `[workspace.lints.clippy]`)
- `unsafe_code = "deny"`
- Errors: `BotError` enum with `is_retryable()` / `is_fatal()` for engine decisions
- Decimal: `rust_decimal::Decimal` everywhere in bb-core types; adapters convert at the boundary
- No panics in adapters — return `BotError` and let the engine decide
