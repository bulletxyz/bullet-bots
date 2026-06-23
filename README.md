# bullet-bots

<p align="center">
  <img src="docs/assets/bullet-bots-banner.png" alt="bullet-bots: Rust trading bots for Bullet perpetuals">
</p>

[![CI](https://github.com/bulletxyz/bullet-bots/actions/workflows/ci.yml/badge.svg)](https://github.com/bulletxyz/bullet-bots/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85+-orange.svg)](https://www.rust-lang.org)

Live-trading-capable reference framework for [Bullet](https://bullet.xyz) perpetuals and other exchanges — written in Rust, with a small typed runtime for building event-driven bots.

Ships with exchange adapters for Bullet, Hyperliquid, and Binance, plus five ready-to-run strategies you can use out of the box or extend with your own logic.

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

## Quick start — testing (testnet, ~5 minutes)

The fastest path: a throwaway testnet key funded from the faucet. **No wallet,
no web UI, no real funds.** This is the recommended way to try the bot.

```sh
cargo build
cargo run --bin bb-bot -- validate --config config/simple-mm-example.toml   # no keys needed

# 1. Generate a testnet burner key → writes ~/.config/bullet/id.key (0600),
#    prints your address and the exact faucet command.
cargo run --bin bb-bot -- keygen --network testnet

# 2. Fund it from the faucet.
cargo run --bin bb-bot -- faucet --network testnet
#    The faucet is rate-limited and Cloudflare-protected; if this 403s, use the
#    web faucet at https://app.testnet.bullet.xyz and fund your printed address.

# 3. Move funds into the perp margin account. This also initializes the trading
#    account — without it, order placement fails with `user_variants not found`.
cargo run --bin bb-bot -- deposit --network testnet --asset USDC --amount 5000

# 4. Run the starter market maker (reads ~/.config/bullet/id.key by default).
cargo run --bin bb-bot -- run --config config/simple-mm-example.toml
```

Other commands: `observe` (collect Bullet/Binance spread data, no trading),
`flatten` (cancel orders + market-close positions), `validate` (preflight a config).

## Production (mainnet, real funds)

**Do not run mainnet with a `keygen` burner** — that puts a key controlling real
funds inside the bot. Instead use a **delegate** (Bullet) / **API wallet**
(Hyperliquid): a separate key scoped to trading only (cannot deposit or
withdraw), revocable from the webapp at any time, so the bot never holds a key
that can drain your wallet.

1. Sign in at [app.bullet.xyz](https://app.bullet.xyz) with your wallet (e.g.
   Phantom) — this creates the embedded wallet that is your Bullet trading account.
2. Deposit collateral through the webapp.
3. Create a delegate (see the
   [delegate setup guide](https://docs.bullet.xyz/bulletx-exchange/how-to-guide/delegate-account-setup)),
   then put its **base58** key in `.env` as `BB_BULLET_PRIVATE_KEY` (or save it
   to a file and point `BB_BULLET_KEY_FILE` at it).
4. For Hyperliquid, create an API wallet at
   [app.hyperliquid.xyz/API](https://app.hyperliquid.xyz/API). Set
   `BB_HYPERLIQUID_PRIVATE_KEY` to the **API-wallet key** (hex), and
   `BB_HYPERLIQUID_ACCOUNT_ADDRESS` to your **main account address** (the `0x…`
   shown in the HL UI). The API wallet signs; positions/balances/fills are read
   from the main account.
5. Set `network = "mainnet"` in the config's `[exchanges.*]` sections.

> **What is a delegate / API wallet?** A separate keypair authorized to trade on
> behalf of your account. It can place and cancel orders but **cannot deposit or
> withdraw**, and you can revoke it from the webapp at any time — so you trade
> without exposing your main wallet's private key. On both venues the bot signs
> with this key but reads account state from the **main account** — Bullet
> resolves the master automatically via `delegateOf`; on Hyperliquid you supply
> it via `BB_HYPERLIQUID_ACCOUNT_ADDRESS`.

Put these in `.env` — `bb-bot` auto-loads `./.env` at startup, so
`cp .env.example .env`, fill it in, and run. (Use `--env-file <path>` to load a
different file; real environment variables already set take precedence.)

## Key management

Keys use each venue's native format — **paste exactly what the UI gives you**:
**base58 for Bullet** (Phantom / delegation export), **hex for Hyperliquid** (the
HL API page). Never put them in `config/*.toml`.

- **Key string in `.env` (typical):** set `BB_BULLET_PRIVATE_KEY` (base58) and
  `BB_HYPERLIQUID_PRIVATE_KEY` (hex); auto-loaded from `.env`. When the
  Hyperliquid key is an API wallet, also set `BB_HYPERLIQUID_ACCOUNT_ADDRESS` to
  your main account address.
- **Key file (keeps the secret off the environment):** for Bullet, generate one
  with `cargo run --bin bb-bot -- keygen`, then set `BB_BULLET_KEY_FILE` (or
  `key_file` in `[exchanges.bullet]`). Both venues accept `key_file` /
  `BB_<VENUE>_KEY_FILE` — a file containing the key string; it takes precedence
  over the inline key.

## Strategies

Start with `simple-mm`, then move down the table as you need more machinery.

| Order | Strategy | Description | Config example |
|---|---|---|---|
| 1 | [Simple MM](crates/strategies/simple-mm/README.md) | One bid/ask around mid with refresh + inventory cap | `config/simple-mm-example.toml` |
| 2 | [Grid](crates/strategies/grid/README.md) | Fixed-range level grid with anchor bias and trend filter | `config/grid-example.toml` |
| 3 | [Reference arb](crates/strategies/reference-arb/README.md) | Spread arb between Bullet and Binance perpetuals | `config/reference-arb-example.toml` |
| 4 | [Avellaneda-Stoikov](crates/strategies/avellaneda-stoikov/README.md) | Reservation-price market maker with inventory skew and multi-level ladder | `config/avellaneda-stoikov-example.toml` |
| 5 | [Funding arb](crates/strategies/funding-arb/README.md) | Cross-venue delta-neutral funding rate arb | `config/funding-arb-example.toml` |

Each README covers what the strategy does, its state machine, key design decisions, config reference, and future work.

## Writing your own strategy

1. Copy `crates/strategies/simple-mm` or create a crate in `crates/strategies/<name>/`.
2. Implement `Actor` + `EventHandler<E>` for each event type you care about.
3. Register in `bb-bot/src/main.rs` with `HarnessBuilder::wire_actor`.
4. Add an example config.

Full walkthrough: [HACKING.md](HACKING.md).

## Status API

While running, the bot exposes an HTTP status endpoint on `engine.status_port`
(default 3030, bound to `127.0.0.1`):

- `GET /health` — liveness check
- `GET /status` — uptime plus every actor's JSON snapshot keyed by name

```sh
curl localhost:3030/status
```

```jsonc
{
  "uptime_secs": 142,
  "actors": {
    "simple-mm": {
      "symbol": "BTC-USD",
      "dry_run": false,
      "net_position": "0.002",   // signed inventory (+ long / - short)
      "realized_pnl": "1.37",    // closed-trade PnL in quote units
      "total_fills": 5,          // count of Trade events applied
      "mid": "68250.5",
      "bid": { "side": "Buy",  "price": "68181.2", "client_id": "12", "order_id": "9001" },
      "ask": { "side": "Sell", "price": "68319.8", "client_id": "13", "order_id": "9002" }
    }
  }
}
```

The actor snapshot is whatever that strategy's `Actor::status()` returns, so
fields vary per strategy (the example above is `simple-mm`).

To expose on a non-loopback address (e.g. for remote monitoring), set
`engine.status_bind = "0.0.0.0:3030"` — note that the endpoint exposes
positions and PnL, so firewall accordingly.

## Troubleshooting

Common first-run errors and their fixes:

- **`user_variants not found`** on order placement — you skipped the `deposit`
  step, so the trading account was never initialized. Run:
  ```sh
  cargo run --bin bb-bot -- deposit --network testnet --asset USDC --amount 5000
  ```
- **reference-arb "refuses to run with a non-flat position"** — a prior session
  left an open position. Flatten it first:
  ```sh
  cargo run --bin bb-bot -- flatten --network testnet --symbol BTC-USD
  ```
- **Status port already in use** — two bots can't share `engine.status_port`.
  Give each running bot a unique port (the example configs use 3030–3034).

## Intentionally out of scope

The following are explicit non-goals for v1 — listing them reduces issues
and clarifies where to build extensions:

- **Backtest / replay harness** — The framework ships `Clock` / `MockBroker` /
  `ScriptedFeed` test primitives; a full fill-simulation engine is not provided.
- **Persistence / crash recovery / journal** — No event log or replay on restart.
- **Prometheus metrics** — No `/metrics` endpoint; `/status` is JSON-only.
- **Rate limiting** — Each broker manages its own rate limit; the framework has no
  built-in token-bucket or request queue.
- **Global instrument validation** — Bullet snaps tick-size / lot-size in its
  broker, but the framework does not provide a venue-independent min-notional
  or risk-budget preflight.
- **Extended `OrderType`** — `Limit`, `PostOnly`, `Market` only. No IOC, FOK, GTD,
  or `time_in_force` plumbing beyond what the adapters already need.

## Requirements

- Rust 1.85+ (edition 2024)
- `cargo +nightly fmt` for formatting (optional)

## Risk Disclaimer

This is an open-source **reference implementation** intended for educational and research purposes. It is not a commercial product and does not constitute financial or investment advice.

Automated trading strategies involve substantial financial risk. Bugs, network failures, exchange outages, adverse market conditions, and misconfiguration can all result in partial or total loss of capital. You are solely responsible for any funds you deploy using this software.

By running this software against live markets you accept that:

- The authors and contributors make no representations or warranties of any kind, express or implied, regarding the software's fitness for trading or any other purpose.
- The authors and contributors shall not be liable for any financial losses, damages, or other claims arising from the use of this software.
- Past behaviour in test or simulated environments is not indicative of future results in live markets.

The MIT licence under which this software is distributed expressly disclaims all implied warranties and limits liability to the fullest extent permitted by applicable law. See [LICENSE](LICENSE).
