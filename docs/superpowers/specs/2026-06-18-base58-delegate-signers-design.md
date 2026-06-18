# Base58 signers + delegate support + onboarding docs

Date: 2026-06-18
Status: approved

## Problem

Two UX gaps make the starter repo hard to use with real Bullet wallets, plus a
documentation gap:

1. **Keys must be hex.** Bullet's delegation export and Phantom both hand users
   a **base58** secret. The adapter only accepts hex (`Keypair::from_hex`), so
   users have to convert by hand.
2. **Delegate signers break on startup.** A delegate (a.k.a. API) wallet has no
   account of its own. The adapter queries account/balances/orders and
   subscribes to the user-orders stream using the *signer's own* address, so
   with a delegate key every account read fails (the delegate address has no
   account data). Trading actions themselves work — the exchange routes a
   delegate's signed orders to the master account server-side — only the read
   path is broken.
3. **No from-scratch onboarding.** There's a testnet-burner quick start
   (`keygen` + faucet + `deposit`) but no guide for the recommended real-usage
   path: bring your own wallet, deposit via the UI, create a scoped delegate,
   and trade with that delegate key.

## Findings (grounding)

SDK: `bullet-rust-sdk` 0.0.27.

- `Keypair` exposes `from_hex(&str)`, `from_bytes([u8; 32])`, `generate()`,
  `read_from_file(path)`. **No base58 constructor.** `from_hex` strips `0x`,
  hex-decodes to 32 bytes, calls `from_bytes`. `address()` returns the base58
  address string.
- Account reads have two flavors. The `my_*` wrappers derive the address from
  the keypair; the underlying methods take an explicit address and return the
  **same types**:
  - `my_balances()` → `account_balance(&address)` → `Vec<Balance>`
  - `my_account()` → `account_info(&address)` → `Account`
  - `my_open_orders(symbol)` → `query_open_orders(&address, Some(symbol))` → `Vec<BinanceOrder>`
- The SDK has **no delegate concept** and `delegateOf` is **not** in its
  generated OpenAPI, so resolution is a raw HTTP call.
- `Client::url()` returns the REST base URL; the SDK does not expose its inner
  `reqwest` client, so the delegate call uses its own `reqwest` client.

The official Bullet docs confirm the design
([delegate-accounts](https://tradingapi.bullet.xyz/docs/delegate-accounts.md),
[account-setup](https://tradingapi.bullet.xyz/docs/account-setup.md)):

- "For read endpoints (account info, balances, positions, open orders), always
  use your **main account's** address — not the delegate's. All state lives on
  the main account." — exactly the read-path bug.
- The exchange "resolves the delegate to your main account automatically — no
  special fields or flags needed" when signing. Confirms signing is untouched.
- Delegates **cannot deposit or withdraw** — confirms `deposit` stays
  master-only.
- "A delegate address cannot already have its own account" — the delegate
  address genuinely has no account, so `delegateOf`-404 = "this key is its own
  account" is correct.
- The Bullet **main account is an embedded wallet** created on sign-in at
  app.bullet.xyz; deposits are made through the webapp.

`delegateOf` contract (from the live Bullet Trading API OpenAPI 3.1 spec):

```
GET {base_url}/api/v1/delegateOf?address=<delegate_address>
200 → DelegateOf { parent: string, name: string, flags: i32, expiresAt: i64|null }
404 → ApiErrorResponse  (address is not a delegate)
```

Entry points that parse a key / read account data:

- `connect()` (`crates/exchanges/bullet/src/connection.rs`) — the run path. Also
  reused by `flatten` and `observe` via `connect_bullet()`, so fixing it here
  fixes all three.
- `load_deposit_keypair()` (`crates/bb-bot/src/main.rs`) — `deposit` only. Signs
  a `Deposit` action; never reads account data.

## Design

### 1. Base58 key parsing (auto-detect)

New module `crates/exchanges/bullet/src/key.rs`:

```rust
pub fn keypair_from_secret(s: &str) -> Result<Keypair, BotError>
```

Detection (unambiguous):

1. Trim whitespace; strip an optional `0x` prefix.
2. If it matches `^[0-9a-fA-F]{64}$` → **hex** → `Keypair::from_hex`.
3. Else → **base58** → `bs58::decode`:
   - 64 bytes → Phantom/Solana full secret key → take first 32 bytes →
     `Keypair::from_bytes`.
   - 32 bytes → raw seed → `Keypair::from_bytes`.
   - otherwise → error with a clear message.

This is unambiguous because a real base58 secret is ~44 chars (32 bytes) or ~88
chars (64 bytes) — never 64 hex chars — and a 64-hex key decodes cleanly to 32
bytes.

Wired into both `connect()` (replacing the inline `Keypair::from_hex` at
connection.rs:91-99, keeping the existing `key_file` branch and empty-key error)
and `load_deposit_keypair()`.

**Config surface.** Keep the struct field `private_key_hex` (internal name), add
serde `#[serde(alias = "private_key")]`, and accept env `BB_BULLET_PRIVATE_KEY`.
Both feed the same field; the parser auto-detects format. The existing
`private_key_hex` / `BB_BULLET_PRIVATE_KEY_HEX` names keep working as aliases.
`private_key` / `BB_BULLET_PRIVATE_KEY` become the documented canonical names.
Update the env-merge in `main.rs` (lines ~160-163) and `bullet_config_from_env`
to read `BB_BULLET_PRIVATE_KEY` first, then `BB_BULLET_PRIVATE_KEY_HEX`.

New dependency on the bullet adapter crate: `bs58`.

### 2. Delegate auto-resolution

New module `crates/exchanges/bullet/src/delegate.rs`:

```rust
// thin HTTP wrapper
pub async fn resolve_account_address(base_url: &str, signer: &str) -> Result<String, BotError>;

// pure decision fn — unit-testable without HTTP
fn account_address_from(signer: &str, status: u16, body: &str) -> Result<String, BotError>;
```

- `200` → parse `DelegateOf`, return `parent`; log
  `delegate '{name}' → master {parent}`.
- `404` → return `signer` (the key is its own account; this is the normal
  master-key path).
- anything else → `BotError`.

In `connect()`, after `let signer = client.address()?`:

```rust
let account_address = resolve_account_address(client.url(), &signer).await?;
```

Use `account_address` for the `Topic::user_orders(...)` subscription
(connection.rs:131,137) and pass it into the broker. Log both `signer` and
`account_address` so the distinction is visible.

`BulletBroker` gains an `account_address: String` field (set via
`BulletBroker::new`). Its three reads switch to the explicit-address SDK methods
(same return types, so the mapping code below them is unchanged):

| Before | After |
|---|---|
| `self.client.my_balances()` | `self.client.account_balance(&self.account_address)` |
| `self.client.my_account()` | `self.client.account_info(&self.account_address)` |
| `self.client.my_open_orders(symbol)` | `self.client.query_open_orders(&self.account_address, Some(symbol))` |

Signing is untouched: the delegate keypair signs, and the exchange routes
trading actions to the master account.

New dependency on the bullet adapter crate: `reqwest` (json feature; already in
the lockfile via other adapters/SDK).

### 3. Onboarding docs

- Expand README "Recommended first path" + "Key management" and the AGENTS.md
  setup section with a **from-scratch (recommended, real-usage) path**, mirroring
  Bullet's official [account-setup](https://tradingapi.bullet.xyz/docs/account-setup.md)
  flow:
  1. Sign in at [app.bullet.xyz](https://app.bullet.xyz) (or
     app.testnet.bullet.xyz) with your wallet (e.g. Phantom). This creates the
     embedded wallet that is your Bullet trading account.
  2. Deposit collateral through the webapp UI (initializes the trading account).
  3. Create/register a delegate (see Bullet's
     [delegate setup guide](https://docs.bullet.xyz/bulletx-exchange/how-to-guide/delegate-account-setup));
     copy the delegate signer private key into `.env` as `BB_BULLET_PRIVATE_KEY`
     (base58 from Phantom, hex, or a Solana JSON keystore via `key_file` all
     work).
  4. Hyperliquid: create an API wallet at app.hyperliquid.xyz/API; copy its key
     into `.env` as `BB_HYPERLIQUID_PRIVATE_KEY_HEX`.
- Add a short **"What are delegate / API wallets?"** note: a separate keypair
  scoped to trading only (cannot deposit or withdraw), revocable from the webapp
  at any time like an API key, so you trade without exposing your main wallet's
  private key. Link Bullet's delegate-accounts doc.
- Link the API's machine-readable docs (`/llms.txt`,
  `/docs/rest/openapi.json`) from the contributing/exchange docs.
- Keep the existing `keygen` + faucet + `deposit` flow documented as the
  **testnet burner / quick-start** alternative.
- Add `.env.example` with `BB_BULLET_PRIVATE_KEY=` and
  `BB_HYPERLIQUID_PRIVATE_KEY_HEX=` plus comments. (`.env` is already
  gitignored.)

## Out of scope / follow-up

- **Hyperliquid** gets docs only. HL API/agent wallets likely need the same
  master-address resolution for info queries; the current code assumes the
  signer's own address is queryable. Flagged as a follow-up, not implemented
  here.
- `delegateOf`'s `expiresAt` / `flags` are logged but not enforced.
- `deposit` stays signer-address-only (no delegate resolution): users deposit
  from the master wallet via the UI, not via a delegate.

## Testing

- Unit tests for `keypair_from_secret`: a generated key exported as hex (with and
  without `0x`), base58-32, and base58-64 all resolve to the **same address**;
  malformed input errors.
- Unit tests for `account_address_from`: `200`+parent → parent; `404` → signer;
  `500` → error.
- Existing broker/connection tests continue to pass; the broker change is a
  method swap with identical return types.

## Docs-platform feedback (not part of this repo)

The API docs are already in good shape for machine consumption — `/llms.txt`
indexes dedicated Markdown docs (including delegate-accounts and account-setup)
and links the raw spec at `/docs/rest/openapi.json`. Two minor notes for the API
team: (1) `WebFetch`/plain HTTP clients get `403` on the docs host (looks like
User-Agent filtering) — `curl` with a browser UA works; allowlisting common
fetchers would help LLM tooling. (2) The conventional `/openapi.json` path 404s;
a redirect to `/docs/rest/openapi.json` would catch tools that guess the
canonical location.
