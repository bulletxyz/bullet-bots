# Avellaneda-Stoikov market maker

## What it does

A closed-form market maker based on the Avellaneda-Stoikov (2008) paper.
The strategy quotes a multi-level bid/ask ladder around a *reservation price*
that shifts with inventory:

```
r = s − q · γ · σ² · τ
```

where `s` is the mid price, `q` is normalized inventory, `γ` is risk aversion,
`σ` is rolling volatility, and `τ` is the quote horizon. The optimal half-spread
from the paper is:

```
δ = γ · σ² · τ / 2  +  (1/γ) · ln(1 + γ/κ)
```

The strategy earns by capturing the bid/ask spread while the inventory skew
keeps net position near zero. As the book fills long, bids shift down and asks
come in easier — the model naturally unwinds before hitting `max_position`.

**Two modes**, selected by config:

- **Single-venue (default):** `s` and `σ` come from the trading venue's own
  `BookUpdate` mid. Faithful to the paper.
- **Fair-value MM:** when `reference_exchange` is set (e.g. Binance), `s` and
  `σ` come from the reference feed instead. The local book is used only for
  would-cross checks. This is the preferred mode on Bullet, where the local
  book may be thinner than a tier-1 CEX reference.

## State machine

The actor maintains a `Vec<QuoteSlot>` tracking every resting order (side,
level, price, order_id). On each price update it runs `reconcile_orders()`, a
diff against the desired ladder that computes the minimum set of amends, places,
and cancels:

- Slots within `amend_threshold_bps` of their target price: kept (no round-trip)
- Slots whose price moved past threshold: batch-amended
- Slots with no corresponding target (e.g. inventory cap disabled a side): cancelled
- Targets with no matching slot: placed fresh

A periodic REST reconciliation (`reconcile_interval_secs`) catches phantom slots
(slot tracked locally but order not live on venue) and orphan orders (live on
venue but not tracked locally). It also fires immediately on any WS reconnect.

## Key design decisions

**Bessel-corrected sample variance.** The volatility estimator uses
`÷ (n−1)` (sample variance, not population variance). With the naive `÷ n`
formula, short windows of 2–4 samples produce a systematically tighter spread
than the A-S model requires — exactly when the estimate should be most
conservative. Minimum 3 samples (2 returns) are required before any quote is
placed.

**Per-level amend thresholds.** The threshold widens with level index
(`amend_threshold_bps + amend_threshold_step_bps × level`). Inner levels need
tighter price discipline; outer levels can absorb more drift before a round-trip
is worth it.

**PostOnly would-cross guard.** Before keeping a slot in the would-cross branch,
the reconciler checks whether the *existing* slot's price also crosses the current
book. If it does, the slot is allowed to fall through to the cancel sweep rather
than being kept as a phantom resting order.

**Rate limiting.** `min_refresh_interval_ms` throttles refreshes so that a
fast-firing reference feed doesn't saturate the broker's REST endpoint between
settle intervals. The next event after the cooldown picks up the latest mid.

**Reference staleness guard.** When in fair-value mode, the actor refuses to
refresh quotes if the reference feed hasn't updated within `reference_stale_secs`.
This prevents quoting around a stale mid while the local book has moved.

## Configuration

| Field | Default | Description |
|---|---|---|
| `gamma` | required | Risk aversion γ — higher = more aggressive inventory skew |
| `kappa` | required | Order-flow intensity κ — higher = tighter optimal spread |
| `order_horizon_secs` | `60` | Finite horizon τ — acts as a spread-width tuning knob |
| `vol_window_secs` | `600` | Rolling volatility window |
| `order_size` | required | Base per-quote quantity |
| `max_position` | required | Hard inventory cap |
| `inventory_target` | `0` | Target net position (non-zero shifts bias) |
| `order_levels` | `3` | Number of ladder rungs per side |
| `order_level_spread_bps` | `10` | Outward step per level in bps |
| `order_level_amount_step` | `0` | Size multiplier increment per level |
| `min_half_spread_bps` | `5` | Floor on the inner half-spread |
| `max_half_spread_bps` | `500` | Cap on the inner half-spread |
| `amend_threshold_bps` | `0.5` | Price drift (inner) before amend fires |
| `amend_threshold_step_bps` | `0.5` | Additional threshold per outer level |
| `order_refresh_secs` | `10` | Fallback re-quote cadence (Tick handler) |
| `min_refresh_interval_ms` | `100` | Rate-limit floor between refreshes |
| `reconcile_interval_secs` | `10` | Periodic REST reconciliation interval |
| `reference_exchange` | — | Optional reference venue name |
| `reference_symbol` | — | Symbol on reference venue |
| `reference_market` | `perp` | `perp` or `spot` |
| `reference_stale_secs` | `10` | Max age of reference data before stale |
| `fees.maker_bps` | — | Optional fee-floor validation |

## Tuning intuition

- **γ (gamma):** 0.01–0.1 is typical. Below 0.005 the inventory skew is
  negligible; above 0.1 the spread blows out even at low inventory.
- **κ (kappa):** 1–5 for liquid perps. Higher κ assumes more market-order flow
  arriving at close-to-mid prices, which tightens the spread.
- **τ (order_horizon_secs):** On crypto perps with no natural expiry, this is
  a tuning knob not a real horizon. 30–120s tends to produce reasonable spreads
  at typical volatility.
- **vol_window_secs:** 300–600s smooths intraday noise. Shorter windows react
  faster but produce more spread churn.

## Future work

**Better volatility model.** The current rolling log-return stddev is a
reasonable starting point but doesn't account for bid/ask bounce or microstructure
noise. Realized variance from mid changes (not tick-by-tick) would be less
noisy.

**Perpetual-specific τ calibration.** The finite-horizon term `σ² · τ` should
ideally scale with the 8h funding cycle on perps, not a fixed wall-clock horizon.
A daily EMA of funding rate magnitude could replace `τ` as the inventory-aversion
driver during high-funding regimes.

**GLFT (inventory-cap) variant.** The Guéant-Lehalle-Fernandez-Tapia (2013)
extension imposes a hard inventory cap and derives a `T → ∞` stationary control
that doesn't depend on an artificial horizon. It's more natural for crypto perps
and should produce tighter spreads at the cap boundary than the current binary
`max_position` gate.

**Funding-aware quoting.** When Bullet's perp funding is significantly positive,
an MM holding net-long is implicitly paying a carry cost that the spread may not
cover. Subscribe to `MarkPriceUpdate.funding_rate` and widen the bid (or reduce
bid size) when funding is adverse.

**Backtesting harness.** A fill-accurate replay of L2 snapshots would let
γ/κ/τ be tuned from data rather than intuition.
