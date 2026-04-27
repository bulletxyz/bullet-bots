# Funding rate arbitrage

## What it does

A cross-venue delta-neutral strategy. It shorts the perpetual with the higher
funding rate and longs the perpetual with the lower funding rate on a second
venue. The position collects the funding rate differential at each payment
window while holding approximately zero directional exposure.

**Example:** Bullet pays 0.05%/hr long funding, Hyperliquid pays 0.01%/hr.
The strategy shorts Bullet (pays no funding as short) and longs Hyperliquid
(receives 0.01%/hr). Net carry = 0.05% − 0.01% = 0.04%/hr before fees.

Entry and exit are governed by `entry_threshold` and `exit_threshold` expressed
as per-hour funding rate differentials.

## State machine

```
Flat → Entering → Active → Exiting → Flat
```

- **Flat:** Monitoring funding rates. Entry fires when spread exceeds
  `entry_threshold` and both venues have reported at least one real funding rate
  (the default-zero rate from startup is not treated as a valid signal).
- **Entering:** Both entry orders have been placed; waiting for fills. The actor
  transitions to `Active` once `both_legs_fully_entered()` is satisfied. If the
  timeout (`phase_timeout_secs`) expires, emergency flatten fires.
- **Active:** Collecting funding. On each `Tick`, the delta imbalance is checked
  against `max_delta_imbalance`. If the net delta exceeds the threshold (partial
  fill asymmetry), emergency flatten fires. Exit triggers when spread falls below
  `exit_threshold`.
- **Exiting:** Close orders placed on both legs; waiting for fills. On fill,
  transitions to `Flat` and increments `cycles_completed`. Same timeout
  protection as Entering.

## Key design decisions

**Rate guard (`has_rate_a` / `has_rate_b`).** The default value for cached
funding rates is `Decimal::ZERO`. A venue that sends mark-price-only updates
(e.g., Hyperliquid `AllMids`) would leave its rate at zero, making a real rate
on the other venue look like a 100% spread. The actor tracks whether each venue
has sent at least one `funding_rate: Some(...)` and blocks entry until both
flags are set.

**Entry failure recovery.** Orders on both legs are placed via `tokio::join!`.
If either placement returns an error or `success: false`, the actor issues
`cancel_all_orders` on both venues (to clean up any order that may have landed
on one side) and transitions back to `Flat`. The min-flat-hold cooldown then
applies before the next re-entry attempt.

**Emergency flatten semantics.** When emergency flatten fires, the actor attempts
market close orders on both venues. If any flatten order fails, it calls
`cx.request_shutdown()` to surface the incomplete flatten to the operator —
the position cannot be assumed closed. State transitions to `Flat` regardless
(staying in `Entering` would cause more entries on top of an open position), but
the shutdown signal ensures the operator can intervene.

**`min_flat_hold_secs` cooldown.** After any exit (normal or emergency), entry
is blocked for at least `min_flat_hold_secs`. This guards against rapid
re-entry when the spread wobbles around the threshold immediately after closing.

**Per-venue inventory tracking.** Each venue has its own `InventoryTracker`.
Fills are accepted only when the `client_id` of the trade matches one the actor
issued — external fills from other bots or manual orders on the same wallet are
ignored. This prevents the double-count bug that plagued pre-harness versions.

**WS reconnect reconciliation.** On any WS reconnect signal from a broker, the
actor calls `get_positions()` and resets the `InventoryTracker` to the actual
on-venue position if they diverge. This corrects any inventory drift from events
missed during the disconnect window.

## Configuration

| Field | Default | Description |
|---|---|---|
| `symbol` | required | Trading symbol (e.g. `"BTC-USD"`) |
| `exchange_a` | required | First venue name |
| `exchange_b` | required | Second venue name |
| `entry_threshold` | required | Min funding differential to enter (per hour) |
| `exit_threshold` | required | Exit when differential drops below this |
| `order_size` | required | Base asset size per leg |
| `max_delta_imbalance` | `0.005` | Max net delta before emergency flatten |
| `max_funding_rate` | `0.005` | Reject rates above this (anomaly filter) |
| `order_mode` | `aggressive` | `aggressive` (IoC) or `passive` (PostOnly) |
| `phase_timeout_secs` | `30` | Timeout before emergency flatten on Entering/Exiting |
| `slippage` | `0.001` | Price slippage for aggressive orders (fraction) |
| `min_flat_hold_secs` | `60` | Cooldown in Flat before re-entry |

Config is validated at startup: `exit_threshold < entry_threshold`,
`order_size > 0`, `max_delta_imbalance > 0`, `slippage ≥ 0`.

## Future work

**Passive entry mode tuning.** The `Passive` order mode (PostOnly) reduces
entry cost via maker rebates but risks partial fills, leaving a one-sided
position longer. A hybrid approach — aggressive on entry, passive on exit when
spread has collapsed — may improve net economics.

**Pre-window entry.** The current strategy reacts to the current funding rate.
Funding is predictable near the payment window (8h on most perps) — entering
30 minutes before the window collects the payment with less competition. A
simple `time_to_funding` input from the MarkPriceUpdate feed would enable this.

**Multi-leg support.** Three or more venues with a common symbol could be arbed
in rotation: short the highest, long the lowest, dynamically reallocated as
rates shift. Requires a more general leg model than the current `leg_a`/`leg_b`.

**Startup position reconciliation.** Currently, the actor cancels stale orders
at startup but does not check for existing positions. A pre-existing position on
one venue (e.g., from a prior run that crashed during Exiting) would corrupt
the inventory tracker. Add a `get_positions` check at `init()` and refuse to
start on a non-flat position, or resume into `Active` if both legs look balanced.

**Funding forecast integration.** Use a trailing EMA of realized funding to
predict the next window's rate. If the forecast suggests the spread will close
before the payment, skip the entry. This reduces cycles that pay two round-trips
to collect a shrinking differential.
