# Grid strategy

## What it does

A static level grid bot. N price levels are computed once at startup, uniformly
spaced across `[lower_price, upper_price]`. An `anchor_price` divides the levels
into initial buys (at or below anchor) and sells (above anchor), giving a
configurable long or short bias without requiring a separate config field.

When a level fills, the actor re-arms the adjacent level on the opposite side.
A buy fill at level N re-arms level N+1 as a sell; a sell fill at level N
re-arms N−1 as a buy. Profit per round trip equals `spacing × order_size`.

The strategy does not re-center. Levels are fixed for the lifetime of the run.

**A note on profitability.** Grids make money through spread capture in
mean-reverting markets and lose to mark-to-market inventory accumulation in
trending ones. The economics floor is `spacing > round_trip_fees`. The trend
filter exists precisely because parameter tuning alone cannot fix a grid that
runs in a trending regime.

## State machine

Each level cycles through three states:

```
Dormant → Pending → Active → Dormant
```

- `Dormant`: no resting order. Re-armed by an adjacent fill or on `resume()`.
- `Pending`: needs placement on the next `place_pending_orders` pass.
- `Active`: order is live on the exchange.

Partial fills accumulate `filled_qty` until the full `order_size` is reached,
at which point the level transitions to `Dormant` and the re-arm fires.

## Key design decisions

**PostOnly by default.** All new orders are placed as `PostOnly`. Before
placing, the actor checks `orderbook.would_cross()` and skips any level whose
price would cross the current best bid/ask. This prevents batch rejects from
ruining a full placement pass.

**Trend filter.** An optional dual-EMA divergence filter (`fast_secs` /
`slow_secs`). When the divergence exceeds `pause_divergence_bps`, the grid
calls `suspend_all()` (cancels all resting orders, resets levels to Dormant)
and pauses new placement. When the divergence falls back below threshold, it
calls `resume()` to re-arm every dormant level using the anchor bias.
`suspend_all` resets `filled_qty` on all levels to prevent stale partial-fill
state from carrying across the pause.

**max_position gate.** Placement on a given side is skipped when
`net_position` would exceed `max_position`. This is a binary gate, not an
inventory skew — see future work for a softer version.

**Missing-order reconciliation.** On each tick, the actor queries live orders
(`get_open_orders`) and reconciles against its tracked level state. Active
levels with no matching live order are re-queued as Pending. This catches
orders that disappeared silently (venue rejects PostOnly as taker, WS gap
during placement, etc.).

**Fee floor.** The optional `[fees]` block validates at startup that
`spacing × mid > min_spread_fee_multiplier × round_trip_fees`. The bot refuses
to run if this invariant doesn't hold.

## Configuration

| Field | Default | Description |
|---|---|---|
| `lower_price` | required | Bottom of grid range |
| `upper_price` | required | Top of grid range |
| `num_levels` | required | Number of price levels (≥ 2) |
| `anchor_price` | current mid | Bias anchor: levels ≤ anchor are buys |
| `order_size` | required | Per-level order quantity |
| `order_type` | `PostOnly` | `PostOnly`, `Limit`, or `Market` |
| `max_position` | required | Hard inventory cap per side |
| `fees.maker_bps` | — | Optional fee-floor validation |
| `fees.min_spread_fee_multiplier` | — | Required edge multiple over round-trip fees |
| `trend_filter.fast_secs` | — | Fast EMA window (seconds) |
| `trend_filter.slow_secs` | — | Slow EMA window (seconds) |
| `trend_filter.pause_divergence_bps` | — | Divergence threshold to pause grid |

## Future work

**Volatility-anchored spacing.** Fixed `grid_spacing` is the biggest weakness.
The "right" spacing is `≈ hourly_σ / num_levels` so the full grid spans roughly
one standard deviation per hour. Too tight → whipsaw fills that don't cover
fees. Too wide → orders sit unfilled. Implement a `spacing_mode = "volatility"`
that derives spacing from a trailing realized-vol window.

**Asymmetric bias.** Expose separate `buy_levels` / `sell_levels` counts and
per-side spacing multipliers. Long-bias (more/tighter buys) is a natural fit
for assets you'd want to accumulate. Short-bias on perps can collect positive
funding on top of spread capture. The current anchor mechanism approximates
this but isn't intuitive for operators.

**Inventory-aware quoting (A-S lite).** The current `max_position` gate is
binary. The Avellaneda-Stoikov result says the better primitive is to widen the
quote on the leaned side proportional to current inventory, turning the cap
into a soft gradient. Rough form: `buy_price_adj = k × (net_pos / max_pos)`.
This reduces adverse selection and naturally unwinds inventory without hard
pauses.

**Soft rebalance.** Replace the cancel-all on `resume()` with a diff-based
reconciler that only touches levels outside the new intent — cheaper on fees,
less churn in whippy markets.

**Funding awareness (perps).** A long-bias grid paying positive funding bleeds
quietly; a short-bias grid collecting negative funding gets free carry. Subscribe
to the funding forecast and flip or pause the bias accordingly. KuCoin documents
this as the primary perp-grid tuning lever; no retail product acts on it
programmatically. Bullet is a perps venue so this is the cheapest
differentiator.

**Backtesting infrastructure.** Every parameter choice above is hand-waving
until measurable. Build a `grid-backtest` binary that replays L2 snapshots with
a realistic fill model (opposite side *trading through* the quote, not just
touching it) and outputs realized PnL, inventory path, max drawdown. Without
this, spacing and bias choices are unfalsifiable.

**Stress-test checklist.**
- Mid gaps through half the grid in one print (funding hour, liquidation cascade)
- Fee tier change making `2 × fee > spacing`
- Funding flip on a long-bias perp grid
- 60-second WS drop mid-session (verify no orphan orders via reconcile)
- `max_position` hit during a trend — does pause-then-resume reduce exposure or
  just wait for a rebalance that never comes?
