# Grid strategy — future work

Practitioner notes on turning the current naive grid into something you'd
want to run against real money. In rough priority order.

## What the current implementation is

- Symmetric geometric / arithmetic levels around mid.
- Fixed `grid_spacing`, fixed `order_size`, fixed `max_position`.
- Hard rebalance when mid drifts past `rebalance_threshold_pct`.
- Maker-only (`PostOnly`) orders.
- No regime filter: runs the same in a flat tape, a trend, or a liquidity hole.

This is enough to demonstrate the mechanics. It is not a profitable
strategy on its own.

## Core economics (the floor)

Grid PnL per round-trip ≈ `spacing − round_trip_fees`.

- If `spacing < 2 × taker_fee`, the strategy cannot be profitable.
- With maker rebates: `spacing < 2 × (maker_fee − rebate)`.
- Every config change should be benchmarked against this floor first.

**Action:** add a startup check that refuses to run if
`grid_spacing * mid_price < N × fee`, with `N` configurable (e.g. 3x).
Log the implied break-even win-rate.

## Volatility-anchored spacing

Fixed `grid_spacing` is the biggest weakness of the current config. The
"right" spacing is a function of realized volatility, not a constant.

- Starting point: `spacing ≈ hourly_σ / num_levels`, i.e. the full grid is
  roughly one stdev wide per hour.
- Tighter than that: whipsaw fills that don't cover fees.
- Wider: orders sit unfilled.

**Action:** new `spacing_mode = "volatility"` that takes `σ_window` (e.g.
`"1h"`) and a `spacing_sigma_multiplier` (e.g. `1.0`) and derives spacing
from trailing realized vol. Needs a rolling price buffer in `GridState`.

## Bias (long / short / neutral)

Current grid is strictly symmetric. In practice the interesting lever is
**asymmetric** layouts:

- **Neutral**: symmetric buy/sell counts, small `max_position`.
  Delta ≈ 0 in expectation. Hardest to make money with — relies purely on
  spread capture.
- **Long-bias**: more or tighter buy levels, wider/fewer sell levels.
  Implicit thesis: "I'm happy accumulating at these prices." Works well on
  assets you'd want to hold anyway.
- **Short-bias** (perps only): opposite. Can also collect funding if
  funding is positive. This is the regime where `funding-arb` and a
  short-bias grid overlap conceptually.

The mental anchor that makes this work: **center the grid on a strike
price you'd be happy to accumulate at**, not on the current mid. This
stops the bot from chasing breakouts and moving its cost basis up.

**Action:** extend config with:

```toml
bias = "neutral" | "long" | "short"
buy_levels = 7      # override num_levels on one side
sell_levels = 3
buy_spacing_mult = 1.0
sell_spacing_mult = 1.5
center_anchor = "mid" | "fixed" | "ema"
center_price = "..."  # only for fixed
```

Start with just `buy_levels` / `sell_levels` asymmetry; the rest can
follow.

## Regime filter (biggest free win)

Grids monetize mean reversion. In a trend, cumulative spread capture
almost always loses to mark-to-market drawdown on accumulated inventory.

Adding a "pause if trending" gate typically adds more to annualized
return than any parameter tuning inside the grid.

**Action:** `trend_filter` config block, e.g.:

```toml
[strategy.grid.trend_filter]
kind = "ema_slope" | "adx"
fast = "5m"
slow = "1h"
# Pause all placement when |slope| > threshold; cancel existing and wait.
pause_slope_threshold_pct = 0.5
```

Cheapest implementation: keep a rolling EMA of mark price; if the slope
exceeds the threshold, cancel the grid and wait for it to flatten again.
No need for a separate signal library.

## Rebalance semantics

Current rebalance aggressively re-centers on mid after any drift past
threshold. That **locks in MTM losses** and churns fees to re-post.

Two things to fix:

1. **Soft rebalance**: instead of canceling the whole grid, cancel only
   orders now outside the intended band and top up on the near side.
   Cheaper on fees, less realize-and-reset noise.
2. **Rebalance cooldown**: require N seconds between rebalances to
   prevent oscillation-driven churn in whippy markets.

**Action:** replace the `compute_levels → cancel_all → place_all` path in
`on_tick` with a diff-based reconciler that issues only the add/cancel
delta. Uses the `client_id`-based level correlation we already have.

## Inventory-aware quoting (Avellaneda-Stoikov lite)

Our "skip side at max_position" is binary and blunt. The better primitive
is to **widen the quote on the leaned side** proportional to current
inventory — the optimal-MM result from Avellaneda-Stoikov.

Rough form:
- `inventory_skew = net_position / max_position`  (∈ [-1, 1])
- buy prices adjusted down by `k * inventory_skew`
- sell prices adjusted up by `k * inventory_skew`

So as you accumulate longs, buys get harder to fill and sells get easier
— you naturally unwind before hitting the cap. Turns the cap from a
binary kill switch into a soft gradient.

**Reference:** Avellaneda & Stoikov (2008), *High-frequency trading in
a limit order book*. The full closed form is overkill; the skew term is
the 80/20.

## Funding awareness (perps)

A long-bias grid paying positive funding bleeds quietly; a short-bias
grid collecting negative funding gets a free carry on top of spread
capture. Ignoring funding on perps is leaving money on the table on one
side and losing it on the other.

**Action:** subscribe to `MarkPrice` (already done) and factor the 8-hour
funding forecast into the bias decision:

- If next-period funding predicted > threshold and positive → prefer
  short-bias grid or neutral.
- If predicted < -threshold → long-bias grid; you're paid to hold.

Simple version: a daily EMA of realized funding, and a config
`funding_bias_threshold_bps` that flips the bias.

## Sizing rule

Per-level size should be derived, not configured:

```
order_size = max_position / (num_levels × expected_avg_lean)
```

`expected_avg_lean` ≈ 3 is a reasonable default (at any moment, ~1/3 of
your grid is leaned one way before reversion). This prevents hitting
`max_position` and pausing the profitable side in normal oscillation.

**Action:** make `order_size` optional; if omitted, derive from
`max_position` / `num_levels` / `expected_avg_lean`.

## Backtesting infrastructure

Every parameter discussion above is hand-waving until we can measure.
The biggest trap in backtesting grid bots is **fill assumption**: naive
backtests assume your PostOnly quote at price X fills whenever the
tape trades through X. Real fills are conditional on market *coming to
you*, not you crossing to market.

**Action:** build a `grid-backtest` binary that:

1. Replays historical L2 snapshots.
2. For each simulated quote, counts a fill only when the opposite side of
   the book actually traded through the quote's price (not just touched).
3. Applies realistic fee schedule.
4. Outputs: fills count, realized PnL, inventory path, max drawdown,
   Sharpe.

Without this, every config change is vibes. With this, the items above
can be ranked and parameter-swept.

## Stress-test checklist (to run before mainnet)

- Mid **gaps** through half the grid in one print (funding hour, ETF
  news, liq cascade). What's the resulting inventory and MTM?
- Fee tier drops a level (exchange schedule change). Does
  `2 × fee > spacing` anywhere?
- Funding flips sign on a long-bias perp grid. How long until spread
  capture is overwhelmed?
- WS drops for 60 seconds mid-session (already handled by reconnect,
  but verify no orphan orders).
- `max_position` hit during a trend — does the pause-then-resume logic
  actually reduce exposure, or does it just wait for a rebalance that
  never comes?

## Reference reading

- Avellaneda & Stoikov, *High-frequency trading in a limit order book*
  (2008). Foundational. Closed-form optimal maker quotes given
  inventory.
- Guéant, Lehalle & Fernandez-Tapia, *Dealing with the inventory risk*
  (2013). Practical extension with inventory-aversion parameters.
- Hummingbot research blog — practitioner-level discussion of grid and
  market-making configurations; readable and concrete.
