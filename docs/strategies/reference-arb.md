# Reference-price arbitrage

## What it does

Monitors the spread between Bullet's mid price and Binance's perpetual
microprice. When Bullet diverges far enough from the reference for long enough,
the strategy takes a directional position on Bullet that profits from convergence.

```
spread_bps = (bullet_mid − binance_mid) / binance_mid × 10_000
```

- **Bullet rich** (spread > 0): short Bullet, expect it to fall toward Binance.
- **Bullet cheap** (spread < 0): long Bullet, expect it to rise toward Binance.

Exit triggers:
- **Take-profit:** spread reverts past `exit_threshold_bps` on the entry side.
- **Stop-loss:** spread widens past `stop_loss_bps`.
- **Timeout:** position held for `max_hold_ticks` without hitting TP or SL.

## State machine

```
Flat → Entering → Holding → Exiting → Flat
```

- **Flat:** Accumulating signal streak via persistence filter.
- **Entering:** Market order placed; waiting for fill Trade event.
- **Holding:** Position live; evaluating TP/SL on every price update.
- **Exiting:** Closing market order placed; waiting for fill.

If an exit order is cancelled, the actor drops back to `Holding` with the
tick count preserved (`ticks_at_exit`), so `max_hold_ticks` remains a hard
bound even across repeated failed exits.

## Key design decisions

**Persistence filter.** A single spread observation above threshold could be a
WS latency gap, a book-snapshot artifact, or a momentary wide bid/ask. Requiring
N consecutive qualifying evaluations (`persistence_ticks`) before entry filters
single-tick spikes. Both `BookUpdate` and `ReferencePriceUpdate` trigger
`evaluate()`, so the streak accumulates across whichever feed fires first.

**Staleness guard.** Binance data older than `reference_stale_secs` is rejected.
The guard uses the actual `received_at` timestamp (not the event sequence) and
defaults to treating no-data-seen as stale. A single stale warning is logged
per episode; subsequent ticks are suppressed to avoid flooding.

**Dry-run mode.** `dry_run = true` paper-trades on live data: entry and exit
fills are simulated against the current book (or mid fallback) rather than sent
to the broker. PnL is tracked and logged identically to a live run. Use this
to validate signal quality before committing capital.

**Fee-floor validation.** At config parse time, `min_edge_multiple` is checked
against the round-trip taker fee. `entry_threshold_bps` must exceed
`min_edge_multiple × 2 × taker_fee_bps`. The bot refuses to start on a config
that can't be profitable after fees.

**Aggressive IoC pricing.** Bullet's `Market` order type is an IoC limit. The
actor computes a worst-case price of `mid ± market_slippage_bps` so the venue
accepts the order; actual fills occur at or better than the current top of book.

**Monotonic hold timeout.** When an exit order is cancelled and the actor
re-enters `Holding`, it restores the tick count from before the exit attempt
(`ticks_at_exit`). Without this, repeated failed exits would reset the counter
to zero on each cancel, allowing the position to be held indefinitely.

**Wind-down flatten.** On any shutdown reason (signal, feed failure, actor
error), if a non-zero position is held, the actor attempts a closing market
order. The earlier check that skipped the flatten on `FeedFailed` has been
removed — exit orders are priced from the Bullet book, not the Binance
reference, so Binance staleness is irrelevant.

## Configuration

| Field | Default | Description |
|---|---|---|
| `exchange` | required | Trading venue (e.g. `"bullet"`) |
| `symbol` | required | Symbol on trading venue (e.g. `"BTC-USD"`) |
| `binance_symbol` | required | Symbol on Binance (e.g. `"btcusdt"`) |
| `binance_market` | `perp` | `perp` or `spot` |
| `order_size` | required | Order quantity per entry |
| `max_position` | required | Hard position cap (defense-in-depth) |
| `entry_threshold_bps` | required | Min spread to trigger signal streak |
| `exit_threshold_bps` | required | TP threshold (must be < entry) |
| `stop_loss_bps` | required | SL threshold (must be > entry) |
| `persistence_ticks` | `2` | Consecutive qualifying evals before entry |
| `max_hold_ticks` | `24` | Force-exit after this many ticks |
| `reference_stale_secs` | `10` | Max Binance data age before rejection |
| `taker_fee_bps` | required | Bullet taker fee for fee-floor check |
| `min_edge_multiple` | `1.5` | Required edge multiple over round-trip fees |
| `market_slippage_bps` | `50` | Worst-case IoC bound above/below mid |
| `dry_run` | `false` | Paper trade mode (no real orders) |

## Future work

**Multi-reference aggregation.** Using a single reference exchange creates
exposure to Binance-specific events (outages, funding-window spikes). A median
or VWAP across two or three CEX references would be more robust.

**Adaptive persistence.** In low-latency environments `persistence_ticks = 1`
might be appropriate; during periods of high WS jitter, `3–5` is safer. A
rolling measurement of inter-event latency variance could auto-tune the threshold.

**Partial fills on entry.** The current model assumes the market order fills
in full on entry. On illiquid books, a partial fill transitions to `Holding`
with a smaller position than intended, which affects exit sizing. Track and
handle partial entry fills explicitly.

**A-S integration.** The reference price can serve as the fair-value mid for
the Avellaneda-Stoikov market maker (already wired via `reference_exchange` in
the A-S config). Reference-arb and A-S can run in complementary modes:
A-S quotes around the reference for steady-state, reference-arb takes
directional shots on larger divergences.

**Backtesting harness.** Spread data (Bullet mid vs. Binance) replayed with
simulated fill latency would let `persistence_ticks`, thresholds, and
`max_hold_ticks` be tuned from history rather than intuition. Entry and exit
timing relative to the convergence path is the main PnL driver.
