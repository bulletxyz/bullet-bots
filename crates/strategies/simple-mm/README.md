# Simple market maker

This is the starter strategy. If you are new to `bullet-bots`, copy this one
before touching Grid, Reference Arb, Funding Arb, or Avellaneda-Stoikov.

## What it does

Quotes one bid and one ask around the current Bullet mid price:

```text
bid = mid * (1 - bid_spread_bps / 10_000)
ask = mid * (1 + ask_spread_bps / 10_000)
```

Every `refresh_secs`, it cancels/replaces quotes whose price drifted by at
least `refresh_threshold_bps`. Fills update inventory from `Trade` events only.
`max_position` blocks the side that would increase exposure past the cap.

## Why it exists

The other strategies are useful references, but they include extra ideas:
static grids, external references, funding rates, volatility estimation, and
multi-level quote ladders. `simple-mm` shows the smallest useful loop:

- cache a book
- compute prices
- place/cancel orders
- track fills
- expose status
- shut down cleanly

## Configuration

| Field | Default | Description |
|---|---|---|
| `exchange` | `bullet` | Broker name |
| `symbol` | required | Trading symbol |
| `bid_spread_bps` | required | Bid distance below mid |
| `ask_spread_bps` | required | Ask distance above mid |
| `order_size` | required | Size per quote |
| `max_position` | required | Absolute inventory cap |
| `refresh_secs` | `5` | Cancel/replace cadence |
| `refresh_threshold_bps` | `1` | Minimum drift before refresh |
| `order_type` | `post_only` | `post_only`, `limit`, or `market` |
| `dry_run` | `false` | Paper quote intent, no broker orders |
| `fees.maker_bps` | — | Optional fee-floor validation |

Example: [`config/simple-mm-example.toml`](../../../config/simple-mm-example.toml).
