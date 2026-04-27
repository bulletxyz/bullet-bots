# Grid trading: landscape and literature review

This document situates the `bb-bots` grid strategy against retail grid bots,
the open-source market-making stack, and the academic literature. The
engineering roadmap lives in [`grid-future-work.md`](./grid-future-work.md); this is the
evidence base behind it.

## 1. Landscape of grid bots

The retail space converged on a similar feature set: a user-chosen
upper/lower bound, arithmetic or geometric spacing, and some range-extension
mechanic (trailing or stop). Volatility-adjusted spacing and true inventory
skew are rare, and live almost exclusively in Hummingbot.

| Feature | bb-bots (this repo) | Hummingbot `pure_market_making` | Hummingbot `avellaneda_market_making` | Hummingbot `grid_strike` (V2) | Pionex Grid / Futures Grid | 3Commas GRID | Binance Spot/Futures Grid | Bitsgap GRID | KuCoin Futures Grid | Freqtrade (community) |
|---|---|---|---|---|---|---|---|---|---|---|
| Level layout | geometric or arithmetic, symmetric | symmetric around reference price | spread around reservation price (A-S) | fixed range, long or short grid | arithmetic or geometric | arithmetic or geometric | arithmetic or geometric | arithmetic or geometric | arithmetic or geometric | strategy-defined |
| Volatility-adjusted spacing | no | no (static `bid_spread` / `ask_spread`) | yes, volatility enters reservation price and optimal spread directly | no (user sets range) | no (user sets range, docs suggest widening in high vol) | no (user sets step; Optimize button sweeps static steps) | no (AI mode suggests static params from history) | no (user sets range) | no | strategy-dependent |
| Long / short / neutral bias | symmetric only | inventory-target biases fills | inventory-target biases fills | long grid or short grid, one-way | spot neutral; futures long OR short bot | directional strategies (Rising/Falling) with buy-only or sell-only grids | spot neutral; futures long / short / neutral | neutral with trailing up/down | long / short / neutral (neutral added 2026) | strategy-defined |
| Trend filter / pause-during-trend | none | `price_ceiling` / `price_floor` only | none built in | take-profit / stop-loss on each executor | take-profit / stop-loss around the range | TradingView-signal filter on pair selection; trailing | global TP / SL | Pump Protection pause when trailing triggers | documentation unclear | custom in strategy code |
| Rebalance / re-center | hard cancel-all on `rebalance_threshold_pct` drift | periodic refresh (`order_refresh_time`) with `hanging_orders` | periodic recompute of gamma / kappa / eta | grid is fixed; executors self-manage | stop when out of range; no in-flight recenter | trailing up (geometric only) and trailing down extend the range | no trailing; bot stops at range edge | trailing up / trailing down | trailing with funding-rate monitoring | custom |
| Inventory skew (A-S style) | binary `max_position` gate | `inventory_skew_enabled` linear skew toward `inventory_target_base_pct` | full A-S reservation-price skew, tunable risk aversion | none (TP/SL per level instead) | none (hard range) | none (hard range) | none | none | none | none |
| Size derived from `max_position` | no, user sets both | no | partially (shape factor η) | user sets `total_amount_quote`, size is derived | user picks grids and investment, bot splits | user picks quote and grids | user picks quote and grids | user picks quote and grids | user picks quote and grids | custom |
| Perps / funding-aware | mark price subscribed but unused | no | no | supports perps connector | futures-grid product, docs discuss funding qualitatively | perps supported, no funding logic | perps supported, no funding logic | perps supported (docs unclear on funding) | explicitly recommends monitoring 8h funding to pick direction | custom |
| Backtest harness | none | none built in | none built in | none built in | none (forward-only) | 120-day backtest plus an Optimize sweep | none | full historical backtest | none | backtest built into Freqtrade core |
| Maker-only / PostOnly | yes (`PostOnly` default) | yes (`order_optimization_enabled`) | yes | yes | maker-only | maker-only | maker-only | maker-only | maker-only | strategy-defined |
| Source / tier | open source (this repo) | open source | open source | open source | free product, closed source | paid (grid, backtest, Optimize) | free (exchange-native) | paid tiers from $29/mo | free (exchange-native) | open source |

Notes and citations:

- Hummingbot `pure_market_making` ships a linear `inventory_skew_enabled` toward `inventory_target_base_pct`; `avellaneda_market_making` implements the 2008 paper parameterised by gamma, kappa, eta ([Hummingbot strategies](https://hummingbot.org/strategies/), [A-S deep dive](https://hummingbot.org/blog/technical-deep-dive-into-the-avellaneda--stoikov-strategy/)).
- Hummingbot [Grid Strike](https://hummingbot.org/blog/strategy-guide-grid-strike/) (V2) is the controller closest to retail "grid bots": fixed range, long or short side, per-level TP/SL via Grid Executors ([source](https://github.com/hummingbot/hummingbot/blob/master/controllers/generic/grid_strike.py)).
- Pionex splits into Spot Grid, Futures Long, Futures Short, Coin-M, and Reverse Grid products ([Pionex Futures Grid](https://support.pionex.com/hc/en-us/articles/45343668185113-Futures-Grid-Bot)).
- 3Commas added trailing down in 2024 (trailing up is geometric-only), runs a 120-day backtest with an Optimize step-sweep, and filters pair selection on TradingView Technicals ([trailing down](https://3commas.io/blog/introducing-trailing-down-for-grid-bots), [automatic backtesting](https://3commas.io/blog/maximize-grid-bot-performance-automatic-backtesting), [pair selection](https://help.3commas.io/en/articles/7931795-grid-bot-choosing-a-strategy-or-a-trading-pair)).
- Binance's AI mode recommends static parameters from history but does not adapt live; arithmetic is pitched for range, geometric for high-vol ([Spot Grid](https://www.binance.com/en/support/faq/what-is-spot-grid-trading-and-how-does-it-work-d5f441e8ab544a5b98241e00efb3a4ab), [Futures Grid AI](https://www.binance.com/en/support/faq/binance-futures-grid-trading-ai-parameters-guide-647b0dba72d145219688b04aa51405fc)).
- Bitsgap pairs trailing with Pump Protection (pauses new orders during detected moves) and ships a first-class backtester ([Advanced GRID settings](https://bitsgap.com/helpdesk/article/10038646989340-Advanced-Bitsgap-GRID-Bot-Settings), [Backtest](https://bitsgap.com/en/helpdesk/article/10023850035612-Backtest-bot-efficiency-analysis)).
- KuCoin Futures Grid is the only major retail product that documents funding-rate awareness as part of direction selection ([KuCoin docs](https://www.kucoin.com/learn/trading-bot/kucoin-futures-grid-bot)).
- Freqtrade has no native grid mode; community tracks the gap in [freqtrade-strategies #245](https://github.com/freqtrade/freqtrade-strategies/issues/245) and [freqtrade #10310](https://github.com/freqtrade/freqtrade/issues/10310) with user-contributed strategies.

Takeaway: the only genuinely differentiated axes across retail products are
trailing/re-centre behaviour and backtest tooling. None do volatility-adjusted
spacing or A-S inventory skew; those live in Hummingbot.

## 2. Practitioner consensus

Points that appear across independent practitioner sources:

1. **Grids only make money in mean-reverting regimes.** Retail and Hummingbot
   writeups converge: grids scalp oscillations and bleed in trends
   ([3Commas overview](https://help.3commas.io/en/articles/7941367-grid-bot-how-it-works-and-why-use-it),
   [Pionex guide](https://www.pionex.com/blog/pionex-grid-bot/),
   [Coinbureau Pionex review](https://coinbureau.com/guides/how-to-copy-grid-bots-on-pionex/)).

2. **Spacing must cover round-trip fees with margin.** Binance Futures
   writeups recommend 2%+ spacing at standard tiers; guides treat this as a
   floor rather than a tuning choice
   ([wundertrading](https://wundertrading.com/journal/en/learn/article/best-grid-bot-settings)).
   Same invariant as our `grid-future-work.md` "Core economics" section.

3. **Trend filtering or re-centering beats spacing tuning.** 3Commas' flagship
   2024 change was trailing down, and Bitsgap bundles trailing with Pump
   Protection; both are sold as the main defence against breakouts
   ([finestel](https://finestel.com/blog/grid-trading-bots/),
   [paybis](https://paybis.com/blog/ai-rule-based-bot-comparison/)).

4. **Inventory skew and range anchoring dominate step selection.** The
   Hummingbot A-S deep dive positions the reservation-price shift as the
   central PnL lever. Retail guides give the softer version: "centre the
   grid on a price you're happy to own," not on spot mid.

5. **Perps grids without funding awareness leak.** KuCoin's docs are explicit
   that 8-hour funding can erase grid PnL if direction and funding disagree
   ([KuCoin](https://www.kucoin.com/learn/trading-bot/kucoin-futures-grid-bot)).
   This is the clearest perps-specific consensus and is not implemented by
   3Commas, Binance, Bitsgap, or Pionex.

Claims we could not verify as consensus: "AI-tuned beats static" (vendor
marketing only), a specific optimal level count, "geometric always beats
arithmetic."

## 3. Academic underpinnings

Retail grid bots are a heuristic; the academic line treats market making as a
stochastic control problem.

**Avellaneda & Stoikov (2008)** poses the single-dealer problem with mid
following Brownian motion and Poisson market-order arrivals whose intensity
depends on quote distance. Under exponential utility, they derive a
closed-form *reservation price* r = s − q·γ·σ²·(T−t) and an *optimal
half-spread* driven by γ, σ, order-flow intensity κ, and horizon
([Quantitative Finance 8(3)](https://www.tandfonline.com/doi/abs/10.1080/14697680701381228),
[Cornell PDF](https://people.orie.cornell.edu/sfs33/LimitOrderBook.pdf)).
The mechanism is inventory skew: as q grows, the quote shifts so the leaned
side is less likely to fill. Hummingbot's `avellaneda_market_making` is the
visible production implementation
([deep dive](https://hummingbot.org/blog/technical-deep-dive-into-the-avellaneda--stoikov-strategy/)).

**Guéant, Lehalle & Fernandez-Tapia (2013)** — *Dealing with the inventory
risk* — generalise A-S by imposing a hard inventory cap. Under the
constraint the HJB reduces to a linear ODE, and quote adjustments have a
well-defined T → ∞ limit, giving stationary rules that don't depend on an
artificial horizon ([arXiv:1105.3115](https://arxiv.org/abs/1105.3115),
[Math. Fin. Econ. 7(4)](https://link.springer.com/article/10.1007/s11579-012-0087-0)).
GLFT is closer to what a production bot needs.

**Cartea, Jaimungal & Penalva (2015)**, *Algorithmic and High-Frequency
Trading* (Cambridge), is the textbook synthesis, extending A-S and GLFT with
adverse selection, order-flow filtering, and alpha signals
([frontmatter](https://assets.cambridge.org/97811070/91146/frontmatter/9781107091146_frontmatter.pdf)).

Retail grid bots implement none of this. Hummingbot is the only widely used
open-source stack that exposes the A-S / GLFT control; its
`pure_market_making` exposes a linear inventory skew as a cheaper
approximation of the A-S result.

## 4. Where `bb-bots` sits

Functionally, the current grid is a stripped-down Hummingbot
`pure_market_making`: symmetric maker quotes on a static step with a binary
pause at `max_position`. No inventory skew, no volatility estimator, no
trend filter, no funding signal, no backtester. Among retail products it sits
roughly at the level of a bare Binance Spot Grid, minus the trailing,
stop-range, and AI parameter suggestions those have had for years.

Three recommendations follow:

- **Do this first (table stakes).** Build the backtester from
  [`grid-future-work.md`](./grid-future-work.md) with a realistic fill model (opposing
  side trading *through* the quote, not just touching). Every retail
  competitor above Pionex ships one; without it, spacing, bias, and trend
  filter choices are unfalsifiable. It gates the ROI of every other item.

- **Differentiated bet.** Make the grid *funding-aware* on perps: flip bias
  on a trailing funding forecast, refuse long-bias through persistent
  positive funding. KuCoin documents funding as a factor but does not act on
  it; 3Commas, Bitsgap, and Binance ignore it entirely. Bullet is a perps
  venue, so funding awareness is the cheapest differentiator relative to
  data we already subscribe to.

- **Don't waste time on.** The full closed-form Avellaneda-Stoikov control.
  The 80/20 is the inventory-skew term; GLFT and Hummingbot's own blog agree
  that the horizon-dependent optimisation adds little in practice for a
  crypto maker without a meaningful T. A linear inventory skew à la
  `pure_market_making` captures the useful part at a fraction of the
  calibration cost.
