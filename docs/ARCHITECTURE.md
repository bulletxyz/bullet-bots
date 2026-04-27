# bullet-bots — Architecture

This document describes the runtime model: how events flow from venue WebSockets
through the harness to strategy actors, and how actors place orders back through
the broker layer.

## Overview

```mermaid
graph TD
    WS["Exchange WebSocket\n(connection.rs demux)"]
    WS -->|mpsc| FT["EventFeed&lt;Trade&gt;"]
    WS -->|mpsc| FB["EventFeed&lt;BookUpdate&gt;"]
    WS -->|mpsc| FL["EventFeed&lt;OrderLifecycle&gt;"]
    WS -->|mpsc| FM["EventFeed&lt;MarkPriceUpdate&gt;"]
    BinanceWS["Binance WebSocket"] -->|mpsc| FR["EventFeed&lt;ReferencePriceUpdate&gt;"]

    FT -->|tx.send| Bus["EventBus\nbroadcast::Sender&lt;E&gt;\none channel per type"]
    FB -->|tx.send| Bus
    FL -->|tx.send| Bus
    FM -->|tx.send| Bus
    FR -->|tx.send| Bus

    Bus -->|broadcast| S1["sub task\nActor A — Trade"]
    Bus -->|broadcast| S2["sub task\nActor A — Book"]
    S1 -->|Mutex| A["Actor handler\nserialised"]
    S2 -->|Mutex| A
    A -->|REST| Broker["Broker"]
```

## The three primitives

### EventFeed\<E\>

A feed owns one upstream connection and publishes events of a single type `E`.
Feeds are typed: `BulletTradeFeed` only publishes `Trade`, `BulletBookFeed`
only publishes `BookUpdate`, etc. This makes the type of data each feed
produces self-documenting and checked at compile time.

The trait is minimal:

```rust
#[async_trait]
pub trait EventFeed<E: Event>: Send + 'static {
    async fn run(self: Box<Self>, tx: EventTx<E>, cx: FeedContext) -> Result<(), BotError>;
}
```

A feed exits by returning from `run`. On clean exit, the harness records it;
once all feeds exit, it enters `InputsClosed` shutdown. On error, the harness
immediately initiates shutdown with `FeedFailed`.

### EventBus

The bus is a map of `TypeId → broadcast::Sender<Box<dyn Any>>`. One broadcast
channel per event type. When a feed sends an event the harness fans it out to
every subscriber of that type in parallel.

Actors subscribe before feeds start, so no event is ever sent to an empty bus.

### Actor

An actor holds state and implements `EventHandler<E>` for each event type it
cares about. The harness guards each actor with a `Mutex` so handler calls
never overlap — even if two different event types arrive at the same instant,
only one handler runs at a time. Internal state is therefore safe to mutate
without any additional synchronization.

```mermaid
stateDiagram-v2
    [*] --> Running: harness.run() calls init()
    Running --> WindingDown: shutdown triggered
    WindingDown --> [*]: wind_down(reason) returns

    note right of Running
        on_event(E1)
        on_event(E2)   serialised by Mutex
        on_event(E1)
        ...
    end note

    note right of WindingDown
        reason: Signal | FeedFailed | InputsClosed
        cancel orders, flatten, or just log
    end note
```

`wind_down` is called with the `WindDownReason` so actors can decide: cancel
all orders on `Signal`, flatten positions on `FeedFailed`, or just log on
`InputsClosed`.

## Component diagram

```mermaid
graph TD
    BB["<b>bb-core</b><br/>HarnessBuilder · Harness<br/>EventBus · BrokerRegistry<br/>Clock · ActorContext<br/>Helpers: InventoryTracker, ClientIdIssuer, TickFeed"]

    EX["<b>exchanges/</b><br/>bullet · hyperliquid · binance<br/>EventFeed&lt;E&gt; impls<br/>Broker impl · config.rs · convert.rs"]

    ST["<b>strategies/</b><br/>grid · avellaneda-stoikov<br/>reference-arb · funding-arb<br/>Actor impl · EventHandler&lt;E&gt; impl · config.rs"]

    BOT["<b>bb-bot</b><br/>main.rs<br/>CLI entry — reads TOML<br/>dispatches to run_harness_*"]

    EX -->|implements traits from| BB
    ST -->|implements traits from| BB
    EX -->|wired by| BOT
    ST -->|wired by| BOT
```

## Event types and their producers

| Event                  | Canonical meaning                                  | Produced by              |
|------------------------|----------------------------------------------------|--------------------------|
| `Trade`                | Our account got a fill. Update position/PnL here.  | Bullet, Hyperliquid      |
| `OrderLifecycle`       | Order state change. Reconcile only — never PnL.    | Bullet, Hyperliquid      |
| `BookUpdate`           | Orderbook snapshot or delta.                       | Bullet, Hyperliquid      |
| `MarkPriceUpdate`      | Mark price and/or funding rate.                    | Bullet, Hyperliquid      |
| `Tick`                 | Periodic heartbeat. Drives timed strategy work.    | `TickFeed` (framework)   |
| `ReferencePriceUpdate` | External reference price (Binance microprice).     | `bb-exchange-binance`    |

**Key invariant:** `Trade` is the *only* source of position and realized-PnL
changes. `OrderLifecycle::Filled` is never used for this purpose, even though
it also signals a fill. Some adapters emit both for the same execution (HL's
`UserFills` + `OrderUpdates`); crediting both would double-count. The split is
enforced by convention in every strategy.

## Shutdown flow

```mermaid
flowchart TD
    A["Ctrl-C / request_shutdown()"] --> C["cancel all sub tasks\ndrain in-flight events"]
    B["feed returns Err"] --> C
    D["all feeds return Ok"] --> E["InputsClosed reason"]
    F["actor init returns Err"] --> G["ActorFailed reason"]

    C --> H["await sub tasks\ndrop events in transit"]
    E --> H
    G --> H
    H --> I["wind_down(reason)\ncalled on every actor\nin registration order"]
    I --> J["Harness::run() returns\nOk(WindDownReason)"]
```

## Broker contract

```
place_orders(&[NewOrder]) → Result<Vec<OrderResult>, BotError>
```

- `Err(e)` — transport / system failure. The whole call failed; no orders were
  submitted. Retryable errors (`e.is_retryable()`) may be retried by the actor;
  non-retryable errors should trigger shutdown.
- `Ok(results)` — call reached the venue. Each `OrderResult` has `success:
  bool` — `false` means venue-level rejection of that one order (e.g.,
  insufficient margin, price out of bounds). Other orders in the batch may
  have succeeded.
- `order_id: Option<String>` — `Some(id)` when the venue confirmed an ID
  synchronously; `None` when the outcome is unknown until the lifecycle stream
  confirms.

`amend_orders` defaults to sequential cancel-then-place. Bullet and (TODO) HL
override this with native atomic amend.

## Adapter layout

Every exchange adapter follows the same four-file structure:

```
connection.rs   — owns the WebSocket, demuxes into typed mpsc channels,
                  exposes a <Name>Feeds bundle of EventFeed<E> impls.
broker.rs       — implements bb_core::broker::Broker for REST (place, cancel, query).
convert.rs      — value conversions between venue wire types and bb-core types.
config.rs       — TOML-derived adapter config (auth, network, symbol).
```

The `connection.rs` demux loop pattern:

```mermaid
flowchart TD
    WS["WebSocket message"] --> P["parse JSON"]
    P --> M{"match message type"}
    M -->|trade update| T["convert::order_update_to_trade()\n→ trade_tx.send()"]
    M -->|book update| Bo["convert::depth_to_event()\n→ book_tx.send()"]
    M -->|lifecycle| L["convert::order_update_to_lifecycle()\n→ lifecycle_tx.send()"]
    M -->|mark price| Mp["convert::mark_price_to_event()\n→ mark_tx.send()"]

    T & Bo & L & Mp --> F["MpscFeed&lt;E&gt;(rx)\nimplements EventFeed&lt;E&gt;"]
```

`convert.rs` is the only place that touches venue-specific field names and
wire formats. `connection.rs` just routes; `broker.rs` just calls REST.

## Testing model

```mermaid
graph TD
    SF["ScriptedFeed&lt;E&gt;\nsends preset events"] -->|events| Bus["EventBus"]
    Bus --> A["Actor"]
    A -->|"cx.brokers().require('bullet')"| MB["MockBroker\nrecords calls\nreturns queued responses"]
    A --> AS["assert:\nplaced_count()\nlast_placed_orders()\n..."]
```

For time-sensitive tests, swap `ScriptedFeed` for `MarketDataReplayFeed<E>` and
inject a `TestClock` via `HarnessBuilder::with_clock`. The feed advances the
clock to each event's timestamp before sending, so strategies see event-driven
time instead of wall-clock time.

See `crates/strategies/reference-arb/src/strategy.rs` for a full example of
the Flat → Entering → Holding → Exiting → Flat state machine driven end-to-end
through a scripted harness test.
