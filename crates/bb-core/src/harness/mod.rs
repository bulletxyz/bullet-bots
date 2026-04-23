//! Event-driven trading harness.
//!
//! # Why
//!
//! Trading systems have a common shape that the naive "event enum +
//! `match` in each strategy" approach doesn't scale: N independent data
//! sources × M strategies × K venues, with strict "don't drop events, don't
//! double-count, shut down cleanly" invariants. Once you pass ~3 of any of
//! those dimensions, the per-strategy boilerplate starts to eat the
//! business logic.
//!
//! This harness is the bullet-bots answer. It gives each strategy a focused
//! per-event-type interface, moves every cross-cutting concern (dispatch,
//! lifecycle, shutdown) into one well-tested place, and structurally
//! prevents a whole class of fill-accounting bugs.
//!
//! # Four concepts
//!
//! - **Event** — any `Clone + Debug + Send + 'static` value. One Rust type
//!   per kind of world change. See [`crate::events`] for the canonical set
//!   (`Trade`, `OrderLifecycle`, `BookUpdate`, `MarkPriceUpdate`, `Tick`).
//!
//! - **Feed** — an async task that publishes events of a single type. Feeds
//!   own their upstream (e.g., a WebSocket) and handle their own
//!   reconnection. Exchange adapters expose one feed per event type they
//!   can produce. Implement [`EventFeed<E>`].
//!
//! - **Actor** — a stateful consumer. Every strategy is one actor.
//!   Implements [`Actor`] for lifecycle (`init` / `wind_down` / `status`)
//!   plus [`EventHandler<E>`] once per event type it subscribes to. The
//!   harness guards each actor with a mutex so handler calls never overlap.
//!
//! - **Harness** — the coordinator. Builds the bus, spawns feed tasks,
//!   spawns one task per actor subscription, routes published events to
//!   subscribers, and drives the init → event → wind_down lifecycle with
//!   a clean shutdown model ([`WindDownReason`]).
//!
//! # Canonical-source invariant
//!
//! The framework enforces by construction — via the event-type split —
//! that `Trade` is the only source of position/PnL changes. Adapters that
//! receive the same fill through two channels (e.g., HL's `UserFills` +
//! `OrderUpdates`) must emit it as `Trade` once. Lifecycle transitions
//! (open/filled/cancelled) go through `OrderLifecycle` and never update
//! inventory.
//!
//! # Minimal main
//!
//! ```ignore
//! use std::sync::Arc;
//! use bb_core::harness::{ActorSpec, HarnessBuilder};
//! use bb_core::helpers::TickFeed;
//! use bb_core::events::{Trade, BookUpdate, Tick};
//!
//! let harness = HarnessBuilder::new()
//!     .enable_signal_shutdown()
//!     .wire_broker("bullet", Arc::new(bullet_broker))
//!     .wire_feed_named("bullet-trades", bullet_feeds.trade)
//!     .wire_feed_named("bullet-book",   bullet_feeds.book)
//!     .wire_feed_named("ticks",         TickFeed::every_ms(5000))
//!     .wire_actor(
//!         ActorSpec::new("grid", grid_actor)
//!             .sub::<Trade>()
//!             .sub::<BookUpdate>()
//!             .sub::<Tick>(),
//!     )
//!     .build()?;
//!
//! harness.run().await?;
//! ```
//!
//! `.sub::<E>()` is checked at compile time against the actor's
//! `EventHandler<E>` impl — you can't subscribe to an event your actor
//! can't handle.
//!
//! See `AGENTS.md` and `HACKING.md` at the repo root for a fuller tour.

mod actor;
mod builder;
mod bus;
mod event;
mod feed;
#[allow(clippy::module_inception)]
mod harness;
mod status;
pub mod testing;

pub use actor::{Actor, ActorContext, EventHandler, WindDownReason};
pub use builder::{ActorSpec, HarnessBuilder};
pub use bus::EventBus;
pub use event::Event;
pub use feed::{EventFeed, EventTx, FeedContext, NoSubscribers};
pub use harness::Harness;
