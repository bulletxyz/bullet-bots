//! `HarnessBuilder` — wires feeds and actors together before run.
//!
//! The builder stores feeds and actor specs type-erased behind small internal
//! traits (`FeedSpawn`, `ActorSpawn`). `build()` returns a `Harness` ready to
//! `run()`. Everything generic lives in these two traits' impls so the user's
//! wiring code stays typed.

use std::any::TypeId;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::actor::{Actor, ActorContext, EventHandler, WindDownReason};
use super::bus::EventBus;
use super::event::Event;
use super::feed::{EventFeed, EventTx, FeedContext};
use super::harness::{ActorHandle, Harness};
use crate::broker::{Broker, BrokerRegistry};
use crate::clock::{Clock, SystemClock};
use crate::config::EngineConfig;
use crate::error::BotError;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Internal: type-erased feed spawner. One per `(Feed, E)` pair.
pub(super) trait FeedSpawn: Send {
    fn name(&self) -> &str;
    fn spawn(
        self: Box<Self>,
        bus: &EventBus,
        cancel: CancellationToken,
    ) -> JoinHandle<Result<(), BotError>>;
}

struct FeedEntry<F, E: Event> {
    name: Arc<str>,
    feed: Option<Box<F>>,
    _phantom: std::marker::PhantomData<fn() -> E>,
}

impl<F, E> FeedSpawn for FeedEntry<F, E>
where
    F: EventFeed<E>,
    E: Event,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn spawn(
        mut self: Box<Self>,
        bus: &EventBus,
        cancel: CancellationToken,
    ) -> JoinHandle<Result<(), BotError>> {
        let tx = EventTx::new(bus.sender::<E>());
        let ctx = FeedContext::new(cancel);
        let feed = self.feed.take().unwrap_or_else(|| unreachable!("feed entry consumed twice"));
        tokio::spawn(async move { feed.run(tx, ctx).await })
    }
}

/// Internal: type-erased actor spawner. Each `.wire_actor(spec)` stores one.
pub(super) trait ActorSpawn: Send {
    fn name(&self) -> &str;
    fn spawn(
        self: Box<Self>,
        bus: &EventBus,
        brokers: Arc<BrokerRegistry>,
        clock: Arc<dyn Clock>,
        shutdown: CancellationToken,
        actor_failures: mpsc::UnboundedSender<WindDownReason>,
    ) -> ActorHandle;
}

/// Typed actor spec. Produced by [`HarnessBuilder::actor`] (or equivalent) and
/// extended via [`ActorSpec::sub`].
pub struct ActorSpec<A: Actor> {
    name: Arc<str>,
    actor: Arc<Mutex<A>>,
    subscriptions: Vec<SubscriptionFactory<A>>,
}

impl<A: Actor> ActorSpec<A> {
    pub fn new(name: impl Into<Arc<str>>, actor: A) -> Self {
        Self { name: name.into(), actor: Arc::new(Mutex::new(actor)), subscriptions: Vec::new() }
    }

    /// Subscribe this actor to events of type `E`. Requires the actor to
    /// implement `EventHandler<E>`. Each subscription becomes its own task at
    /// run time, guarded by a per-actor mutex so handler calls never overlap.
    ///
    /// Use [`sub_critical`] for loss-sensitive events (`Trade`,
    /// `OrderLifecycle`) where a lagged subscriber is a correctness error.
    #[must_use]
    pub fn sub<E>(mut self) -> Self
    where
        A: EventHandler<E>,
        E: Event,
    {
        self.subscriptions.push(SubscriptionFactory::new::<E>(false));
        self
    }

    /// Like [`sub`], but treats a lagged event stream as fatal: the harness
    /// will shut down if the actor falls behind on this event type. Use for
    /// `Trade` and `OrderLifecycle` subscriptions — a missed fill or lifecycle
    /// update leaves position tracking permanently wrong.
    #[must_use]
    pub fn sub_critical<E>(mut self) -> Self
    where
        A: EventHandler<E>,
        E: Event,
    {
        self.subscriptions.push(SubscriptionFactory::new::<E>(true));
        self
    }
}

/// Internal: closure that, given shared actor/ctx, subscribes to the bus and
/// spawns a handler task. Existing as a separate struct avoids higher-ranked
/// trait-bound headaches on the `.sub::<E>()` call site.
struct SubscriptionFactory<A: Actor> {
    #[allow(clippy::type_complexity)]
    spawn_fn: Box<
        dyn FnOnce(
                Arc<Mutex<A>>,
                &EventBus,
                Arc<ActorContext>,
                CancellationToken,
                mpsc::UnboundedSender<WindDownReason>,
            ) -> JoinHandle<()>
            + Send,
    >,
}

impl<A: Actor> SubscriptionFactory<A> {
    fn new<E>(fatal_on_lag: bool) -> Self
    where
        A: EventHandler<E>,
        E: Event,
    {
        let spawn_fn = Box::new(
            move |actor: Arc<Mutex<A>>,
                  bus: &EventBus,
                  ctx: Arc<ActorContext>,
                  cancel: CancellationToken,
                  actor_failures: mpsc::UnboundedSender<WindDownReason>| {
                let mut rx = bus.subscribe::<E>();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            biased;
                            () = cancel.cancelled() => break,
                            recv = rx.recv() => match recv {
                                Ok(event) => {
                                    let mut guard = actor.lock().await;
                                    if let Err(e) = guard.on_event(event, &ctx).await {
                                        // Severity follows the error's own policy:
                                        //   retryable → WARN (transient, next event retries)
                                        //   otherwise → ERROR (real problem worth attention)
                                        // Fatal errors additionally request shutdown.
                                        if e.is_retryable() {
                                            tracing::warn!(
                                                actor = ctx.actor_name(),
                                                error = %e,
                                                "handler returned retryable error"
                                            );
                                        } else {
                                            tracing::error!(
                                                actor = ctx.actor_name(),
                                                error = %e,
                                                "handler returned error"
                                            );
                                        }
                                        if e.is_fatal() {
                                            let _ = actor_failures.send(WindDownReason::ActorFailed {
                                                actor: ctx.actor_name().to_string(),
                                                error: e.to_string(),
                                            });
                                            ctx.request_shutdown();
                                            break;
                                        }
                                    }
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                    if fatal_on_lag {
                                        tracing::error!(
                                            actor = ctx.actor_name(),
                                            lagged = n,
                                            "actor lagged on critical event stream — shutting down"
                                        );
                                        let _ = actor_failures.send(WindDownReason::ActorFailed {
                                            actor: ctx.actor_name().to_string(),
                                            error: format!(
                                                "lagged on critical event stream by {n} messages"
                                            ),
                                        });
                                        ctx.request_shutdown();
                                        break;
                                    }
                                    tracing::warn!(
                                        actor = ctx.actor_name(),
                                        lagged = n,
                                        "actor lagged on event stream"
                                    );
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            }
                        }
                    }
                })
            },
        );
        Self { spawn_fn }
    }
}

impl<A: Actor> ActorSpawn for ActorSpec<A> {
    fn name(&self) -> &str {
        &self.name
    }

    fn spawn(
        self: Box<Self>,
        bus: &EventBus,
        brokers: Arc<BrokerRegistry>,
        clock: Arc<dyn Clock>,
        shutdown: CancellationToken,
        actor_failures: mpsc::UnboundedSender<WindDownReason>,
    ) -> ActorHandle {
        let ctx = Arc::new(ActorContext::with_clock(self.name.clone(), brokers, clock, shutdown));
        let name = self.name.clone();
        let actor = self.actor;

        let sub_cancel = CancellationToken::new();
        let mut tasks: Vec<JoinHandle<()>> = Vec::with_capacity(self.subscriptions.len());
        for sub in self.subscriptions {
            tasks.push((sub.spawn_fn)(
                Arc::clone(&actor),
                bus,
                Arc::clone(&ctx),
                sub_cancel.clone(),
                actor_failures.clone(),
            ));
        }

        // `init`/`wind_down` drivers. The harness calls `init` before spawning
        // feeds (so the actor is ready before any event arrives) and `wind_down`
        // after all subscription tasks have drained.
        let actor_init = Arc::clone(&actor);
        let ctx_init = Arc::clone(&ctx);
        let init_fn: BoxInit = Box::new(move || {
            Box::pin(async move {
                let mut guard = actor_init.lock().await;
                guard.init(&ctx_init).await
            })
        });

        let actor_wd = Arc::clone(&actor);
        let ctx_wd = Arc::clone(&ctx);
        let wind_down_fn: BoxWindDown = Box::new(move |reason| {
            Box::pin(async move {
                let mut guard = actor_wd.lock().await;
                guard.wind_down(&reason, &ctx_wd).await
            })
        });

        let actor_status = Arc::clone(&actor);
        let status_fn: BoxStatus = Arc::new(move || {
            // Try-lock so `/status` can't block on a slow handler.
            match actor_status.try_lock() {
                Ok(guard) => guard.status(),
                Err(_) => serde_json::json!({ "busy": true }),
            }
        });

        ActorHandle {
            name,
            sub_tasks: tasks,
            sub_cancel,
            init: Some(init_fn),
            wind_down: Some(wind_down_fn),
            status: status_fn,
        }
    }
}

pub(super) type BoxInit = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), BotError>> + Send>;
pub(super) type BoxWindDown =
    Box<dyn FnOnce(WindDownReason) -> BoxFuture<'static, Result<(), BotError>> + Send>;
pub(super) type BoxStatus = Arc<dyn Fn() -> serde_json::Value + Send + Sync>;

/// Builder for a [`Harness`].
pub struct HarnessBuilder {
    feeds: Vec<Box<dyn FeedSpawn>>,
    actors: Vec<Box<dyn ActorSpawn>>,
    brokers: Vec<(Arc<str>, Arc<dyn Broker>)>,
    enable_signal: bool,
    status_bind: Option<SocketAddr>,
    event_capacities: HashMap<TypeId, usize>,
    clock: Arc<dyn Clock>,
}

impl Default for HarnessBuilder {
    fn default() -> Self {
        Self {
            feeds: Vec::new(),
            actors: Vec::new(),
            brokers: Vec::new(),
            enable_signal: false,
            status_bind: None,
            event_capacities: HashMap::new(),
            clock: Arc::new(SystemClock),
        }
    }
}

impl HarnessBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a Ctrl-C handler that triggers graceful shutdown.
    #[must_use]
    pub fn enable_signal_shutdown(mut self) -> Self {
        self.enable_signal = true;
        self
    }

    /// Enable the HTTP status API on the given port, bound to `127.0.0.1`.
    /// `None` disables it. Use [`with_status_bind`] to bind to a different
    /// address (e.g. `0.0.0.0` for remote access, with appropriate firewall
    /// rules — the endpoint exposes positions and `PnL`).
    #[must_use]
    pub fn with_status_port(mut self, port: Option<u16>) -> Self {
        self.status_bind = port.map(|p| SocketAddr::from(([127, 0, 0, 1], p)));
        self
    }

    /// Enable the HTTP status API on an explicit bind address.
    #[must_use]
    pub fn with_status_bind(mut self, addr: SocketAddr) -> Self {
        self.status_bind = Some(addr);
        self
    }

    /// Apply status API settings from [`EngineConfig`].
    ///
    /// `status_bind` wins when present; otherwise `status_port` binds to
    /// `127.0.0.1:<port>`. If neither is set, the status API is disabled.
    #[must_use]
    pub fn with_status_config(self, engine: &EngineConfig) -> Self {
        match engine.status_bind {
            Some(addr) => self.with_status_bind(addr),
            None => self.with_status_port(engine.status_port),
        }
    }

    /// Override the clock used by all actors' [`ActorContext`]. Defaults to
    /// [`SystemClock`]. Inject a [`TestClock`] to write deterministic tests
    /// where time can be advanced programmatically instead of sleeping.
    ///
    /// [`TestClock`]: crate::clock::TestClock
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Override the broadcast channel capacity for a specific event type.
    ///
    /// Use this for high-frequency events where the default 1024-message
    /// buffer is too small. Example: `BookUpdate` on a fast venue may burst
    /// many ticks per second; set it to 8192 so slow actors don't lag.
    ///
    /// Calls after the bus is built have no effect — set capacities before
    /// any wiring calls.
    #[must_use]
    pub fn with_event_capacity<E: Event>(mut self, n: usize) -> Self {
        self.event_capacities.insert(TypeId::of::<E>(), n);
        self
    }

    /// Register a broker (REST/order-placement handle) under a name.
    /// Actors look it up via `cx.brokers().get("<name>")`.
    #[must_use]
    pub fn wire_broker(mut self, name: impl Into<Arc<str>>, broker: Arc<dyn Broker>) -> Self {
        self.brokers.push((name.into(), broker));
        self
    }

    /// Wire a feed under a debug-friendly name.
    #[must_use]
    pub fn wire_feed_named<F, E>(mut self, name: impl Into<Arc<str>>, feed: F) -> Self
    where
        F: EventFeed<E>,
        E: Event,
    {
        self.feeds.push(Box::new(FeedEntry::<F, E> {
            name: name.into(),
            feed: Some(Box::new(feed)),
            _phantom: std::marker::PhantomData,
        }));
        self
    }

    /// Wire an actor spec.
    #[must_use]
    pub fn wire_actor<A: Actor>(mut self, spec: ActorSpec<A>) -> Self {
        self.actors.push(Box::new(spec));
        self
    }

    /// Build the harness. Returns an error if broker names collide.
    /// Zero brokers is allowed — pure-data actors (monitors, tests,
    /// strategies that don't place orders) don't need one.
    pub fn build(self) -> Result<Harness, BotError> {
        let mut registry = BrokerRegistry::new();
        for (name, broker) in self.brokers {
            registry.insert(name, broker)?;
        }
        let bus = EventBus::with_capacities(self.event_capacities);
        Ok(Harness::new(
            self.feeds,
            self.actors,
            Arc::new(registry),
            bus,
            self.clock,
            self.enable_signal,
            self.status_bind,
        ))
    }
}
