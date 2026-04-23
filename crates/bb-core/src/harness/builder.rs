//! `HarnessBuilder` — wires feeds and actors together before run.
//!
//! The builder stores feeds and actor specs type-erased behind small internal
//! traits (`FeedSpawn`, `ActorSpawn`). `build()` returns a `Harness` ready to
//! `run()`. Everything generic lives in these two traits' impls so the user's
//! wiring code stays typed.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::actor::{Actor, ActorContext, EventHandler, WindDownReason};
use super::bus::EventBus;
use super::event::Event;
use super::feed::{EventFeed, EventTx, FeedContext};
use super::harness::{ActorHandle, Harness};
use crate::broker::{Broker, BrokerRegistry};
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
        let feed = self.feed.take().expect("feed consumed twice");
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
        shutdown: CancellationToken,
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
        Self {
            name: name.into(),
            actor: Arc::new(Mutex::new(actor)),
            subscriptions: Vec::new(),
        }
    }

    /// Subscribe this actor to events of type `E`. Requires the actor to
    /// implement `EventHandler<E>`. Each subscription becomes its own task at
    /// run time, guarded by a per-actor mutex so handler calls never overlap.
    #[must_use]
    pub fn sub<E>(mut self) -> Self
    where
        A: EventHandler<E>,
        E: Event,
    {
        self.subscriptions.push(SubscriptionFactory::new::<E>());
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
            ) -> JoinHandle<()>
            + Send,
    >,
}

impl<A: Actor> SubscriptionFactory<A> {
    fn new<E>() -> Self
    where
        A: EventHandler<E>,
        E: Event,
    {
        let spawn_fn = Box::new(
            move |actor: Arc<Mutex<A>>,
                  bus: &EventBus,
                  ctx: Arc<ActorContext>,
                  cancel: CancellationToken| {
                let mut rx = bus.subscribe::<E>();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => break,
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
                                            ctx.request_shutdown();
                                            break;
                                        }
                                    }
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
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
        shutdown: CancellationToken,
    ) -> ActorHandle {
        let ctx = Arc::new(ActorContext::new(self.name.clone(), brokers, shutdown));
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
            ));
        }

        // `init`/`wind_down` drivers. We store the actor Arc + ctx here so the
        // harness can call into them at the right points — `init` before any
        // event reaches the subscription tasks (we can't strictly enforce
        // that ordering with broadcast channels; callers should publish
        // after `harness.run()` is awaited), `wind_down` after subscription
        // tasks have ended.
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
#[derive(Default)]
pub struct HarnessBuilder {
    feeds: Vec<Box<dyn FeedSpawn>>,
    actors: Vec<Box<dyn ActorSpawn>>,
    brokers: Vec<(Arc<str>, Arc<dyn Broker>)>,
    enable_signal: bool,
    status_port: Option<u16>,
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

    /// Enable the HTTP status API on the given port. `None` disables it.
    /// `/status` returns each actor's JSON snapshot keyed by name.
    #[must_use]
    pub fn with_status_port(mut self, port: Option<u16>) -> Self {
        self.status_port = port;
        self
    }

    /// Register a broker (REST/order-placement handle) under a name.
    /// Actors look it up via `cx.brokers().get("<name>")`.
    #[must_use]
    pub fn wire_broker(
        mut self,
        name: impl Into<Arc<str>>,
        broker: Arc<dyn Broker>,
    ) -> Self {
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
        Ok(Harness::new(
            self.feeds,
            self.actors,
            Arc::new(registry),
            self.enable_signal,
            self.status_port,
        ))
    }
}
