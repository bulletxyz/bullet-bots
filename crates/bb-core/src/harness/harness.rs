//! `Harness::run()` — the event loop that owns feeds + actors.
//!
//! Responsibilities:
//!   1. Instantiate the bus and spawn actor handler tasks so subscribers exist
//!      before feeds start publishing.
//!   2. Call each actor's `init`.
//!   3. Spawn feed tasks.
//!   4. Wait for one of: Ctrl-C, a feed task failing, an actor requesting
//!      shutdown (via `ActorContext::request_shutdown`), or all feeds
//!      finishing cleanly (→ `InputsClosed`).
//!   5. Cancel subscription tasks, let them drain, call `wind_down` on every
//!      actor with the reason.

use std::sync::Arc;
use std::time::Instant;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::actor::WindDownReason;
use super::builder::{ActorSpawn, BoxInit, BoxStatus, BoxWindDown, FeedSpawn};
use super::bus::EventBus;
use super::status::{StatusState, spawn_server};
use crate::broker::BrokerRegistry;
use crate::error::BotError;

type FeedTask = (Arc<str>, JoinHandle<Result<(), BotError>>);

pub(super) struct ActorHandle {
    pub(super) name: Arc<str>,
    pub(super) sub_tasks: Vec<JoinHandle<()>>,
    pub(super) sub_cancel: CancellationToken,
    pub(super) init: Option<BoxInit>,
    pub(super) wind_down: Option<BoxWindDown>,
    pub(super) status: BoxStatus,
}

pub struct Harness {
    feeds: Vec<Box<dyn FeedSpawn>>,
    actors: Vec<Box<dyn ActorSpawn>>,
    brokers: Arc<BrokerRegistry>,
    primary_broker: Arc<str>,
    enable_signal: bool,
    status_port: Option<u16>,
    bus: EventBus,
}

impl Harness {
    pub(super) fn new(
        feeds: Vec<Box<dyn FeedSpawn>>,
        actors: Vec<Box<dyn ActorSpawn>>,
        brokers: Arc<BrokerRegistry>,
        primary_broker: Arc<str>,
        enable_signal: bool,
        status_port: Option<u16>,
    ) -> Self {
        Self {
            feeds,
            actors,
            brokers,
            primary_broker,
            enable_signal,
            status_port,
            bus: EventBus::new(),
        }
    }

    /// Run until shutdown.
    pub async fn run(self) -> Result<WindDownReason, BotError> {
        let shutdown = CancellationToken::new();

        // 1. Spawn actor subscription tasks up front so broadcasts have subscribers.
        let mut actor_handles: Vec<ActorHandle> = Vec::with_capacity(self.actors.len());
        for spec in self.actors {
            let name = spec.name().to_string();
            let handle = spec.spawn(
                &self.bus,
                Arc::clone(&self.brokers),
                Arc::clone(&self.primary_broker),
                shutdown.clone(),
            );
            tracing::info!(actor = %name, "actor subscribed");
            actor_handles.push(handle);
        }

        // Status API: spawn only once the actor snapshots exist.
        if let Some(port) = self.status_port {
            let state = Arc::new(StatusState {
                start_time: Instant::now(),
                actors: actor_handles
                    .iter()
                    .map(|h| (h.name.clone(), h.status.clone()))
                    .collect(),
            });
            // Detached — the server lives as a background task and is torn
            // down when the tokio runtime exits.
            drop(spawn_server(port, state));
        }

        // 2. Call each actor's init.
        for handle in &mut actor_handles {
            if let Some(init) = handle.init.take() {
                tracing::info!(actor = %handle.name, "actor init");
                if let Err(e) = init().await {
                    tracing::error!(actor = %handle.name, error = %e, "actor init failed");
                    let reason = WindDownReason::ActorFailed {
                        actor: handle.name.to_string(),
                        error: e.to_string(),
                    };
                    return wind_down_all(actor_handles, reason, shutdown).await;
                }
            }
        }

        // 3. Spawn feeds.
        let mut feed_tasks: Vec<FeedTask> = Vec::new();
        for feed in self.feeds {
            let name: Arc<str> = Arc::from(feed.name().to_string());
            let task = feed.spawn(&self.bus, shutdown.clone());
            tracing::info!(feed = %name, "feed started");
            feed_tasks.push((name, task));
        }

        // 4. Wait for a shutdown-inducing event.
        let signal_fut = async {
            if self.enable_signal {
                let _ = tokio::signal::ctrl_c().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(signal_fut);

        let reason: WindDownReason = loop {
            if feed_tasks.is_empty() {
                break WindDownReason::InputsClosed;
            }
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::info!("shutdown requested by actor");
                    break WindDownReason::Signal;
                }
                _ = &mut signal_fut => {
                    tracing::info!("Ctrl-C received");
                    break WindDownReason::Signal;
                }
                result = await_any_feed(&mut feed_tasks) => {
                    match result {
                        FeedExit::Done(name) => {
                            tracing::info!(feed = %name, "feed exited cleanly");
                        }
                        FeedExit::Err(name, e) => {
                            tracing::error!(feed = %name, error = %e, "feed failed");
                            break WindDownReason::FeedFailed {
                                feed: name.to_string(),
                                error: e.to_string(),
                            };
                        }
                        FeedExit::Panicked(name) => {
                            break WindDownReason::FeedFailed {
                                feed: name.to_string(),
                                error: "panicked".to_string(),
                            };
                        }
                    }
                }
            }
        };

        wind_down_all(actor_handles, reason, shutdown).await
    }
}

/// Cancel subscriptions, drain handler tasks, call `wind_down` on every actor.
async fn wind_down_all(
    actor_handles: Vec<ActorHandle>,
    reason: WindDownReason,
    shutdown: CancellationToken,
) -> Result<WindDownReason, BotError> {
    shutdown.cancel();
    for h in &actor_handles {
        h.sub_cancel.cancel();
    }
    for mut h in actor_handles {
        for task in h.sub_tasks.drain(..) {
            let _ = task.await;
        }
        if let Some(wd) = h.wind_down.take() {
            tracing::info!(actor = %h.name, ?reason, "actor wind_down");
            if let Err(e) = wd(reason.clone()).await {
                tracing::error!(actor = %h.name, error = %e, "wind_down error");
            }
        }
    }
    Ok(reason)
}

enum FeedExit {
    Done(Arc<str>),
    Err(Arc<str>, BotError),
    Panicked(Arc<str>),
}

/// Await the first task in `tasks` to finish, remove it, and return its exit.
///
/// Under the hood we poll each `JoinHandle` once per wakeup. `JoinHandle`
/// registers the waker on each poll, so when any task completes the runtime
/// wakes us and we return — no busy loop, no per-tick CPU cost while all
/// feeds are idle. For small N (one muxer feed per event type per venue —
/// typically < 10) this beats the ceremony of `FuturesUnordered` + rehydrate
/// the survivors.
async fn await_any_feed(tasks: &mut Vec<FeedTask>) -> FeedExit {
    use std::task::Poll;
    use futures_util::future::poll_fn;

    let (idx, res) = poll_fn(|cx| {
        for (i, (_, task)) in tasks.iter_mut().enumerate() {
            if let Poll::Ready(res) = std::pin::Pin::new(task).poll(cx) {
                return Poll::Ready((i, res));
            }
        }
        Poll::Pending
    })
    .await;

    let (name, _) = tasks.swap_remove(idx);
    match res {
        Ok(Ok(())) => FeedExit::Done(name),
        Ok(Err(e)) => FeedExit::Err(name, e),
        Err(_) => FeedExit::Panicked(name),
    }
}

