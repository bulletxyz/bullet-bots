//! `Harness::run()` — the event loop that owns feeds + actors.
//!
//! Responsibilities:
//!   1. Instantiate the bus and spawn actor handler tasks so subscribers exist before feeds start
//!      publishing.
//!   2. Call each actor's `init`.
//!   3. Spawn feed tasks.
//!   4. Wait for one of: Ctrl-C, a feed task failing, an actor requesting shutdown (via
//!      `ActorContext::request_shutdown`), or all feeds finishing cleanly (→ `InputsClosed`).
//!   5. Cancel subscription tasks, let them drain, call `wind_down` on every actor with the reason.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use super::actor::WindDownReason;
use super::builder::{ActorSpawn, BoxInit, BoxStatus, BoxWindDown, FeedSpawn};
use super::bus::EventBus;
use super::status::{StatusState, spawn_server};
use crate::broker::BrokerRegistry;
use crate::clock::Clock;
use crate::error::BotError;

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
    clock: Arc<dyn Clock>,
    enable_signal: bool,
    status_bind: Option<SocketAddr>,
    bus: EventBus,
}

impl Harness {
    pub(super) fn new(
        feeds: Vec<Box<dyn FeedSpawn>>,
        actors: Vec<Box<dyn ActorSpawn>>,
        brokers: Arc<BrokerRegistry>,
        bus: EventBus,
        clock: Arc<dyn Clock>,
        enable_signal: bool,
        status_bind: Option<SocketAddr>,
    ) -> Self {
        Self { feeds, actors, brokers, clock, enable_signal, status_bind, bus }
    }

    /// Run until shutdown.
    #[allow(clippy::too_many_lines)]
    pub async fn run(self) -> Result<WindDownReason, BotError> {
        let shutdown = CancellationToken::new();
        let (actor_failure_tx, mut actor_failure_rx) = mpsc::unbounded_channel();

        // 1. Spawn actor subscription tasks up front so broadcasts have subscribers.
        let mut actor_handles: Vec<ActorHandle> = Vec::with_capacity(self.actors.len());
        for spec in self.actors {
            let name = spec.name().to_string();
            let handle = spec.spawn(
                &self.bus,
                Arc::clone(&self.brokers),
                Arc::clone(&self.clock),
                shutdown.clone(),
                actor_failure_tx.clone(),
            );
            tracing::info!(actor = %name, "actor subscribed");
            actor_handles.push(handle);
        }

        // Status API: bind eagerly so a port-in-use error surfaces before init.
        if let Some(addr) = self.status_bind {
            let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
                crate::error::BotError::config(format!("status server bind {addr}: {e}"))
            })?;
            tracing::info!(%addr, "Status API listening");
            let state = Arc::new(StatusState {
                start_time: Instant::now(),
                actors: actor_handles.iter().map(|h| (h.name.clone(), h.status.clone())).collect(),
            });
            // Detached — the server lives as a background task and is torn
            // down when the tokio runtime exits.
            drop(spawn_server(listener, state));
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

        // 3. Spawn feeds. Each wrapper task carries its own name so join_next can identify the
        //    completed feed without an external index.
        let mut feed_set: JoinSet<(Arc<str>, Result<(), BotError>)> = JoinSet::new();
        for feed in self.feeds {
            let name: Arc<str> = Arc::from(feed.name().to_string());
            let task = feed.spawn(&self.bus, shutdown.clone());
            let feed_name = name.clone();
            feed_set.spawn(async move {
                let result = match task.await {
                    Ok(r) => r,
                    Err(_) => Err(BotError::config(format!("{feed_name} feed panicked"))),
                };
                (feed_name, result)
            });
            tracing::info!(feed = %name, "feed started");
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
            if feed_set.is_empty() {
                break WindDownReason::InputsClosed;
            }
            tokio::select! {
                biased;
                Some(reason) = actor_failure_rx.recv() => {
                    tracing::error!(?reason, "actor failure reported");
                    break reason;
                }
                () = shutdown.cancelled() => {
                    tracing::info!("shutdown requested by actor");
                    break WindDownReason::Signal;
                }
                () = &mut signal_fut => {
                    tracing::info!("Ctrl-C received");
                    break WindDownReason::Signal;
                }
                Some(join_res) = feed_set.join_next() => {
                    match join_res {
                        Ok((name, Ok(()))) => {
                            tracing::info!(feed = %name, "feed exited cleanly");
                        }
                        Ok((name, Err(e))) => {
                            tracing::error!(feed = %name, error = %e, "feed failed");
                            break WindDownReason::FeedFailed {
                                feed: name.to_string(),
                                error: e.to_string(),
                            };
                        }
                        Err(e) => {
                            // The wrapper task itself panicked — shouldn't happen
                            // but guard it anyway.
                            break WindDownReason::FeedFailed {
                                feed: "<unknown>".to_string(),
                                error: format!("feed wrapper panicked: {e}"),
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
