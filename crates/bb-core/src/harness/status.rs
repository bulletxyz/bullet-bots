//! HTTP status API surfaces per-actor snapshots on a configurable port.
//!
//! Wired by `HarnessBuilder::with_status_port(Some(p))`. Each registered
//! actor contributes a name → `BoxStatus` entry; `/status` calls every
//! closure on request and returns the combined JSON. Calls are `try_lock`
//! so a slow handler never blocks the status endpoint.

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::extract::State;
use axum::response::Json;
use axum::routing::get;
use serde::Serialize;
use tokio::task::JoinHandle;

use super::builder::BoxStatus;

#[derive(Clone)]
pub(super) struct StatusState {
    pub start_time: Instant,
    pub actors: Vec<(Arc<str>, BoxStatus)>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

#[derive(Serialize)]
struct StatusResponse {
    uptime_secs: u64,
    actors: serde_json::Value,
}

async fn status(State(state): State<Arc<StatusState>>) -> Json<StatusResponse> {
    let mut actors = serde_json::Map::new();
    for (name, snap) in &state.actors {
        actors.insert(name.to_string(), snap());
    }
    Json(StatusResponse {
        uptime_secs: state.start_time.elapsed().as_secs(),
        actors: serde_json::Value::Object(actors),
    })
}

pub(super) fn spawn_server(
    listener: tokio::net::TcpListener,
    state: Arc<StatusState>,
) -> JoinHandle<Result<(), std::io::Error>> {
    let app =
        Router::new().route("/health", get(health)).route("/status", get(status)).with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(())
    })
}
