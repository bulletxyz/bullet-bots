//! HTTP status API surfaces per-actor snapshots on a configurable port.
//!
//! Wired by `HarnessBuilder::with_status_port(p)`. Each registered actor
//! contributes a name → `BoxStatus` entry; `/status` calls every closure on
//! request and returns the combined JSON. Calls are `try_lock` so a slow
//! handler never blocks the status endpoint.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::extract::{Query, State};
use axum::response::Json;
use axum::routing::get;
use serde::{Deserialize, Serialize};
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

#[derive(Deserialize, Default)]
struct StatusQuery {
    /// When `compact=1`, strip large array fields from each actor's
    /// snapshot — keeps `/status` small for grids/ladders with many
    /// levels. Scalar fields are preserved.
    #[serde(default)]
    compact: u8,
}

async fn status(
    State(state): State<Arc<StatusState>>,
    Query(q): Query<StatusQuery>,
) -> Json<StatusResponse> {
    let mut actors = serde_json::Map::new();
    for (name, snap) in &state.actors {
        let mut value = snap();
        if q.compact != 0 {
            strip_arrays(&mut value);
        }
        actors.insert(name.to_string(), value);
    }
    Json(StatusResponse {
        uptime_secs: state.start_time.elapsed().as_secs(),
        actors: serde_json::Value::Object(actors),
    })
}

/// Recursively drop array-valued fields (and remember how long they were)
/// so the compact view stays O(scalar fields). Simpler than defining a
/// verbose/compact schema per actor — strategies just emit their full
/// snapshot and callers opt into trimming.
fn strip_arrays(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(map) = value {
        let mut elided: HashMap<String, usize> = HashMap::new();
        map.retain(|k, v| {
            if let serde_json::Value::Array(a) = v {
                elided.insert(k.clone(), a.len());
                false
            } else {
                true
            }
        });
        for (k, n) in elided {
            map.insert(format!("{k}_len"), serde_json::json!(n));
        }
    }
}

pub(super) fn spawn_server(
    port: u16,
    state: Arc<StatusState>,
) -> JoinHandle<Result<(), std::io::Error>> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .with_state(state);
    tokio::spawn(async move {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(%addr, "Status API listening");
        axum::serve(listener, app)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(())
    })
}
