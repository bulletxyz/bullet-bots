use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::extract::State;
use axum::response::Json;
use axum::routing::get;
use serde::Serialize;
use tokio::sync::watch;

/// Shared state for the status API.
#[derive(Clone)]
pub struct StatusState {
    pub strategy_name: String,
    pub symbol: String,
    pub start_time: Instant,
    pub strategy_status: watch::Receiver<serde_json::Value>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct StatusResponse {
    strategy: String,
    symbol: String,
    uptime_secs: u64,
    strategy_status: serde_json::Value,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn status(State(state): State<Arc<StatusState>>) -> Json<StatusResponse> {
    Json(StatusResponse {
        strategy: state.strategy_name.clone(),
        symbol: state.symbol.clone(),
        uptime_secs: state.start_time.elapsed().as_secs(),
        strategy_status: state.strategy_status.borrow().clone(),
    })
}

/// Build the status API router.
pub fn router(state: Arc<StatusState>) -> Router {
    Router::new().route("/health", get(health)).route("/status", get(status)).with_state(state)
}

/// Spawn the status API server on the given port. Returns a `JoinHandle`.
pub fn spawn_server(port: u16, state: Arc<StatusState>) -> tokio::task::JoinHandle<()> {
    let app = router(state);
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
            .await
            .expect("Failed to bind status API port");
        tracing::info!(port, "Status API listening");
        axum::serve(listener, app).await.expect("Status API server error");
    })
}
