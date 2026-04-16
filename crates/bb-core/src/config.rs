use serde::Deserialize;

/// Engine-level configuration shared across all strategies.
#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// The primary trading symbol (e.g., "BTC-USD").
    pub symbol: String,

    /// How often to call `Strategy::on_tick()`, in milliseconds.
    #[serde(default = "default_tick_interval")]
    pub tick_interval_ms: u64,

    /// Maximum reconnection delay in milliseconds (exponential backoff caps here).
    #[serde(default = "default_reconnect_max_delay")]
    pub reconnect_max_delay_ms: u64,

    /// Port for the HTTP status API.
    #[serde(default = "default_status_port")]
    pub status_port: u16,
}

fn default_tick_interval() -> u64 {
    5000
}

fn default_reconnect_max_delay() -> u64 {
    60000
}

fn default_status_port() -> u16 {
    3030
}
