use std::net::SocketAddr;

use serde::Deserialize;

/// Semantic validation for parsed config structs.
///
/// TOML/serde parsing answers "is the shape right?". `ValidateConfig` answers
/// "do these values make sense together?".
pub trait ValidateConfig {
    fn validate(&self) -> Result<(), String>;
}

/// Engine-level configuration shared across all strategies.
///
/// Symbol is deliberately *not* here — each strategy owns its own trading
/// symbol, which keeps multi-symbol setups clean and stops the symbol from
/// being an invisible global.
#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// How often `TickFeed` fires, in milliseconds.
    #[serde(default = "default_tick_interval")]
    pub tick_interval_ms: u64,

    /// Port for the HTTP status API. Omit from config to disable the server.
    #[serde(default)]
    pub status_port: Option<u16>,

    /// Explicit bind address for the HTTP status API. Takes precedence over
    /// `status_port`, e.g. `"0.0.0.0:3030"` for remote monitoring.
    #[serde(default)]
    pub status_bind: Option<SocketAddr>,
}

fn default_tick_interval() -> u64 {
    5000
}
