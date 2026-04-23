use serde::Deserialize;

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
}

fn default_tick_interval() -> u64 {
    5000
}
