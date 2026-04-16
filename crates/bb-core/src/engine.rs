use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::backoff::ExponentialBackoff;
use crate::config::EngineConfig;
use crate::error::BotError;
use crate::exchange::Exchange;
use crate::status::{self, StatusState};
use crate::strategy::{Strategy, StrategyContext};
use crate::types::ExchangeEvent;

/// The bot engine. Wires exchanges, strategy, and the event loop together.
pub struct Engine {
    ctx: StrategyContext,
    strategy: Box<dyn Strategy>,
    status_tx: watch::Sender<serde_json::Value>,
}

impl Engine {
    /// Create a new engine from named exchanges and a strategy.
    pub fn new(
        exchanges: HashMap<String, Box<dyn Exchange>>,
        strategy: Box<dyn Strategy>,
        config: EngineConfig,
    ) -> Self {
        let primary_name = exchanges.keys().next().expect("At least one exchange required").clone();

        let (status_tx, _) = watch::channel(serde_json::Value::Null);

        let ctx = StrategyContext::new(exchanges, primary_name, config);
        Self { ctx, strategy, status_tx }
    }

    /// Run the bot. Blocks until shutdown or fatal error.
    pub async fn run(mut self) -> Result<(), BotError> {
        let config = self.ctx.config.clone();

        // Connect all exchanges
        self.connect_all_exchanges().await?;

        // Subscribe all exchanges to the symbol
        let symbol = config.symbol.clone();
        for (name, exchange) in self.ctx.exchanges_mut() {
            tracing::info!(exchange = name, symbol = %symbol, "Subscribing");
            exchange.subscribe(&symbol).await?;
        }

        // Spawn status API
        let status_state = Arc::new(StatusState {
            strategy_name: self.strategy.name().to_string(),
            symbol: symbol.clone(),
            start_time: Instant::now(),
            strategy_status: self.status_tx.subscribe(),
        });
        let _status_handle = status::spawn_server(config.status_port, status_state);

        // Initial state refresh
        self.ctx.refresh_state().await?;

        // on_start
        tracing::info!(strategy = self.strategy.name(), "Starting strategy");
        self.strategy.on_start(&mut self.ctx).await?;
        self.update_status();

        // Main event loop
        let mut tick_interval =
            tokio::time::interval(Duration::from_millis(config.tick_interval_ms));
        tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let shutdown = tokio::signal::ctrl_c();
        tokio::pin!(shutdown);

        loop {
            // We need to poll recv_event from all exchanges. Build a future that
            // returns the first event from any exchange.
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("Shutdown signal received");
                    break;
                }
                _ = tick_interval.tick() => {
                    if let Err(e) = self.strategy.on_tick(&mut self.ctx).await {
                        tracing::error!(error = %e, "on_tick error");
                        if e.is_fatal() { break; }
                    }
                    self.update_status();
                }
                event = recv_from_any(self.ctx.exchanges_mut()) => {
                    match event {
                        Some(ExchangeEvent::Disconnected { ref exchange }) => {
                            tracing::warn!(exchange = exchange.as_str(), "Exchange disconnected, reconnecting");
                            let name = exchange.clone();
                            if let Err(e) = self.reconnect_exchange(&name, &config).await {
                                tracing::error!(exchange = name.as_str(), error = %e, "Reconnect failed fatally");
                                if e.is_fatal() { break; }
                            }
                        }
                        Some(event) => {
                            self.ctx.apply_event(&event);
                            if let Err(e) = self.strategy.on_event(&mut self.ctx, event).await {
                                tracing::error!(error = %e, "on_event error");
                                if e.is_fatal() { break; }
                            }
                            self.update_status();
                        }
                        None => {
                            // All exchange streams ended
                            tracing::warn!("All exchange streams ended");
                            break;
                        }
                    }
                }
            }
        }

        // Graceful shutdown
        tracing::info!("Running strategy shutdown");
        if let Err(e) = self.strategy.on_stop(&mut self.ctx).await {
            tracing::error!(error = %e, "on_stop error");
        }

        Ok(())
    }

    fn update_status(&self) {
        let _ = self.status_tx.send(self.strategy.status());
    }

    async fn connect_all_exchanges(&mut self) -> Result<(), BotError> {
        let max_delay = Duration::from_millis(self.ctx.config.reconnect_max_delay_ms);

        for (name, exchange) in self.ctx.exchanges_mut() {
            let mut backoff = ExponentialBackoff::new(Duration::from_secs(1), max_delay);

            loop {
                tracing::info!(exchange = name.as_str(), "Connecting");
                match exchange.connect().await {
                    Ok(()) => {
                        tracing::info!(exchange = name.as_str(), "Connected");
                        break;
                    }
                    Err(e) if e.is_retryable() => {
                        let delay = backoff.next_delay();
                        tracing::warn!(
                            exchange = name.as_str(),
                            error = %e,
                            delay_ms = delay.as_millis() as u64,
                            "Connect failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }

    async fn reconnect_exchange(
        &mut self,
        name: &str,
        config: &EngineConfig,
    ) -> Result<(), BotError> {
        let max_delay = Duration::from_millis(config.reconnect_max_delay_ms);
        let mut backoff = ExponentialBackoff::new(Duration::from_secs(1), max_delay);

        let exchange = self
            .ctx
            .exchanges_mut()
            .get_mut(name)
            .ok_or_else(|| BotError::UnknownExchange(name.to_string()))?;

        loop {
            match exchange.connect().await {
                Ok(()) => {
                    exchange.subscribe(&config.symbol).await?;
                    tracing::info!(exchange = name, "Reconnected");
                    break;
                }
                Err(e) if e.is_retryable() => {
                    let delay = backoff.next_delay();
                    tracing::warn!(
                        exchange = name,
                        error = %e,
                        delay_ms = delay.as_millis() as u64,
                        "Reconnect failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }

        self.ctx.refresh_state().await?;
        Ok(())
    }
}

/// Poll `recv_event()` from all exchanges, returning the first one that fires.
///
/// This uses a simple round-robin approach. For a small number of exchanges
/// (1-3 typical), this is efficient enough.
async fn recv_from_any(
    exchanges: &mut HashMap<String, Box<dyn Exchange>>,
) -> Option<ExchangeEvent> {
    if exchanges.is_empty() {
        return None;
    }

    // For a single exchange (the common case), just call recv directly
    if exchanges.len() == 1 {
        let (_, exchange) = exchanges.iter_mut().next()?;
        return exchange.recv_event().await;
    }

    // For multiple exchanges, use select! over all of them.
    // We can't use FuturesUnordered easily with mutable borrows,
    // so we use a tokio::select! macro with a dynamic approach.
    // For 2-3 exchanges this is fine.
    let names: Vec<String> = exchanges.keys().cloned().collect();

    // Poll each exchange with a short timeout, round-robin.
    // For 2-3 exchanges this adds at most 1ms latency per round, which is
    // acceptable for strategies that tick on second-scale intervals.
    loop {
        for name in &names {
            if let Some(exchange) = exchanges.get_mut(name) {
                // Use tokio::time::timeout for non-blocking poll
                let result =
                    tokio::time::timeout(Duration::from_millis(1), exchange.recv_event()).await;

                match result {
                    Ok(Some(event)) => return Some(event),
                    Ok(None) => return None, // Stream closed
                    Err(_) => continue,      // Timeout, try next
                }
            }
        }
        // Yield to avoid busy-spinning
        tokio::task::yield_now().await;
    }
}
