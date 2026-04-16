use std::collections::HashMap;

use bb_core::config::EngineConfig;
use bb_core::engine::Engine;
use bb_core::exchange::Exchange;
use bb_core::strategy::Strategy;
use bb_exchange_bullet::{BulletConfig, BulletExchange};
use bb_exchange_hyperliquid::{HyperliquidConfig, HyperliquidExchange};
use bb_strategy_grid::{GridConfig, GridStrategy};
use bb_strategy_funding_arb::{FundingArbConfig, FundingArbStrategy};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "bb-bot", about = "Bullet Bots trading system")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the bot with the given config file.
    Run {
        /// Path to the TOML config file.
        #[arg(short, long)]
        config: String,
    },
    /// Validate a config file without connecting.
    Validate {
        /// Path to the TOML config file.
        #[arg(short, long)]
        config: String,
    },
}

/// Top-level application config.
#[derive(Debug, Deserialize)]
struct AppConfig {
    engine: EngineConfig,
    exchanges: HashMap<String, ExchangeEntry>,
    strategy: StrategyEntry,
    #[serde(default)]
    logging: LoggingConfig,
}

#[derive(Debug, Deserialize)]
struct ExchangeEntry {
    #[serde(rename = "type")]
    exchange_type: String,
    #[serde(flatten)]
    config: toml::Value,
}

#[derive(Debug, Deserialize)]
struct StrategyEntry {
    #[serde(rename = "type")]
    strategy_type: String,
    #[serde(flatten)]
    config: toml::Value,
}

#[derive(Debug, Default, Deserialize)]
struct LoggingConfig {
    #[serde(default = "default_log_level")]
    level: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

fn load_config(path: &str) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    let mut config: AppConfig = toml::from_str(&content)?;

    // Apply environment variable overrides for sensitive fields
    if let Some(bullet) = config.exchanges.get_mut("bullet") {
        if let Ok(key) = std::env::var("BB_BULLET_PRIVATE_KEY_HEX") {
            if let Some(table) = bullet.config.as_table_mut() {
                table.insert("private_key_hex".to_string(), toml::Value::String(key));
            }
        }
    }
    if let Some(hl) = config.exchanges.get_mut("hyperliquid") {
        if let Ok(key) = std::env::var("BB_HYPERLIQUID_PRIVATE_KEY_HEX") {
            if let Some(table) = hl.config.as_table_mut() {
                table.insert("private_key_hex".to_string(), toml::Value::String(key));
            }
        }
    }

    Ok(config)
}

fn build_exchanges(
    entries: HashMap<String, ExchangeEntry>,
) -> Result<HashMap<String, Box<dyn Exchange>>, Box<dyn std::error::Error>> {
    let mut exchanges: HashMap<String, Box<dyn Exchange>> = HashMap::new();

    for (name, entry) in entries {
        // The flatten may include the type field; remove it for sub-deserialization
        let mut config_value = entry.config;
        if let Some(table) = config_value.as_table_mut() {
            table.remove("type");
        }

        let exchange: Box<dyn Exchange> = match entry.exchange_type.as_str() {
            "bullet" => {
                let config: BulletConfig =
                    config_value.try_into().map_err(|e: toml::de::Error| {
                        format!("Invalid bullet config for '{name}': {e}")
                    })?;
                Box::new(BulletExchange::new(config))
            }
            "hyperliquid" => {
                let config: HyperliquidConfig =
                    config_value.try_into().map_err(|e: toml::de::Error| {
                        format!("Invalid hyperliquid config for '{name}': {e}")
                    })?;
                Box::new(HyperliquidExchange::new(config))
            }
            other => {
                return Err(format!("Unknown exchange type: {other}").into());
            }
        };
        exchanges.insert(name, exchange);
    }

    Ok(exchanges)
}

fn build_strategy(
    entry: StrategyEntry,
    primary_exchange: &str,
) -> Result<Box<dyn Strategy>, Box<dyn std::error::Error>> {
    let strategy_type = &entry.strategy_type;

    // The config contains the strategy-specific sub-table keyed by type name.
    // e.g., [strategy.grid] -> config has key "grid" with the grid config.
    let sub_config = entry.config.get(strategy_type).cloned().unwrap_or(entry.config.clone());

    match strategy_type.as_str() {
        "grid" => {
            let config: GridConfig = sub_config
                .try_into()
                .map_err(|e: toml::de::Error| format!("Invalid grid config: {e}"))?;
            Ok(Box::new(GridStrategy::new(config, primary_exchange.to_string())))
        }
        "funding-arb" => {
            let config: FundingArbConfig = sub_config
                .try_into()
                .map_err(|e: toml::de::Error| format!("Invalid funding-arb config: {e}"))?;
            Ok(Box::new(FundingArbStrategy::new(config)))
        }
        other => Err(format!("Unknown strategy type: {other}").into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Validate { config: path } => {
            let config = load_config(&path)?;
            let exchanges = build_exchanges(config.exchanges)?;
            let primary = exchanges.keys().next().ok_or("No exchanges configured")?.clone();
            let _strategy = build_strategy(config.strategy, &primary)?;
            println!("Config is valid.");
            println!(
                "  Engine: symbol={}, tick={}ms",
                config.engine.symbol, config.engine.tick_interval_ms
            );
            println!("  Exchanges: {:?}", exchanges.keys().collect::<Vec<_>>());
            println!("  Status API: port {}", config.engine.status_port);
            Ok(())
        }
        Command::Run { config: path } => {
            let config = load_config(&path)?;

            // Initialize logging
            let filter = EnvFilter::try_new(&config.logging.level)
                .unwrap_or_else(|_| EnvFilter::new("info"));
            tracing_subscriber::fmt().with_env_filter(filter).with_target(false).init();

            tracing::info!(config_path = %path, "Starting bb-bot");

            let engine_config = config.engine;
            let exchanges = build_exchanges(config.exchanges)?;
            let primary = exchanges.keys().next().ok_or("No exchanges configured")?.clone();
            let strategy = build_strategy(config.strategy, &primary)?;

            let engine = Engine::new(exchanges, strategy, engine_config);
            engine.run().await?;

            Ok(())
        }
    }
}
