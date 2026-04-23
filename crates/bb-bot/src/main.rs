use std::collections::HashMap;

use bb_core::config::EngineConfig;
use bb_core::engine::Engine;
use bb_core::exchange::Exchange;
use bb_core::strategy::Strategy;
use bb_exchange_bullet::{BulletConfig, BulletExchange};
use bullet_rust_sdk::types::bullet_exchange_interface;
use bullet_rust_sdk::{
    CallMessage, Client, Keypair, Network, PositiveDecimal, Transaction, UserAction,
};
use rust_decimal::Decimal;
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
    /// Generate a burner Ed25519 keypair for Bullet (prints hex secret + base58 address).
    Keygen {
        /// Network to print a faucet command for ("testnet" or "mainnet").
        #[arg(long, default_value = "testnet")]
        network: String,
    },
    /// Deposit funds from on-chain balance into the perp margin account.
    ///
    /// Reads `BB_BULLET_PRIVATE_KEY_HEX` from env. Required after funding a fresh
    /// address via the faucet — `account_info` returns 404 until the first deposit.
    Deposit {
        #[arg(long, default_value = "testnet")]
        network: String,
        /// Asset symbol (e.g. "USDC", "SOL"). Looked up via /fapi/v1/exchangeInfo.
        #[arg(long)]
        asset: String,
        /// Human-readable amount (e.g. "1000" for 1000 USDC). Scaled by on-chain decimals.
        #[arg(long)]
        amount: Decimal,
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
        Command::Keygen { network } => {
            let kp = Keypair::generate();
            let hex = kp.to_hex();
            let address = kp.address();
            let faucet_host = match network.as_str() {
                "mainnet" => {
                    return Err("Faucet is only available on testnet".into());
                }
                _ => "app.testnet.bullet.xyz",
            };
            println!("Bullet {network} burner keypair");
            println!("  private_key_hex: 0x{hex}");
            println!("  address:         {address}");
            println!();
            println!("Export for bb-bot:");
            println!("  export BB_BULLET_PRIVATE_KEY_HEX=\"0x{hex}\"");
            println!();
            println!("Fund via faucet:");
            println!(
                "  curl -X POST \"https://{faucet_host}/api/testnet/faucet?address={address}\""
            );
            Ok(())
        }
        Command::Deposit { network, asset, amount } => {
            let hex = std::env::var("BB_BULLET_PRIVATE_KEY_HEX")
                .map_err(|_| "BB_BULLET_PRIVATE_KEY_HEX is not set")?;
            let keypair = Keypair::from_hex(&hex)?;
            let address = keypair.address();
            let client = Client::builder()
                .network(Network::from(network.as_str()))
                .keypair(keypair)
                .build()
                .await?;

            let info = client.exchange_info().await?.into_inner();
            let asset_entry = info
                .assets
                .iter()
                .find(|a| a.asset.eq_ignore_ascii_case(&asset))
                .ok_or_else(|| format!("Unknown asset '{asset}' — not in exchangeInfo"))?;

            let asset_id =
                bullet_exchange_interface::types::AssetId(u16::try_from(asset_entry.asset_id)?);
            let positive = PositiveDecimal::try_from(amount)
                .map_err(|e| format!("Invalid deposit amount: {e}"))?;

            println!(
                "Depositing {amount} {asset} (asset_id={}) from {address}",
                asset_entry.asset_id
            );

            let call_msg = CallMessage::User(UserAction::Deposit { asset_id, amount: positive });
            let signed = Transaction::builder().call_message(call_msg).client(&client).build()?;
            let resp = client.send_transaction(&signed).await?;
            println!("Deposit submitted. tx_id={}", resp.id);
            Ok(())
        }
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
