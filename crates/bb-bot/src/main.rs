//! bb-bot entrypoint.
//!
//! Every run path goes through the harness. Main's job is to parse config,
//! call the right exchange `connect_*` helpers, wire the resulting brokers +
//! typed feeds into a `HarnessBuilder`, attach the strategy actor, and run.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bb_core::broker::Broker;
use bb_core::config::EngineConfig;
use bb_core::events::{BookUpdate, MarkPriceUpdate, OrderLifecycle, Tick, Trade};
use bb_core::harness::{ActorSpec, HarnessBuilder};
use bb_core::helpers::TickFeed;
use bb_exchange_bullet::{BulletConfig, connect_bullet};
use bb_exchange_hyperliquid::{HyperliquidConfig, connect_hyperliquid};
use bb_strategy_avellaneda_stoikov::{AvellanedaStoikovActor, AvellanedaStoikovConfig};
use bb_strategy_funding_arb::{FundingArbActor, FundingArbConfig};
use bb_strategy_grid::{GridActor, GridConfig};
use bullet_rust_sdk::types::bullet_exchange_interface;
use bullet_rust_sdk::{
    CallMessage, Client, Keypair, Network, PositiveDecimal, Transaction, UserAction,
};
use clap::{Parser, Subcommand};
use rust_decimal::Decimal;
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
        #[arg(short, long)]
        config: String,
    },
    /// Validate a config file without connecting.
    Validate {
        #[arg(short, long)]
        config: String,
    },
    /// Generate a burner Ed25519 keypair for Bullet.
    Keygen {
        #[arg(long, default_value = "testnet")]
        network: String,
    },
    /// Deposit funds from on-chain balance into the perp margin account.
    Deposit {
        #[arg(long, default_value = "testnet")]
        network: String,
        #[arg(long)]
        asset: String,
        #[arg(long)]
        amount: Decimal,
    },
}

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
    /// Kept so serde accepts the `type = "..."` tag in config files; we
    /// dispatch by the HashMap key (`bullet` / `hyperliquid`) in practice.
    #[serde(rename = "type")]
    #[allow(dead_code)]
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

fn strip_type_key(mut v: toml::Value) -> toml::Value {
    if let Some(t) = v.as_table_mut() {
        t.remove("type");
    }
    v
}

fn parse_exchange_config<T>(
    entries: &HashMap<String, ExchangeEntry>,
    name: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    T: serde::de::DeserializeOwned,
{
    let entry = entries
        .get(name)
        .ok_or_else(|| format!("Missing required [exchanges.{name}] section"))?;
    let raw = strip_type_key(entry.config.clone());
    let cfg: T = raw
        .try_into()
        .map_err(|e: toml::de::Error| format!("Invalid {name} config: {e}"))?;
    Ok(cfg)
}

fn sub_strategy<T>(
    strategy: &StrategyEntry,
    sub_name: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    T: serde::de::DeserializeOwned,
{
    let sub = strategy
        .config
        .get(sub_name)
        .cloned()
        .unwrap_or_else(|| strategy.config.clone());
    let parsed: T = sub
        .try_into()
        .map_err(|e: toml::de::Error| format!("Invalid {sub_name} config: {e}"))?;
    Ok(parsed)
}

// -- Dispatch: one function per strategy type -------------------------------

async fn run_grid(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let bullet_cfg: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
    let grid_cfg: GridConfig = sub_strategy(&config.strategy, "grid")?;

    let (broker, feeds) = connect_bullet(&bullet_cfg, &grid_cfg.symbol).await?;
    let broker: Arc<dyn Broker> = Arc::new(broker);

    let grid = GridActor::new(grid_cfg, "bullet");
    let tick = TickFeed::new(Duration::from_millis(config.engine.tick_interval_ms));

    let harness = HarnessBuilder::new()
        .enable_signal_shutdown()
        .with_status_port(config.engine.status_port)
        .wire_broker("bullet", broker)
        .wire_feed_named("bullet-trades", feeds.trade)
        .wire_feed_named("bullet-book", feeds.book)
        .wire_feed_named("bullet-lifecycle", feeds.lifecycle)
        .wire_feed_named("bullet-mark", feeds.mark_price)
        .wire_feed_named("ticks", tick)
        .wire_actor(
            ActorSpec::new("grid", grid)
                .sub::<BookUpdate>()
                .sub::<Trade>()
                .sub::<OrderLifecycle>()
                .sub::<Tick>(),
        )
        .build()?;
    let reason = harness.run().await?;
    tracing::info!(?reason, "Harness wound down");
    Ok(())
}

async fn run_avellaneda_stoikov(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let bullet_cfg: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
    let strat_cfg: AvellanedaStoikovConfig =
        sub_strategy(&config.strategy, "avellaneda-stoikov")?;

    let (broker, feeds) = connect_bullet(&bullet_cfg, &strat_cfg.symbol).await?;
    let broker: Arc<dyn Broker> = Arc::new(broker);

    let actor = AvellanedaStoikovActor::new(strat_cfg, "bullet");
    let tick = TickFeed::new(Duration::from_millis(config.engine.tick_interval_ms));

    let harness = HarnessBuilder::new()
        .enable_signal_shutdown()
        .with_status_port(config.engine.status_port)
        .wire_broker("bullet", broker)
        .wire_feed_named("bullet-trades", feeds.trade)
        .wire_feed_named("bullet-book", feeds.book)
        .wire_feed_named("bullet-lifecycle", feeds.lifecycle)
        .wire_feed_named("bullet-mark", feeds.mark_price)
        .wire_feed_named("ticks", tick)
        .wire_actor(
            ActorSpec::new("avellaneda-stoikov", actor)
                .sub::<BookUpdate>()
                .sub::<Trade>()
                .sub::<OrderLifecycle>()
                .sub::<Tick>(),
        )
        .build()?;
    let reason = harness.run().await?;
    tracing::info!(?reason, "Harness wound down");
    Ok(())
}

async fn run_funding_arb(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let bullet_cfg: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
    let hl_cfg: HyperliquidConfig = parse_exchange_config(&config.exchanges, "hyperliquid")?;
    let arb_cfg: FundingArbConfig = sub_strategy(&config.strategy, "funding-arb")?;
    let symbol = arb_cfg.symbol.clone();

    let (bullet_broker, bullet_feeds) = connect_bullet(&bullet_cfg, &symbol).await?;
    let (hl_broker, hl_feeds) = connect_hyperliquid(&hl_cfg, &symbol).await?;
    let bullet_broker: Arc<dyn Broker> = Arc::new(bullet_broker);
    let hl_broker: Arc<dyn Broker> = Arc::new(hl_broker);

    let actor = FundingArbActor::new(arb_cfg);
    let tick = TickFeed::new(Duration::from_millis(config.engine.tick_interval_ms));

    let harness = HarnessBuilder::new()
        .enable_signal_shutdown()
        .with_status_port(config.engine.status_port)
        .wire_broker("bullet", bullet_broker)
        .wire_broker("hyperliquid", hl_broker)
        .wire_feed_named("bullet-trades", bullet_feeds.trade)
        .wire_feed_named("bullet-book", bullet_feeds.book)
        .wire_feed_named("bullet-lifecycle", bullet_feeds.lifecycle)
        .wire_feed_named("bullet-mark", bullet_feeds.mark_price)
        .wire_feed_named("hl-trades", hl_feeds.trade)
        .wire_feed_named("hl-book", hl_feeds.book)
        .wire_feed_named("hl-lifecycle", hl_feeds.lifecycle)
        .wire_feed_named("hl-mark", hl_feeds.mark_price)
        .wire_feed_named("ticks", tick)
        .wire_actor(
            ActorSpec::new("funding-arb", actor)
                .sub::<MarkPriceUpdate>()
                .sub::<Trade>()
                .sub::<BookUpdate>()
                .sub::<Tick>(),
        )
        .build()?;
    let reason = harness.run().await?;
    tracing::info!(?reason, "Harness wound down");
    Ok(())
}

async fn run(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    match config.strategy.strategy_type.as_str() {
        "grid" => run_grid(config).await,
        "avellaneda-stoikov" => run_avellaneda_stoikov(config).await,
        "funding-arb" => run_funding_arb(config).await,
        other => Err(format!("Unknown strategy type: {other}").into()),
    }
}

// -- Validate (no connection) -----------------------------------------------

fn validate(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let stype = config.strategy.strategy_type.as_str();
    match stype {
        "grid" => {
            let _: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
            let _: GridConfig = sub_strategy(&config.strategy, "grid")?;
        }
        "avellaneda-stoikov" => {
            let _: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
            let _: AvellanedaStoikovConfig =
                sub_strategy(&config.strategy, "avellaneda-stoikov")?;
        }
        "funding-arb" => {
            let _: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
            let _: HyperliquidConfig = parse_exchange_config(&config.exchanges, "hyperliquid")?;
            let _: FundingArbConfig = sub_strategy(&config.strategy, "funding-arb")?;
        }
        other => return Err(format!("Unknown strategy type: {other}").into()),
    }
    println!("Config is valid ({stype}).");
    println!("  Engine: tick={}ms", config.engine.tick_interval_ms);
    match config.engine.status_port {
        Some(p) => println!("  Status API: port {p}"),
        None => println!("  Status API: disabled"),
    }
    Ok(())
}

// -- Auxiliary commands (unchanged) -----------------------------------------

async fn keygen(network: String) -> Result<(), Box<dyn std::error::Error>> {
    let kp = Keypair::generate();
    let hex = kp.to_hex();
    let address = kp.address();
    let faucet_host = match network.as_str() {
        "mainnet" => return Err("Faucet is only available on testnet".into()),
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
    println!("  curl -X POST \"https://{faucet_host}/api/testnet/faucet?address={address}\"");
    Ok(())
}

async fn deposit(
    network: String,
    asset: String,
    amount: Decimal,
) -> Result<(), Box<dyn std::error::Error>> {
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

    let asset_id = bullet_exchange_interface::types::AssetId(asset_entry.asset_id);
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Keygen { network } => keygen(network).await,
        Command::Deposit { network, asset, amount } => deposit(network, asset, amount).await,
        Command::Validate { config: path } => validate(load_config(&path)?),
        Command::Run { config: path } => {
            let config = load_config(&path)?;
            let filter = EnvFilter::try_new(&config.logging.level)
                .unwrap_or_else(|_| EnvFilter::new("info"));
            tracing_subscriber::fmt().with_env_filter(filter).with_target(false).init();
            tracing::info!(config_path = %path, "Starting bb-bot");
            run(config).await
        }
    }
}
