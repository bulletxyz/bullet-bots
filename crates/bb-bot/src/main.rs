//! bb-bot entrypoint.
//!
//! Every run path goes through the harness. Main's job is to parse config,
//! call the right exchange `connect_*` helpers, wire the resulting brokers +
//! typed feeds into a `HarnessBuilder`, attach the strategy actor, and run.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bb_core::broker::Broker;
use bb_core::config::{EngineConfig, ValidateConfig};
use bb_core::events::{BookUpdate, MarkPriceUpdate, OrderLifecycle, Tick, Trade};
use bb_core::harness::{ActorSpec, HarnessBuilder};
use bb_core::helpers::TickFeed;
use bb_exchange_binance::{BinanceMarket, ReferencePriceUpdate, connect_binance};
use bb_exchange_bullet::{BulletConfig, BulletFeeds, connect_bullet};
use bb_exchange_hyperliquid::{HyperliquidConfig, connect_hyperliquid};
use bb_strategy_avellaneda_stoikov::{AvellanedaStoikovActor, AvellanedaStoikovConfig};
use bb_strategy_funding_arb::{FundingArbActor, FundingArbConfig};
use bb_strategy_grid::{GridActor, GridConfig};
use bb_strategy_reference_arb::{ReferenceArbActor, ReferenceArbConfig};
use bb_strategy_simple_mm::{SimpleMmActor, SimpleMmConfig};
use bullet_rust_sdk::types::bullet_exchange_interface;
use bullet_rust_sdk::{
    CallMessage, Client, Keypair, Network, PositiveDecimal, Transaction, UserAction,
};
use clap::{Parser, Subcommand};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

mod observer;

use observer::ObserverActor;

#[derive(Parser)]
#[command(name = "bb-bot", about = "Bullet Bots trading system")]
struct Cli {
    /// Load environment variables from this file before running. Defaults to
    /// `./.env` if present. Keys/account addresses are read from the
    /// environment, so this is how a `.env` gets picked up.
    #[arg(long, global = true)]
    env_file: Option<PathBuf>,

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
    /// Generate a burner Ed25519 keypair for Bullet and write its base58 secret
    /// to a 0600 key file.
    Keygen {
        #[arg(long, default_value = "testnet")]
        network: String,
        /// Where to write the key file. Defaults to `$HOME/.config/bullet/id.key`.
        #[arg(long)]
        out: Option<PathBuf>,
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
    /// Cancel all open orders and market-close any open position on a symbol.
    /// Useful for cleaning up after a prior session so `reference-arb` will
    /// start (it refuses to run with a non-flat position).
    Flatten {
        #[arg(long, default_value = "testnet")]
        network: String,
        #[arg(long)]
        symbol: String,
    },
    /// Record (ts, `bullet_mid`, `binance_mid`, `spread_bps`) to CSV continuously.
    /// No trading. Use for collecting a dataset to analyze offline before
    /// committing to a live strategy run.
    Observe {
        #[arg(long, default_value = "testnet")]
        network: String,
        #[arg(long, default_value = "BTC-USD")]
        symbol: String,
        #[arg(long, default_value = "btcusdt")]
        binance_symbol: String,
        /// "perp" (USDT-M futures) or "spot".
        #[arg(long, default_value = "perp")]
        binance_market: String,
        /// Output CSV path. Parent directory is created if missing.
        #[arg(long)]
        output: PathBuf,
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
    /// The `type = "..."` tag in `[exchanges.<name>]`. Required so serde
    /// accepts configs that carry it, but **not** used for dispatch: exchanges
    /// are looked up by their `HashMap` key (`bullet` / `hyperliquid`), so the
    /// value here is effectively cosmetic. (Contrast `StrategyEntry::type`,
    /// which *is* the dispatch key.) `#[allow(dead_code)]` because nothing
    /// reads the field after deserialization.
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

    if let Some(bullet) = config.exchanges.get_mut("bullet")
        && let Some(table) = bullet.config.as_table_mut()
    {
        // Explicit config wins; env only fills a field the config omits or
        // leaves empty. For a trading bot this avoids an ambient
        // BB_BULLET_KEY_FILE silently overriding the wallet set in config.
        if table.get("key_file").and_then(toml::Value::as_str).is_none_or(str::is_empty)
            && let Some(path) = std::env::var("BB_BULLET_KEY_FILE").ok().filter(|v| !v.is_empty())
        {
            table.insert("key_file".to_string(), toml::Value::String(path));
        }
        if table.get("private_key").and_then(toml::Value::as_str).is_none_or(str::is_empty)
            && let Some(key) = bullet_key_from_env()
        {
            table.insert("private_key".to_string(), toml::Value::String(key));
        }
    }
    if let Some(hl) = config.exchanges.get_mut("hyperliquid")
        && let Some(table) = hl.config.as_table_mut()
    {
        // Config wins; env only fills a field left unset or empty.
        if table.get("key_file").and_then(toml::Value::as_str).is_none_or(str::is_empty)
            && let Some(path) =
                std::env::var("BB_HYPERLIQUID_KEY_FILE").ok().filter(|v| !v.is_empty())
        {
            table.insert("key_file".to_string(), toml::Value::String(path));
        }
        if table.get("private_key").and_then(toml::Value::as_str).is_none_or(str::is_empty)
            && let Some(key) =
                std::env::var("BB_HYPERLIQUID_PRIVATE_KEY").ok().filter(|v| !v.is_empty())
        {
            table.insert("private_key".to_string(), toml::Value::String(key));
        }
        if table.get("account_address").and_then(toml::Value::as_str).is_none_or(str::is_empty)
            && let Some(addr) =
                std::env::var("BB_HYPERLIQUID_ACCOUNT_ADDRESS").ok().filter(|v| !v.is_empty())
        {
            table.insert("account_address".to_string(), toml::Value::String(addr));
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
    let entry =
        entries.get(name).ok_or_else(|| format!("Missing required [exchanges.{name}] section"))?;
    let raw = strip_type_key(entry.config.clone());
    let cfg: T =
        raw.try_into().map_err(|e: toml::de::Error| format!("Invalid {name} config: {e}"))?;
    Ok(cfg)
}

fn sub_strategy<T>(
    strategy: &StrategyEntry,
    sub_name: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    T: serde::de::DeserializeOwned,
{
    let sub = strategy.config.get(sub_name).cloned().unwrap_or_else(|| strategy.config.clone());
    let parsed: T =
        sub.try_into().map_err(|e: toml::de::Error| format!("Invalid {sub_name} config: {e}"))?;
    Ok(parsed)
}

fn validate_strategy_config<T>(
    strategy: &StrategyEntry,
    sub_name: &str,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: serde::de::DeserializeOwned + ValidateConfig,
{
    let cfg: T = sub_strategy(strategy, sub_name)?;
    cfg.validate().map_err(|e| format!("{sub_name} config invalid: {e}").into())
}

// -- Dispatch: one function per strategy type -------------------------------

/// Wire the Bullet broker and its four standard feeds (trade / book /
/// lifecycle / mark) onto a `HarnessBuilder`. Per-strategy extras (tick feed,
/// reference feeds, the actor + its `.sub::<E>()`s) stay at the call site.
fn wire_bullet(
    builder: HarnessBuilder,
    broker: Arc<dyn Broker>,
    feeds: BulletFeeds,
) -> HarnessBuilder {
    builder
        .wire_broker("bullet", broker)
        .wire_feed_named("bullet-trades", feeds.trade)
        .wire_feed_named("bullet-book", feeds.book)
        .wire_feed_named("bullet-lifecycle", feeds.lifecycle)
        .wire_feed_named("bullet-mark", feeds.mark_price)
}

async fn run_simple_mm(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let bullet_cfg: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
    let mm_cfg: SimpleMmConfig = sub_strategy(&config.strategy, "simple-mm")?;

    let (broker, feeds) = connect_bullet(&bullet_cfg, &mm_cfg.symbol).await?;
    let broker: Arc<dyn Broker> = Arc::new(broker);

    let actor = SimpleMmActor::new(mm_cfg);
    let tick = TickFeed::new(Duration::from_millis(config.engine.tick_interval_ms));

    let builder = HarnessBuilder::new().with_status_config(&config.engine).enable_signal_shutdown();
    let harness = wire_bullet(builder, broker, feeds)
        .wire_feed_named("ticks", tick)
        .wire_actor(
            ActorSpec::new("simple-mm", actor)
                .sub::<BookUpdate>()
                .sub_critical::<Trade>()
                .sub_critical::<OrderLifecycle>()
                .sub::<Tick>(),
        )
        .build()?;
    let reason = harness.run().await?;
    tracing::info!(?reason, "Harness wound down");
    Ok(())
}

async fn run_grid(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let bullet_cfg: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
    let grid_cfg: GridConfig = sub_strategy(&config.strategy, "grid")?;

    let (broker, feeds) = connect_bullet(&bullet_cfg, &grid_cfg.symbol).await?;
    let broker: Arc<dyn Broker> = Arc::new(broker);

    let grid = GridActor::new(grid_cfg);
    let tick = TickFeed::new(Duration::from_millis(config.engine.tick_interval_ms));

    let builder = HarnessBuilder::new().with_status_config(&config.engine).enable_signal_shutdown();
    let harness = wire_bullet(builder, broker, feeds)
        .wire_feed_named("ticks", tick)
        .wire_actor(
            ActorSpec::new("grid", grid)
                .sub::<BookUpdate>()
                .sub_critical::<Trade>()
                .sub_critical::<OrderLifecycle>()
                .sub::<Tick>(),
        )
        .build()?;
    let reason = harness.run().await?;
    tracing::info!(?reason, "Harness wound down");
    Ok(())
}

async fn run_avellaneda_stoikov(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let bullet_cfg: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
    let strat_cfg: AvellanedaStoikovConfig = sub_strategy(&config.strategy, "avellaneda-stoikov")?;

    let (broker, feeds) = connect_bullet(&bullet_cfg, &strat_cfg.symbol).await?;
    let broker: Arc<dyn Broker> = Arc::new(broker);

    // Optional reference feed (fair-value MM mode).
    let reference = match strat_cfg.reference_exchange.as_deref() {
        None => None,
        Some("binance") => {
            let symbol = strat_cfg.reference_symbol.as_deref().ok_or_else(|| {
                "reference_exchange = \"binance\" requires reference_symbol".to_string()
            })?;
            let market: BinanceMarket = strat_cfg.reference_market.parse()?;
            Some(connect_binance(symbol, market))
        }
        Some(other) => return Err(format!("Unknown reference_exchange: {other}").into()),
    };

    let actor = AvellanedaStoikovActor::new(strat_cfg);
    let tick = TickFeed::new(Duration::from_millis(config.engine.tick_interval_ms));

    let base = HarnessBuilder::new().with_status_config(&config.engine).enable_signal_shutdown();
    let mut builder = wire_bullet(base, broker, feeds).wire_feed_named("ticks", tick);

    let mut spec = ActorSpec::new("avellaneda-stoikov", actor)
        .sub::<BookUpdate>()
        .sub_critical::<Trade>()
        .sub_critical::<OrderLifecycle>()
        .sub::<Tick>();

    if let Some(ref_feed) = reference {
        builder = builder.wire_feed_named("binance-ref", ref_feed);
        spec = spec.sub::<ReferencePriceUpdate>();
    }

    let harness = builder.wire_actor(spec).build()?;
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

    let base = HarnessBuilder::new().with_status_config(&config.engine).enable_signal_shutdown();
    let harness = wire_bullet(base, bullet_broker, bullet_feeds)
        .wire_broker("hyperliquid", hl_broker)
        .wire_feed_named("hl-trades", hl_feeds.trade)
        .wire_feed_named("hl-book", hl_feeds.book)
        .wire_feed_named("hl-lifecycle", hl_feeds.lifecycle)
        .wire_feed_named("hl-mark", hl_feeds.mark_price)
        .wire_feed_named("ticks", tick)
        .wire_actor(
            ActorSpec::new("funding-arb", actor)
                .sub::<MarkPriceUpdate>()
                .sub_critical::<Trade>()
                .sub::<BookUpdate>()
                .sub::<Tick>(),
        )
        .build()?;
    let reason = harness.run().await?;
    tracing::info!(?reason, "Harness wound down");
    Ok(())
}

async fn run_reference_arb(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let bullet_cfg: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
    let arb_cfg: ReferenceArbConfig = sub_strategy(&config.strategy, "reference-arb")?;

    let (broker, feeds) = connect_bullet(&bullet_cfg, &arb_cfg.symbol).await?;
    let broker: Arc<dyn Broker> = Arc::new(broker);

    let market: BinanceMarket = arb_cfg.binance_market.parse()?;
    let ref_feed = connect_binance(&arb_cfg.binance_symbol, market);
    let actor = ReferenceArbActor::new(arb_cfg);
    let tick = TickFeed::new(Duration::from_millis(config.engine.tick_interval_ms));

    let base = HarnessBuilder::new().with_status_config(&config.engine).enable_signal_shutdown();
    let harness = wire_bullet(base, broker, feeds)
        .wire_feed_named("binance-ref", ref_feed)
        .wire_feed_named("ticks", tick)
        .wire_actor(
            ActorSpec::new("reference-arb", actor)
                .sub::<BookUpdate>()
                .sub::<ReferencePriceUpdate>()
                .sub_critical::<Trade>()
                .sub_critical::<OrderLifecycle>()
                .sub::<Tick>(),
        )
        .build()?;
    let reason = harness.run().await?;
    tracing::info!(?reason, "Harness wound down");
    Ok(())
}

async fn run(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    match config.strategy.strategy_type.as_str() {
        "simple-mm" => run_simple_mm(config).await,
        "grid" => run_grid(config).await,
        "avellaneda-stoikov" => run_avellaneda_stoikov(config).await,
        "funding-arb" => run_funding_arb(config).await,
        "reference-arb" => run_reference_arb(config).await,
        other => Err(format!("Unknown strategy type: {other}").into()),
    }
}

// -- Validate (no connection) -----------------------------------------------

fn validate(config: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let stype = config.strategy.strategy_type.as_str();
    match stype {
        "simple-mm" => {
            let _: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
            validate_strategy_config::<SimpleMmConfig>(&config.strategy, "simple-mm")?;
        }
        "grid" => {
            let _: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
            validate_strategy_config::<GridConfig>(&config.strategy, "grid")?;
        }
        "avellaneda-stoikov" => {
            let _: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
            validate_strategy_config::<AvellanedaStoikovConfig>(
                &config.strategy,
                "avellaneda-stoikov",
            )?;
        }
        "funding-arb" => {
            let _: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
            let _: HyperliquidConfig = parse_exchange_config(&config.exchanges, "hyperliquid")?;
            validate_strategy_config::<FundingArbConfig>(&config.strategy, "funding-arb")?;
        }
        "reference-arb" => {
            let _: BulletConfig = parse_exchange_config(&config.exchanges, "bullet")?;
            validate_strategy_config::<ReferenceArbConfig>(&config.strategy, "reference-arb")?;
        }
        other => return Err(format!("Unknown strategy type: {other}").into()),
    }
    println!("Config is valid ({stype}).");
    println!("  Engine: tick={}ms", config.engine.tick_interval_ms);
    match (config.engine.status_bind, config.engine.status_port) {
        (Some(addr), _) => println!("  Status API: {addr}"),
        (None, Some(p)) => println!("  Status API: port {p}"),
        (None, None) => println!("  Status API: disabled"),
    }
    Ok(())
}

// -- Auxiliary commands (unchanged) -----------------------------------------

fn keygen(network: &str, out: Option<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    let faucet_host = match network {
        "mainnet" => return Err("Faucet is only available on testnet".into()),
        _ => "app.testnet.bullet.xyz",
    };

    let path = out.unwrap_or_else(default_key_path);
    if path.exists() {
        return Err(format!(
            "Refusing to overwrite existing key file at {}. \
             Use `--out <path>` to write elsewhere, or remove it first.",
            path.display()
        )
        .into());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let (secret_b58, address) = bb_exchange_bullet::key::generate_base58()?;
    std::fs::write(&path, &secret_b58)?;
    set_keystore_permissions(&path)?;

    println!("Bullet {network} burner keypair");
    println!("  address:  {address}");
    println!("  key_file: {} (base58 secret, 0600)", path.display());
    println!();
    println!("Configure bb-bot:");
    println!("  [exchanges.bullet]");
    println!("  key_file = \"{}\"", path.display());
    println!("  # or:");
    println!("  export BB_BULLET_KEY_FILE=\"{}\"", path.display());
    println!();
    println!("Fund via faucet:");
    // The faucet host rejects requests without a browser User-Agent (returns
    // "Forbidden"), so the printed command sets one.
    println!(
        "  curl -X POST -H \"User-Agent: Mozilla/5.0\" \
         \"https://{faucet_host}/api/testnet/faucet?address={address}\""
    );
    println!();
    println!(
        "Then initialize your trading account (the faucet funds the wallet, not the account):"
    );
    println!("  cargo run --bin bb-bot -- deposit --network {network} --asset USDC --amount 5000");
    println!("Without this, order placement fails with: user_variants not found");
    Ok(())
}

/// `$HOME/.config/bullet/id.key`. Falls back to the current directory if
/// `HOME` is unset (rare — CI sandboxes, some containers).
fn default_key_path() -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(".config/bullet/id.key"),
        None => PathBuf::from("./bullet-id.key"),
    }
}

/// Set the key file to owner-read/write only (0600). On non-Unix platforms
/// this is a no-op — Windows ACLs are left to the user's home directory
/// permissions.
#[cfg(unix)]
fn set_keystore_permissions(path: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(0o600);
    std::fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_keystore_permissions(_path: &std::path::Path) -> std::io::Result<()> {
    Ok(())
}

/// Resolve a Keypair for standalone (non-harness) commands like `deposit`,
/// in the same preference order as `BulletConfig`: `BB_BULLET_KEY_FILE` wins,
/// then `BB_BULLET_PRIVATE_KEY`, then the default path, else error.
fn load_deposit_keypair() -> Result<Keypair, Box<dyn std::error::Error>> {
    if let Some(path) = std::env::var("BB_BULLET_KEY_FILE").ok().filter(|v| !v.is_empty()) {
        return bb_exchange_bullet::key::keypair_from_key_file(std::path::Path::new(&path))
            .map_err(Into::into);
    }
    if let Some(secret) = bullet_key_from_env() {
        return bb_exchange_bullet::key::keypair_from_secret(&secret).map_err(Into::into);
    }
    let default = default_key_path();
    if default.exists() {
        return bb_exchange_bullet::key::keypair_from_key_file(&default).map_err(Into::into);
    }
    Err("No key material — set BB_BULLET_KEY_FILE, BB_BULLET_PRIVATE_KEY, \
         or run `bb-bot keygen` to create a default key file"
        .into())
}

/// Read the Bullet signer key string from the environment (`BB_BULLET_PRIVATE_KEY`).
/// An empty value is treated as absent, so a blank var doesn't shadow a key file
/// or trigger a spurious "no key material" error.
fn bullet_key_from_env() -> Option<String> {
    std::env::var("BB_BULLET_PRIVATE_KEY").ok().filter(|v| !v.is_empty())
}

/// Parse a network name into a [`Network`], accepting only `"mainnet"` /
/// `"testnet"`. Unlike `Network::from`, an unknown value is a hard error
/// rather than silently mapping to `Network::Custom` — matching how
/// `connect_bullet` validates the network in the `run` path.
fn parse_network(s: &str) -> Result<Network, bb_core::error::BotError> {
    match s {
        "mainnet" => Ok(Network::Mainnet),
        "testnet" => Ok(Network::Testnet),
        other => Err(bb_core::error::BotError::config(format!(
            "Unknown Bullet network '{other}' — use 'mainnet' or 'testnet'"
        ))),
    }
}

/// Build a [`BulletConfig`] for standalone commands (`flatten` / `observe`)
/// that don't load a TOML config. Resolves key material the same way as
/// `connect_bullet` / `load_deposit_keypair`: `BB_BULLET_KEY_FILE` env wins,
/// else `BB_BULLET_PRIVATE_KEY`, else the default `~/.config/bullet/id.key`
/// file if it exists. This lets a user who ran `bb-bot keygen` (which writes
/// the default key file) use these commands with no extra env setup.
/// `connect_bullet` enforces that at least one source yields usable key material.
fn bullet_config_from_env(network: String) -> BulletConfig {
    use secrecy::SecretString;

    let private_key = bullet_key_from_env().unwrap_or_default();
    let key_file =
        std::env::var("BB_BULLET_KEY_FILE").ok().filter(|v| !v.is_empty()).map(Into::into).or_else(
            || {
                // Only fall back to the default key file when no key string was supplied,
                // matching `load_deposit_keypair`'s precedence (env key_file → env key →
                // default key file) so flatten/observe/deposit pick the same account.
                if private_key.is_empty() {
                    let default = default_key_path();
                    default.exists().then_some(default)
                } else {
                    None
                }
            },
        );
    BulletConfig { network, key_file, private_key: SecretString::new(private_key) }
}

/// Build a [`HyperliquidConfig`] from the environment for standalone commands
/// (`flatten`). Returns `None` when no HL key material is set, so `flatten`
/// skips the HL venue. `account_address` is the master for API-wallet keys.
fn hyperliquid_config_from_env(network: String) -> Option<HyperliquidConfig> {
    use secrecy::SecretString;

    let private_key = std::env::var("BB_HYPERLIQUID_PRIVATE_KEY").ok().filter(|v| !v.is_empty());
    let key_file = std::env::var_os("BB_HYPERLIQUID_KEY_FILE").map(Into::into);
    if private_key.is_none() && key_file.is_none() {
        return None;
    }
    Some(HyperliquidConfig {
        network,
        key_file,
        private_key: SecretString::new(private_key.unwrap_or_default()),
        account_address: std::env::var("BB_HYPERLIQUID_ACCOUNT_ADDRESS")
            .ok()
            .filter(|v| !v.is_empty()),
    })
}

async fn deposit(
    network: String,
    asset: String,
    amount: Decimal,
) -> Result<(), Box<dyn std::error::Error>> {
    let keypair = load_deposit_keypair()?;
    let address = keypair.address();
    let client =
        Client::builder().network(parse_network(&network)?).keypair(keypair).build().await?;

    let info = client.exchange_info().await?.into_inner();
    let asset_entry = info
        .assets
        .iter()
        .find(|a| a.asset.eq_ignore_ascii_case(&asset))
        .ok_or_else(|| format!("Unknown asset '{asset}' — not in exchangeInfo"))?;

    let asset_id = bullet_exchange_interface::types::AssetId(asset_entry.asset_id);
    let positive =
        PositiveDecimal::try_from(amount).map_err(|e| format!("Invalid deposit amount: {e}"))?;

    println!("Depositing {amount} {asset} (asset_id={}) from {address}", asset_entry.asset_id);

    let call_msg = CallMessage::User(UserAction::Deposit { asset_id, amount: positive });
    let signed = Transaction::builder().call_message(call_msg).client(&client).build()?;
    let resp = client.send_transaction(&signed).await?;
    println!("Deposit submitted. tx_id={}", resp.id);
    Ok(())
}

async fn flatten(network: String, symbol: String) -> Result<(), Box<dyn std::error::Error>> {
    // Flatten every venue the bot trades, so a delta-neutral strategy's legs
    // are both closed. Key material resolves via env (.env is auto-loaded). Each
    // venue is skipped (not fatal) if its keys aren't configured — an HL-only
    // user shouldn't be blocked by a missing Bullet key, and vice versa.
    let bullet_cfg = bullet_config_from_env(network.clone());
    match bb_exchange_bullet::connect_bullet(&bullet_cfg, &symbol).await {
        Ok((bullet, _feeds)) => flatten_broker(&bullet, &symbol, "bullet").await?,
        Err(e) => println!("[bullet] skipping flatten — connect failed: {e}"),
    }

    if let Some(hl_cfg) = hyperliquid_config_from_env(network) {
        match connect_hyperliquid(&hl_cfg, &symbol).await {
            Ok((hl, _feeds)) => flatten_broker(&hl, &symbol, "hyperliquid").await?,
            Err(e) => println!("[hyperliquid] skipping flatten — connect failed: {e}"),
        }
    }
    Ok(())
}

/// Cancel resting orders and market-close any open position on `symbol` for one
/// broker. Used by `flatten` for each configured venue.
async fn flatten_broker<B: Broker>(
    broker: &B,
    symbol: &str,
    venue: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use bb_core::types::{NewOrder, OrderType, Side};
    const FLATTEN_SLIPPAGE_BPS: u64 = 100;

    let _ = broker.cancel_all_orders(symbol).await;
    let positions = broker.get_positions().await?;
    let Some(pos) = positions.iter().find(|p| p.symbol == symbol && !p.size.is_zero()) else {
        println!("[{venue}] No position on {symbol}. Nothing to flatten.");
        return Ok(());
    };

    // Close with an opposite-side market order of the same magnitude.
    let (close_side, qty) = match pos.side {
        Some(Side::Buy) => (Side::Sell, pos.size),
        Some(Side::Sell) => (Side::Buy, pos.size),
        None => {
            println!("[{venue}] Position size {} with no side — skipping.", pos.size);
            return Ok(());
        }
    };

    // Market here is an IoC limit: needs a bounded worst-case price. Use the
    // opposing top-of-book ± 1%; the IoC fills at top-of-book or better.
    let book = broker.get_orderbook(symbol, 5).await?;
    let base = match close_side {
        Side::Buy => book.best_ask().map(|l| l.price),
        Side::Sell => book.best_bid().map(|l| l.price),
    }
    .ok_or("Orderbook has no opposing-side liquidity — cannot flatten")?;
    let slip = Decimal::from(FLATTEN_SLIPPAGE_BPS) / Decimal::from(10_000);
    let ioc_price = match close_side {
        Side::Buy => base * (Decimal::ONE + slip),
        Side::Sell => base * (Decimal::ONE - slip),
    };

    println!(
        "[{venue}] Flattening {symbol}: size={} side={:?} → IoC {:?} {qty} @ {ioc_price}",
        pos.size, pos.side, close_side
    );
    let order = NewOrder {
        symbol: symbol.to_string(),
        side: close_side,
        order_type: OrderType::Market,
        price: ioc_price,
        quantity: qty,
        client_id: None,
        reduce_only: true,
    };
    let results = broker.place_orders(&[order]).await?;
    for r in &results {
        if r.success {
            println!("[{venue}] Close order accepted: order_id={:?}", r.order_id);
        } else {
            println!("[{venue}] Close order FAILED: {:?}", r.error);
        }
    }
    Ok(())
}

// -- Observe: record (ts, bullet_mid, binance_mid, spread_bps) CSV ---------

async fn observe(
    network: String,
    symbol: String,
    binance_symbol: String,
    binance_market: String,
    output: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let bullet_cfg = bullet_config_from_env(network);

    let (_broker, bullet_feeds) = bb_exchange_bullet::connect_bullet(&bullet_cfg, &symbol).await?;
    let market: BinanceMarket = binance_market.parse()?;
    let ref_feed = connect_binance(&binance_symbol, market);

    let observer = ObserverActor::new(symbol.clone(), binance_symbol.clone(), &output)?;
    let tick = TickFeed::new(Duration::from_secs(30));

    tracing::info!(
        output = %output.display(),
        bullet_symbol = %symbol,
        binance = %binance_symbol,
        market = %binance_market,
        "observer starting"
    );

    let harness = HarnessBuilder::new()
        .enable_signal_shutdown()
        .wire_feed_named("bullet-book", bullet_feeds.book)
        .wire_feed_named("binance-ref", ref_feed)
        .wire_feed_named("ticks", tick)
        .wire_actor(
            ActorSpec::new("observer", observer)
                .sub::<BookUpdate>()
                .sub::<ReferencePriceUpdate>()
                .sub::<Tick>(),
        )
        .build()?;
    let reason = harness.run().await?;
    tracing::info!(?reason, "observe wound down");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    // Load .env into the process environment before anything reads env vars.
    // An explicit --env-file must exist; the default ./.env is optional. Real
    // environment variables already set are never overridden.
    match &cli.env_file {
        Some(path) => {
            dotenvy::from_path(path).map_err(|e| format!("--env-file {}: {e}", path.display()))?;
            eprintln!("Loaded env from {}", path.display());
        }
        None => {
            if let Ok(path) = dotenvy::dotenv() {
                eprintln!("Loaded env from {}", path.display());
            }
        }
    }
    match cli.command {
        Command::Keygen { network, out } => keygen(&network, out),
        Command::Deposit { network, asset, amount } => deposit(network, asset, amount).await,
        Command::Flatten { network, symbol } => flatten(network, symbol).await,
        Command::Observe { network, symbol, binance_symbol, binance_market, output } => {
            observe(network, symbol, binance_symbol, binance_market, output).await
        }
        Command::Validate { config: path } => validate(&load_config(&path)?),
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
