//! `ObserverActor` — records (ts, `bullet_mid`, `binance_mid`, `spread_bps`)
//! to CSV. Backs the `observe` subcommand. No trading; data collection only.

use async_trait::async_trait;
use bb_core::events::{BookUpdate, Tick};
use bb_exchange_binance::ReferencePriceUpdate;
use rust_decimal::Decimal;

pub struct ObserverActor {
    symbol: String,
    binance_symbol: String,
    file: std::io::BufWriter<std::fs::File>,
    bullet_mid: Option<Decimal>,
    binance_mid: Option<Decimal>,
    last_written: Option<(Decimal, Decimal)>,
    rows: u64,
    events: u64,
}

impl ObserverActor {
    pub fn new(
        symbol: String,
        binance_symbol: String,
        path: &std::path::Path,
    ) -> std::io::Result<Self> {
        use std::io::Write;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(path)?;
        let mut file = std::io::BufWriter::new(file);
        writeln!(file, "ts_unix_ms,bullet_mid,binance_mid,spread_bps")?;
        Ok(Self {
            symbol,
            binance_symbol,
            file,
            bullet_mid: None,
            binance_mid: None,
            last_written: None,
            rows: 0,
            events: 0,
        })
    }

    /// Write a row only when either mid has changed since the last write.
    /// Bullet testnet can emit many `BookUpdate` events per second with an
    /// unchanged top-of-book; recording them all produces GB-scale CSVs
    /// of duplicates.
    fn record(&mut self) -> std::io::Result<()> {
        use std::io::Write;
        self.events += 1;
        let (Some(b), Some(r)) = (self.bullet_mid, self.binance_mid) else {
            return Ok(());
        };
        if r.is_zero() {
            return Ok(());
        }
        if self.last_written == Some((b, r)) {
            return Ok(());
        }
        let spread_bps = (b - r) / r * Decimal::from(10_000);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        writeln!(self.file, "{ts},{b},{r},{spread_bps}")?;
        self.last_written = Some((b, r));
        self.rows += 1;
        Ok(())
    }
}

#[async_trait]
impl bb_core::harness::Actor for ObserverActor {
    async fn init(
        &mut self,
        _cx: &bb_core::harness::ActorContext,
    ) -> Result<(), bb_core::error::BotError> {
        tracing::info!(symbol = %self.symbol, binance = %self.binance_symbol, "observer started");
        Ok(())
    }
    async fn wind_down(
        &mut self,
        _reason: &bb_core::harness::WindDownReason,
        _cx: &bb_core::harness::ActorContext,
    ) -> Result<(), bb_core::error::BotError> {
        use std::io::Write;
        let _ = self.file.flush();
        tracing::info!(rows = self.rows, "observer final — flushed CSV");
        Ok(())
    }
}

#[async_trait]
impl bb_core::harness::EventHandler<BookUpdate> for ObserverActor {
    async fn on_event(
        &mut self,
        event: BookUpdate,
        _cx: &bb_core::harness::ActorContext,
    ) -> Result<(), bb_core::error::BotError> {
        if event.symbol != self.symbol {
            return Ok(());
        }
        if let Some(mid) = event.orderbook.midpoint() {
            self.bullet_mid = Some(mid);
        }
        self.record().map_err(|e| bb_core::error::BotError::strategy(e.to_string()))
    }
}

#[async_trait]
impl bb_core::harness::EventHandler<ReferencePriceUpdate> for ObserverActor {
    async fn on_event(
        &mut self,
        event: ReferencePriceUpdate,
        _cx: &bb_core::harness::ActorContext,
    ) -> Result<(), bb_core::error::BotError> {
        if !event.symbol.eq_ignore_ascii_case(&self.binance_symbol) {
            return Ok(());
        }
        self.binance_mid = Some(event.mid);
        self.record().map_err(|e| bb_core::error::BotError::strategy(e.to_string()))
    }
}

#[async_trait]
impl bb_core::harness::EventHandler<Tick> for ObserverActor {
    async fn on_event(
        &mut self,
        _event: Tick,
        _cx: &bb_core::harness::ActorContext,
    ) -> Result<(), bb_core::error::BotError> {
        use std::io::Write;
        // Flush every tick so a Ctrl-C doesn't lose the last seconds of data.
        let _ = self.file.flush();
        tracing::info!(
            rows_written = self.rows,
            events_seen = self.events,
            bullet_mid = ?self.bullet_mid,
            binance_mid = ?self.binance_mid,
            "observer tick"
        );
        Ok(())
    }
}
