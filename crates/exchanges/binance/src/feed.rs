//! Binance bookTicker feed → [`ReferencePriceUpdate`].
//!
//! Connects to `wss://stream.binance.com:9443/ws/{symbol}@bookTicker`, which
//! pushes one message per top-of-book change with best bid/ask prices and
//! quantities. We surface the mid `(b + a) / 2` as a reference price event.
//!
//! The connect helper spawns a background task owning the WS connection with
//! an internal reconnect loop (1s → 30s exponential backoff). Parse failures
//! on a single frame are logged and skipped; the connection stays up. Disconnects
//! trigger a reconnect. The feed itself is a thin wrapper around the receiver
//! end of an mpsc channel, following the same `feed_impl!` pattern as the
//! Hyperliquid adapter.

use std::str::FromStr;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::harness::{EventFeed, EventTx, FeedContext};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// Reference price update — a mid price from an external read-only venue.
///
/// Emitted by Binance's bookTicker stream. Consumers should treat `received_at`
/// as a local monotonic timestamp for staleness checks; it is **not** the
/// exchange's event time.
#[derive(Debug, Clone)]
pub struct ReferencePriceUpdate {
    /// Upstream symbol as reported by Binance (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// `(best_bid + best_ask) / 2`.
    pub mid: Decimal,
    /// Local receive instant — used by strategies to detect stale feeds.
    pub received_at: Instant,
}

/// Incoming bookTicker payload. Binance sends unquoted decimals as strings.
#[derive(Debug, Deserialize)]
struct BookTicker {
    s: String,
    b: String,
    a: String,
}

/// Which Binance venue to reference.
///
/// For arbitrage against a perpetual DEX (Bullet), [`BinanceMarket::Perp`]
/// is almost always the right choice — comparing perps-to-perps removes the
/// funding-basis confound between spot and perp prices. Spot is retained as
/// an option for completeness (useful if you're comparing against a spot
/// venue on the other side).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceMarket {
    /// `stream.binance.com:9443` — spot order book.
    Spot,
    /// `fstream.binance.com` — USDT-margined futures (perps).
    Perp,
}

impl BinanceMarket {
    fn host(self) -> &'static str {
        match self {
            Self::Spot => "stream.binance.com:9443",
            Self::Perp => "fstream.binance.com",
        }
    }
}

impl std::str::FromStr for BinanceMarket {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "spot" => Ok(Self::Spot),
            "perp" | "perps" | "futures" | "fstream" => Ok(Self::Perp),
            other => Err(format!("unknown binance market '{other}' (want 'spot' or 'perp')")),
        }
    }
}

/// Open the bookTicker stream for `symbol` on the chosen Binance market.
/// `symbol` is passed lowercased into the URL (e.g. `"btcusdt"`). Spawns a
/// reconnecting background task and returns the feed handle. Connection
/// attempts happen on the spawned task with 1s→30s exponential backoff —
/// this fn itself never fails.
pub fn connect_binance(symbol: &str, market: BinanceMarket) -> BinanceReferencePriceFeed {
    let symbol = symbol.to_lowercase();
    let url = format!("wss://{}/ws/{symbol}@bookTicker", market.host());
    let (tx, rx) = mpsc::unbounded_channel::<ReferencePriceUpdate>();

    tokio::spawn(async move {
        run_ws_loop(url, tx).await;
    });

    BinanceReferencePriceFeed { rx }
}

async fn run_ws_loop(url: String, tx: mpsc::UnboundedSender<ReferencePriceUpdate>) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        if tx.is_closed() {
            tracing::debug!("Binance feed receiver dropped, exiting WS task");
            return;
        }

        match connect_async(&url).await {
            Ok((mut ws, _resp)) => {
                tracing::info!(url = %url, "Binance reference feed connected");
                backoff = Duration::from_secs(1);

                while let Some(msg) = ws.next().await {
                    match msg {
                        Ok(Message::Text(text)) => match parse_ticker(&text) {
                            Ok(event) => {
                                if tx.send(event).is_err() {
                                    tracing::debug!("Binance feed receiver dropped mid-stream");
                                    return;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Binance bookTicker parse error");
                            }
                        },
                        Ok(Message::Ping(payload)) => {
                            if let Err(e) = ws.send(Message::Pong(payload)).await {
                                tracing::warn!(error = %e, "Binance WS pong failed");
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => {
                            tracing::warn!("Binance WS closed by server");
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Binance WS read error");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, backoff_secs = backoff.as_secs(), "Binance WS connect failed");
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn parse_ticker(text: &str) -> Result<ReferencePriceUpdate, String> {
    let tick: BookTicker =
        serde_json::from_str(text).map_err(|e| format!("json: {e}; raw={text}"))?;
    let bid = Decimal::from_str(&tick.b).map_err(|e| format!("bid decimal: {e}"))?;
    let ask = Decimal::from_str(&tick.a).map_err(|e| format!("ask decimal: {e}"))?;
    Ok(ReferencePriceUpdate {
        symbol: tick.s,
        mid: (bid + ask) / Decimal::from(2),
        received_at: Instant::now(),
    })
}

/// Feed handle. Its `run` method forwards events from the background WS
/// task onto the harness bus.
pub struct BinanceReferencePriceFeed {
    rx: mpsc::UnboundedReceiver<ReferencePriceUpdate>,
}

#[async_trait]
impl EventFeed<ReferencePriceUpdate> for BinanceReferencePriceFeed {
    async fn run(
        self: Box<Self>,
        tx: EventTx<ReferencePriceUpdate>,
        cx: FeedContext,
    ) -> Result<(), BotError> {
        let mut this = *self;
        loop {
            tokio::select! {
                biased;
                () = cx.cancelled() => return Ok(()),
                maybe = this.rx.recv() => match maybe {
                    Some(event) => { let _ = tx.send(event); }
                    None => return Ok(()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bookticker_frame() {
        let frame = r#"{"u":400900217,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}"#;
        let event = parse_ticker(frame).expect("parse");
        assert_eq!(event.symbol, "BTCUSDT");
        // mid = (25.3519 + 25.3652) / 2 = 25.35855
        assert_eq!(event.mid, Decimal::from_str("25.35855").unwrap());
    }

    #[test]
    fn rejects_non_json() {
        assert!(parse_ticker("not json").is_err());
    }

    #[test]
    fn rejects_missing_fields() {
        assert!(parse_ticker(r#"{"s":"X"}"#).is_err());
    }

    #[test]
    fn rejects_non_decimal_price() {
        let frame = r#"{"s":"BTCUSDT","b":"notanumber","a":"1"}"#;
        assert!(parse_ticker(frame).is_err());
    }
}
