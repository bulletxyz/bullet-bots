use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::exchange::Exchange;
use bb_core::types::*;
use bullet_rust_sdk::ws::models::ServerMessage;
use bullet_rust_sdk::{
    CancelOrderArgs, Client, ClientOrderId, Keypair, ManagedWebsocket, MarketId, Network,
    NewOrderArgs, OrderId, OrderType as BulletOrderType, OrderbookDepth, PositiveDecimal,
    Side as BulletSide, Topic, UserActionDiscriminants, WsEvent,
};
use rust_decimal::{Decimal, RoundingStrategy};

#[derive(Debug, Clone, Copy)]
struct Increments {
    tick_size: Decimal,
    step_size: Decimal,
}

use crate::config::BulletConfig;
use crate::convert;

/// Bullet exchange adapter.
///
/// Uses the SDK's convenience methods (`client.place_orders()`,
/// `client.cancel_market_orders()`, `client.my_open_orders()`) and
/// `ManagedWebsocket` for auto-reconnecting WS.
pub struct BulletExchange {
    config: BulletConfig,
    client: Option<Arc<Client>>,
    ws: Option<ManagedWebsocket>,
    increments: HashMap<String, Increments>,
}

impl BulletExchange {
    pub fn new(config: BulletConfig) -> Self {
        Self { config, client: None, ws: None, increments: HashMap::new() }
    }

    fn client(&self) -> Result<&Client, BotError> {
        self.client.as_deref().ok_or_else(|| BotError::not_connected("bullet"))
    }

    fn market_id(&self, symbol: &str) -> Result<MarketId, BotError> {
        self.client()?
            .market_id(symbol)
            .ok_or_else(|| BotError::config(format!("Unknown symbol: {symbol}")))
    }

    /// Snap `value` to the nearest multiple of `increment` using `strategy`.
    fn snap(value: Decimal, increment: Decimal, strategy: RoundingStrategy) -> Decimal {
        if increment.is_zero() {
            return value;
        }
        (value / increment).round_dp_with_strategy(0, strategy) * increment
    }
}

#[async_trait]
impl Exchange for BulletExchange {
    fn name(&self) -> &str {
        "bullet"
    }

    async fn connect(&mut self) -> Result<(), BotError> {
        let keypair = Keypair::from_hex(&self.config.private_key_hex)
            .map_err(|e| BotError::config(format!("Invalid private key: {e}")))?;

        let network = match self.config.network.as_str() {
            "mainnet" => Network::Mainnet,
            "testnet" => Network::Testnet,
            other => Network::from(other),
        };

        let client = Client::builder()
            .network(network)
            .keypair(keypair)
            .user_actions(vec![
                UserActionDiscriminants::PlaceOrders,
                UserActionDiscriminants::CancelOrders,
                UserActionDiscriminants::CancelMarketOrders,
            ])
            .build()
            .await
            .map_err(|e| BotError::exchange(e, true))?;

        let address = client.address().map_err(|e| BotError::exchange(e, false))?;

        // Cache tick_size / step_size per symbol from exchangeInfo filters.
        // SymbolInfo exposes only precision (max decimals); actual lot/tick
        // increments live in the PRICE_FILTER / LOT_SIZE filter entries.
        let info = client.exchange_info().await.map_err(|e| BotError::exchange(e, true))?;
        for sym in &info.into_inner().symbols {
            let mut tick = None;
            let mut step = None;
            for f in &sym.filters {
                let Some(kind) = f.get("filterType").and_then(|v| v.as_str()) else { continue };
                match kind {
                    "PRICE_FILTER" => {
                        tick = f.get("tickSize").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok());
                    }
                    "LOT_SIZE" => {
                        step = f.get("stepSize").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok());
                    }
                    _ => {}
                }
            }
            if let (Some(tick_size), Some(step_size)) = (tick, step) {
                self.increments.insert(sym.symbol.clone(), Increments { tick_size, step_size });
            }
        }

        tracing::info!(
            symbols = client.symbols().len(),
            address = %address,
            "Bullet: connected"
        );

        self.client = Some(Arc::new(client));
        Ok(())
    }

    async fn get_orderbook(&self, symbol: &str, depth: usize) -> Result<OrderBook, BotError> {
        let client = self.client()?;
        let resp = client
            .order_book(Some(depth as i32), symbol)
            .await
            .map_err(|e| BotError::exchange(e, true))?
            .into_inner();

        Ok(OrderBook {
            bids: convert::parse_orderbook_levels(&resp.bids),
            asks: convert::parse_orderbook_levels(&resp.asks),
            last_update_id: resp.last_update_id,
        })
    }

    async fn get_balances(&self) -> Result<Vec<Balance>, BotError> {
        let client = self.client()?;
        let resp = client.my_balances().await.map_err(|e| BotError::exchange(e, true))?;

        Ok(resp
            .iter()
            .map(|b| Balance {
                asset: b.asset.clone(),
                available: b.available_balance,
                total: b.balance,
            })
            .collect())
    }

    async fn get_positions(&self) -> Result<Vec<Position>, BotError> {
        let client = self.client()?;
        let resp = client.my_account().await.map_err(|e| BotError::exchange(e, true))?;

        Ok(resp
            .positions
            .iter()
            .filter(|p| !p.position_amt.is_zero())
            .map(|p| {
                let side =
                    if p.position_amt > Decimal::ZERO { Some(Side::Buy) } else { Some(Side::Sell) };
                Position {
                    symbol: p.symbol.clone(),
                    side,
                    size: p.position_amt.abs(),
                    entry_price: p.entry_price,
                    unrealized_pnl: p.unrealized_profit,
                }
            })
            .collect())
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>, BotError> {
        let client = self.client()?;
        let resp = client.my_open_orders(symbol).await.map_err(|e| BotError::exchange(e, true))?;

        Ok(resp
            .iter()
            .map(|o| {
                let side = if o.side == "BUY" { Side::Buy } else { Side::Sell };
                let order_type = match o.order_type.as_str() {
                    "POST_ONLY" => OrderType::PostOnly,
                    _ => OrderType::Limit,
                };
                Order {
                    id: o.order_id.to_string(),
                    client_id: o.client_order_id.clone(),
                    symbol: o.symbol.clone(),
                    side,
                    order_type,
                    price: o.price,
                    quantity: o.orig_qty,
                    filled_quantity: o.executed_qty,
                    status: OrderStatus::Open,
                }
            })
            .collect())
    }

    async fn place_orders(&self, orders: &[NewOrder]) -> Result<Vec<OrderResult>, BotError> {
        if orders.is_empty() {
            return Ok(vec![]);
        }

        let client = self.client()?;
        let symbol = &orders[0].symbol;
        let market_id = self.market_id(symbol)?;
        let incr = *self
            .increments
            .get(symbol)
            .ok_or_else(|| BotError::config(format!("No tick/step cached for {symbol}")))?;

        let sdk_orders: Vec<NewOrderArgs> = orders
            .iter()
            .map(|o| -> Result<NewOrderArgs, BotError> {
                // Snap price toward the book (buy down, sell up) to avoid
                // crossing from a rounding step, and snap qty down to step_size.
                let price_strategy = match o.side {
                    Side::Buy => RoundingStrategy::ToZero,
                    Side::Sell => RoundingStrategy::AwayFromZero,
                };
                let snapped_price = Self::snap(o.price, incr.tick_size, price_strategy);
                let snapped_qty = Self::snap(o.quantity, incr.step_size, RoundingStrategy::ToZero);
                let price = PositiveDecimal::try_from(snapped_price)
                    .map_err(|e| BotError::strategy(format!("Invalid price: {e}")))?;
                let size = PositiveDecimal::try_from(snapped_qty)
                    .map_err(|e| BotError::strategy(format!("Invalid quantity: {e}")))?;
                let side = match o.side {
                    Side::Buy => BulletSide::Bid,
                    Side::Sell => BulletSide::Ask,
                };
                let order_type = match o.order_type {
                    OrderType::Limit => BulletOrderType::Limit,
                    OrderType::PostOnly => BulletOrderType::PostOnly,
                    OrderType::Market => BulletOrderType::ImmediateOrCancel,
                };

                // Map caller-supplied NewOrder.client_id (String) to the
                // on-chain ClientOrderId(u64). We parse rather than hash so
                // the strategy keeps full control over the namespace.
                let client_order_id = o
                    .client_id
                    .as_deref()
                    .map(|s| {
                        s.parse::<u64>().map(ClientOrderId).map_err(|e| {
                            BotError::strategy(format!(
                                "client_id '{s}' must be a u64 for Bullet: {e}"
                            ))
                        })
                    })
                    .transpose()?;

                Ok(NewOrderArgs {
                    price,
                    size,
                    side,
                    order_type,
                    reduce_only: o.reduce_only,
                    client_order_id,
                    pending_tpsl_pair: None,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        match client.place_orders(market_id, sdk_orders, false, None).await {
            Ok(resp) => {
                tracing::debug!(tx_id = %resp.id, "Orders placed");
                Ok(orders
                    .iter()
                    .map(|o| OrderResult {
                        order_id: String::new(),
                        client_id: o.client_id.clone(),
                        success: true,
                        error: None,
                    })
                    .collect())
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to place orders");
                Ok(orders
                    .iter()
                    .map(|o| OrderResult {
                        order_id: String::new(),
                        client_id: o.client_id.clone(),
                        success: false,
                        error: Some(e.to_string()),
                    })
                    .collect())
            }
        }
    }

    async fn cancel_orders(&self, cancels: &[CancelOrder]) -> Result<Vec<CancelResult>, BotError> {
        if cancels.is_empty() {
            return Ok(vec![]);
        }

        let client = self.client()?;
        let market_id = self.market_id(&cancels[0].symbol)?;

        let sdk_cancels: Vec<CancelOrderArgs> = cancels
            .iter()
            .filter_map(|c| {
                // Prefer exchange order_id; fall back to client_id when the
                // order_id hasn't landed yet (the usual case right after
                // placement, before OrderUpdate events come through).
                let order_id = c.order_id.parse::<u64>().ok().map(OrderId);
                let client_order_id = c
                    .client_id
                    .as_deref()
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(ClientOrderId);
                if order_id.is_none() && client_order_id.is_none() {
                    return None;
                }
                Some(CancelOrderArgs { order_id, client_order_id })
            })
            .collect();

        match client.cancel_orders(market_id, sdk_cancels, None).await {
            Ok(_) => Ok(cancels
                .iter()
                .map(|c| CancelResult { order_id: c.order_id.clone(), success: true, error: None })
                .collect()),
            Err(e) => {
                tracing::error!(error = %e, "Failed to cancel orders");
                Ok(cancels
                    .iter()
                    .map(|c| CancelResult {
                        order_id: c.order_id.clone(),
                        success: false,
                        error: Some(e.to_string()),
                    })
                    .collect())
            }
        }
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), BotError> {
        let client = self.client()?;
        let market_id = self.market_id(symbol)?;
        client
            .cancel_market_orders(market_id, None)
            .await
            .map_err(|e| BotError::exchange(e, false))?;
        Ok(())
    }

    async fn subscribe(&mut self, symbol: &str) -> Result<(), BotError> {
        let client = self.client.as_ref().ok_or_else(|| BotError::not_connected("bullet"))?;
        let address = client.address().map_err(|e| BotError::exchange(e, false))?;

        let ws = client
            .connect_ws_managed()
            .call()
            .await
            .map_err(|e| BotError::exchange(e, true))?;

        ws.subscribe(
            [
                Topic::depth(symbol, OrderbookDepth::D20),
                Topic::book_ticker(symbol),
                Topic::agg_trade(symbol),
                Topic::mark_price(symbol),
            ],
            None,
        )
        .map_err(|e| BotError::exchange(e, false))?;

        // User-order lifecycle stream. Bullet uses an address-prefixed topic
        // (no listenKey / auth handshake — the address itself is the filter).
        // Topic enum doesn't model this yet, so submit as a raw string.
        // Docs: https://tradingapi.bullet.xyz/docs/ws/index.html#orderupdate
        // `{addr}@ORDER_TRADE_UPDATE` is also accepted as a Binance alias.
        let user_topic = format!("{address}@user.orders");
        ws.subscribe_raw([user_topic.clone()], None)
            .map_err(|e| BotError::exchange(e, false))?;

        tracing::info!(symbol, user_topic, "Bullet: subscribed to market + user-order data");
        self.ws = Some(ws);
        Ok(())
    }

    async fn recv_event(&mut self) -> Option<ExchangeEvent> {
        let ws = self.ws.as_mut()?;

        // `None` from `recv_event` tells the engine the stream is dead. So
        // loop past non-terminal events (subscribe acks, pongs, heartbeats,
        // reconnecting notices) until we get something the engine should see.
        loop {
            let Some(ws_event) = ws.recv().await else {
                tracing::warn!("Bullet: managed WS recv returned None, stream ended");
                return None;
            };
            match ws_event {
                WsEvent::Message(msg) => match *msg {
                    ServerMessage::DepthUpdate(ref depth) => {
                        return Some(convert::depth_to_event(depth));
                    }
                    ServerMessage::BookTicker(ref bt) => {
                        return Some(convert::book_ticker_to_event(bt));
                    }
                    ServerMessage::MarkPrice(ref mp) => {
                        return Some(convert::mark_price_to_event(mp));
                    }
                    ServerMessage::AggTrade(_) => {
                        // Public-trade stream. Own-fill accounting is driven
                        // by `ServerMessage::OrderUpdate` (authoritative
                        // user-order lifecycle stream), so we no longer
                        // convert AggTrade into `ExchangeEvent::Trade` — that
                        // was a pre-OrderUpdate fallback that double-counted
                        // fills once OrderUpdate started working.
                    }
                    ServerMessage::OrderUpdate(ref update) => {
                        return Some(convert::order_update_to_event(update));
                    }
                    // Subscribe/unsubscribe acks, pongs, unknown types — not
                    // surfaced to the strategy, but the stream is still alive.
                    _ => continue,
                },
                WsEvent::Reconnecting => {
                    tracing::info!("Bullet: WebSocket reconnecting (managed)");
                    continue;
                }
                WsEvent::Disconnected(reason) => {
                    tracing::error!(%reason, "Bullet: permanently disconnected");
                    return Some(ExchangeEvent::Disconnected {
                        exchange: "bullet".to_string(),
                    });
                }
            }
        }
    }
}
