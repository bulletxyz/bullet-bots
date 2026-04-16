use std::sync::Arc;

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::exchange::Exchange;
use bb_core::types::*;
use bullet_rust_sdk::ws::models::ServerMessage;
use bullet_rust_sdk::{
    CancelOrderArgs, Client, Keypair, ManagedWebsocket, MarketId, Network, NewOrderArgs, OrderId,
    OrderType as BulletOrderType, OrderbookDepth, PositiveDecimal, Side as BulletSide, Topic,
    UserActionDiscriminants, WsEvent,
};
use rust_decimal::Decimal;

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
}

impl BulletExchange {
    pub fn new(config: BulletConfig) -> Self {
        Self { config, client: None, ws: None }
    }

    fn client(&self) -> Result<&Client, BotError> {
        self.client.as_deref().ok_or_else(|| BotError::not_connected("bullet"))
    }

    fn market_id(&self, symbol: &str) -> Result<MarketId, BotError> {
        self.client()?
            .market_id(symbol)
            .ok_or_else(|| BotError::config(format!("Unknown symbol: {symbol}")))
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
        let market_id = self.market_id(&orders[0].symbol)?;

        let sdk_orders: Vec<NewOrderArgs> = orders
            .iter()
            .map(|o| -> Result<NewOrderArgs, BotError> {
                let price = PositiveDecimal::try_from(o.price)
                    .map_err(|e| BotError::strategy(format!("Invalid price: {e}")))?;
                let size = PositiveDecimal::try_from(o.quantity)
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

                Ok(NewOrderArgs {
                    price,
                    size,
                    side,
                    order_type,
                    reduce_only: o.reduce_only,
                    client_order_id: None,
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
                c.order_id.parse::<u64>().ok().map(|id| CancelOrderArgs {
                    order_id: Some(OrderId(id)),
                    client_order_id: None,
                })
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

        let mut ws =
            client.connect_ws_managed().call().await.map_err(|e| BotError::exchange(e, true))?;

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

        tracing::info!(symbol, "Bullet: subscribed to market data");
        self.ws = Some(ws);
        Ok(())
    }

    async fn recv_event(&mut self) -> Option<ExchangeEvent> {
        let ws = self.ws.as_mut()?;

        match ws.recv().await? {
            WsEvent::Message(msg) => match *msg {
                ServerMessage::DepthUpdate(ref depth) => Some(convert::depth_to_event(depth)),
                ServerMessage::BookTicker(ref bt) => Some(convert::book_ticker_to_event(bt)),
                ServerMessage::MarkPrice(ref mp) => Some(convert::mark_price_to_event(mp)),
                ServerMessage::AggTrade(ref trade) => {
                    let addr =
                        self.client.as_ref().and_then(|c| c.address().ok()).unwrap_or_default();
                    convert::agg_trade_to_event(trade, &addr)
                }
                ServerMessage::OrderUpdate(ref update) => {
                    Some(convert::order_update_to_event(update))
                }
                _ => None,
            },
            WsEvent::Reconnecting => {
                tracing::info!("Bullet: WebSocket reconnecting (managed)");
                None
            }
            WsEvent::Disconnected(reason) => {
                tracing::error!(%reason, "Bullet: permanently disconnected");
                Some(ExchangeEvent::Disconnected { exchange: "bullet".to_string() })
            }
        }
    }
}
