//! `BulletBroker` — REST-side implementation of `bb_core::broker::Broker`.
//!
//! Holds an `Arc<Client>` shared with the streaming side (`BulletConnection`).
//! All order-placement / account-query methods go through this type; streaming
//! events are handled separately by the typed feeds.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bb_core::broker::Broker;
use bb_core::error::BotError;
use bb_core::types::{
    Balance, CancelOrder, CancelResult, NewOrder, Order, OrderBook, OrderResult, OrderStatus,
    OrderType, Position, Side,
};
use bullet_rust_sdk::{
    CancelOrderArgs, Client, ClientOrderId, MarketId, NewOrderArgs, OrderId,
    OrderType as BulletOrderType, PositiveDecimal, Side as BulletSide,
};
use rust_decimal::{Decimal, RoundingStrategy};

use crate::convert;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Increments {
    pub tick_size: Decimal,
    pub step_size: Decimal,
}

pub struct BulletBroker {
    client: Arc<Client>,
    increments: HashMap<String, Increments>,
}

impl BulletBroker {
    pub(crate) fn new(client: Arc<Client>, increments: HashMap<String, Increments>) -> Self {
        Self { client, increments }
    }

    fn market_id(&self, symbol: &str) -> Result<MarketId, BotError> {
        self.client
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

/// Fetch symbol filters (tickSize / stepSize) from exchangeInfo and cache them
/// by symbol. Used at connect-time by `BulletConnection` before handing the
/// broker to the harness.
pub(crate) async fn load_increments(
    client: &Client,
) -> Result<HashMap<String, Increments>, BotError> {
    let info = client.exchange_info().await.map_err(|e| BotError::exchange(e, true))?;
    let mut out = HashMap::new();
    for sym in &info.into_inner().symbols {
        let mut tick = None;
        let mut step = None;
        for f in &sym.filters {
            let Some(kind) = f.get("filterType").and_then(|v| v.as_str()) else {
                continue;
            };
            match kind {
                "PRICE_FILTER" => {
                    tick = f
                        .get("tickSize")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Decimal::from_str(s).ok());
                }
                "LOT_SIZE" => {
                    step = f
                        .get("stepSize")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Decimal::from_str(s).ok());
                }
                _ => {}
            }
        }
        if let (Some(tick_size), Some(step_size)) = (tick, step) {
            out.insert(sym.symbol.clone(), Increments { tick_size, step_size });
        }
    }
    Ok(out)
}

#[async_trait]
impl Broker for BulletBroker {
    fn name(&self) -> &str {
        "bullet"
    }

    async fn get_orderbook(&self, symbol: &str, depth: usize) -> Result<OrderBook, BotError> {
        let resp = self
            .client
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
        let resp = self.client.my_balances().await.map_err(|e| BotError::exchange(e, true))?;
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
        let resp = self.client.my_account().await.map_err(|e| BotError::exchange(e, true))?;
        Ok(resp
            .positions
            .iter()
            .filter(|p| !p.position_amt.is_zero())
            .map(|p| {
                let side = if p.position_amt > Decimal::ZERO {
                    Some(Side::Buy)
                } else {
                    Some(Side::Sell)
                };
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
        let resp = self
            .client
            .my_open_orders(symbol)
            .await
            .map_err(|e| BotError::exchange(e, true))?;
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
        let symbol = &orders[0].symbol;
        let market_id = self.market_id(symbol)?;
        let incr = *self
            .increments
            .get(symbol)
            .ok_or_else(|| BotError::config(format!("No tick/step cached for {symbol}")))?;

        let sdk_orders: Vec<NewOrderArgs> = orders
            .iter()
            .map(|o| -> Result<NewOrderArgs, BotError> {
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

        match self.client.place_orders(market_id, sdk_orders, false, None).await {
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

    async fn cancel_orders(
        &self,
        cancels: &[CancelOrder],
    ) -> Result<Vec<CancelResult>, BotError> {
        if cancels.is_empty() {
            return Ok(vec![]);
        }
        let market_id = self.market_id(&cancels[0].symbol)?;

        let sdk_cancels: Vec<CancelOrderArgs> = cancels
            .iter()
            .filter_map(|c| {
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

        match self.client.cancel_orders(market_id, sdk_cancels, None).await {
            Ok(_) => Ok(cancels
                .iter()
                .map(|c| CancelResult {
                    order_id: c.order_id.clone(),
                    success: true,
                    error: None,
                })
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
        let market_id = self.market_id(symbol)?;
        self.client
            .cancel_market_orders(market_id, None)
            .await
            .map_err(|e| BotError::exchange(e, false))?;
        Ok(())
    }
}
