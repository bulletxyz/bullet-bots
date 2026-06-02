//! `BulletBroker` — REST-side implementation of `bb_core::broker::Broker`.
//!
//! Holds an `Arc<Client>` shared with the streaming side (`BulletConnection`).
//! All order-placement / account-query methods go through this type; streaming
//! events are handled separately by the typed feeds.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bb_core::broker::Broker;
use bb_core::error::BotError;
use bb_core::types::{
    AmendOrder, Balance, CancelOrder, CancelResult, NewOrder, Order, OrderBook, OrderResult,
    OrderStatus, OrderType, Position, Side,
};
use bullet_rust_sdk::{
    AmendOrderArgs, CancelOrderArgs, Client, ClientOrderId, MarketId, NewOrderArgs, OrderId,
    OrderType as BulletOrderType, PositiveDecimal, Side as BulletSide,
};
use rust_decimal::{Decimal, RoundingStrategy};

use crate::convert;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Increments {
    pub tick_size: Decimal,
    pub step_size: Decimal,
}

/// Cross-thread health state shared with the WS muxer task. The muxer flips
/// `reconcile_pending` on every reconnect (so the strategy can resync immediately
/// rather than wait for the next periodic sweep) and `disconnected` once the
/// SDK has exhausted its retry budget (so the strategy can request shutdown).
#[derive(Debug, Default)]
pub(crate) struct ConnectionHealth {
    pub reconcile_pending: AtomicBool,
    pub disconnected: AtomicBool,
}

pub struct BulletBroker {
    client: Arc<Client>,
    increments: HashMap<String, Increments>,
    health: Arc<ConnectionHealth>,
}

impl BulletBroker {
    pub(crate) fn new(
        client: Arc<Client>,
        increments: HashMap<String, Increments>,
        health: Arc<ConnectionHealth>,
    ) -> Self {
        Self { client, increments, health }
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
                        .and_then(|s| s.parse::<Decimal>().ok());
                }
                "LOT_SIZE" => {
                    step = f
                        .get("stepSize")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<Decimal>().ok());
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
    fn name(&self) -> &'static str {
        "bullet"
    }

    fn take_reconcile_signal(&self) -> bool {
        self.health.reconcile_pending.swap(false, Ordering::AcqRel)
    }

    fn is_disconnected(&self) -> bool {
        self.health.disconnected.load(Ordering::Acquire)
    }

    async fn get_orderbook(&self, symbol: &str, depth: usize) -> Result<OrderBook, BotError> {
        let resp = self
            .client
            .order_book(Some(i32::try_from(depth).unwrap_or(i32::MAX)), symbol)
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
        let resp =
            self.client.my_open_orders(symbol).await.map_err(|e| BotError::exchange(e, true))?;
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
                let qty_pd = PositiveDecimal::try_from(snapped_qty)
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
                    size: qty_pd,
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
                // Bullet echoes each accepted order as a `place_order` event
                // carrying its venue order_id + the client_order_id we sent, so we
                // surface the order_id now instead of awaiting the WS stream.
                // Keyed by client_order_id (not zipped positionally) so it survives
                // partial acceptance, interleaved events, and reordering.
                let oids: HashMap<u64, String> = resp
                    .events
                    .iter()
                    .filter_map(|ev| convert::place_order_ids(&ev.value))
                    .collect();
                tracing::debug!(tx_id = %resp.id, status = ?resp.status, oids = oids.len(), "Orders placed");
                let results = orders
                    .iter()
                    .map(|o| {
                        let order_id = o
                            .client_id
                            .as_deref()
                            .and_then(|c| c.parse::<u64>().ok())
                            .and_then(|cid| oids.get(&cid).cloned());
                        OrderResult {
                            order_id,
                            client_id: o.client_id.clone(),
                            success: true,
                            error: None,
                        }
                    })
                    .collect();
                Ok(results)
            }
            Err(e) => {
                // Broker is intentionally a thin pass-through: an HTTP error
                // here may mean the tx reverted OR that it committed and the
                // response delivery failed. We don't try to disambiguate at
                // submit time — the strategy treats this as "outcome
                // unknown" and trusts the WS user-data stream
                // (OrderUpdate/Trade) to surface the truth, with a periodic
                // REST reconcile as the gap-recovery safety net.
                tracing::warn!(error = %e, "place tx errored — outcome unknown until WS confirms");
                let err_str = e.to_string();
                Ok(orders
                    .iter()
                    .map(|o| OrderResult {
                        order_id: None,
                        client_id: o.client_id.clone(),
                        success: false,
                        error: Some(err_str.clone()),
                    })
                    .collect())
            }
        }
    }

    async fn cancel_orders(&self, cancels: &[CancelOrder]) -> Result<Vec<CancelResult>, BotError> {
        if cancels.is_empty() {
            return Ok(vec![]);
        }
        let market_id = self.market_id(&cancels[0].symbol)?;

        let mut results: Vec<Option<CancelResult>> = vec![None; cancels.len()];
        let mut sdk_cancels: Vec<CancelOrderArgs> = Vec::new();
        let mut sent_indices: Vec<usize> = Vec::new();
        for (i, c) in cancels.iter().enumerate() {
            let order_id = c.order_id.parse::<u64>().ok().map(OrderId);
            let client_order_id =
                c.client_id.as_deref().and_then(|s| s.parse::<u64>().ok()).map(ClientOrderId);
            let cancel = match (order_id, client_order_id) {
                (Some(oid), _) => CancelOrderArgs { order_id: Some(oid), client_order_id: None },
                (None, Some(cid)) => CancelOrderArgs { order_id: None, client_order_id: Some(cid) },
                (None, None) => {
                    results[i] = Some(CancelResult {
                        order_id: c.order_id.clone(),
                        success: false,
                        error: Some(
                            "cancel not sent: Bullet requires numeric order_id or client_id"
                                .to_string(),
                        ),
                    });
                    continue;
                }
            };
            sent_indices.push(i);
            sdk_cancels.push(cancel);
        }

        if sdk_cancels.is_empty() {
            return Ok(results
                .into_iter()
                .enumerate()
                .map(|(i, r)| {
                    r.unwrap_or(CancelResult {
                        order_id: cancels[i].order_id.clone(),
                        success: false,
                        error: Some("cancel not sent".to_string()),
                    })
                })
                .collect());
        }

        match self.client.cancel_orders(market_id, sdk_cancels, None).await {
            Ok(_) => {
                for i in sent_indices {
                    results[i] = Some(CancelResult {
                        order_id: cancels[i].order_id.clone(),
                        success: true,
                        error: None,
                    });
                }
                Ok(results
                    .into_iter()
                    .enumerate()
                    .map(|(i, r)| {
                        r.unwrap_or(CancelResult {
                            order_id: cancels[i].order_id.clone(),
                            success: false,
                            error: Some("cancel not sent".to_string()),
                        })
                    })
                    .collect())
            }
            Err(e) => {
                tracing::warn!(error = %e, "cancel tx errored — outcome unknown until WS confirms");
                let err_str = e.to_string();
                for i in sent_indices {
                    results[i] = Some(CancelResult {
                        order_id: cancels[i].order_id.clone(),
                        success: false,
                        error: Some(err_str.clone()),
                    });
                }
                Ok(results
                    .into_iter()
                    .enumerate()
                    .map(|(i, r)| {
                        r.unwrap_or(CancelResult {
                            order_id: cancels[i].order_id.clone(),
                            success: false,
                            error: Some("cancel not sent".to_string()),
                        })
                    })
                    .collect())
            }
        }
    }

    async fn amend_orders(&self, amends: &[AmendOrder]) -> Result<Vec<OrderResult>, BotError> {
        if amends.is_empty() {
            return Ok(vec![]);
        }
        let symbol = &amends[0].new_order.symbol;
        let market_id = self.market_id(symbol)?;
        let incr = *self
            .increments
            .get(symbol)
            .ok_or_else(|| BotError::config(format!("No tick/step cached for {symbol}")))?;

        let mut results: Vec<Option<OrderResult>> = vec![None; amends.len()];
        let mut sdk_amends: Vec<AmendOrderArgs> = Vec::new();
        let mut sent_indices: Vec<usize> = Vec::new();
        for (i, amend) in amends.iter().enumerate() {
            match build_amend_args(amend, incr) {
                Ok(args) => {
                    sent_indices.push(i);
                    sdk_amends.push(args);
                }
                Err(error) => {
                    results[i] = Some(OrderResult {
                        order_id: None,
                        client_id: amend.new_order.client_id.clone(),
                        success: false,
                        error: Some(error),
                    });
                }
            }
        }

        if sdk_amends.is_empty() {
            return Ok(results
                .into_iter()
                .enumerate()
                .map(|(i, r)| {
                    r.unwrap_or(OrderResult {
                        order_id: None,
                        client_id: amends[i].new_order.client_id.clone(),
                        success: false,
                        error: Some("amend not sent".to_string()),
                    })
                })
                .collect());
        }

        match self.client.amend_orders(market_id, sdk_amends, None).await {
            Ok(_) => {
                for i in sent_indices {
                    results[i] = Some(OrderResult {
                        order_id: None,
                        client_id: amends[i].new_order.client_id.clone(),
                        success: true,
                        error: None,
                    });
                }
                Ok(results
                    .into_iter()
                    .enumerate()
                    .map(|(i, r)| {
                        r.unwrap_or(OrderResult {
                            order_id: None,
                            client_id: amends[i].new_order.client_id.clone(),
                            success: false,
                            error: Some("amend not sent".to_string()),
                        })
                    })
                    .collect())
            }
            Err(e) => {
                tracing::warn!(error = %e, "amend tx errored — outcome unknown until WS confirms");
                let err_str = e.to_string();
                for i in sent_indices {
                    results[i] = Some(OrderResult {
                        order_id: None,
                        client_id: amends[i].new_order.client_id.clone(),
                        success: false,
                        error: Some(err_str.clone()),
                    });
                }
                Ok(results
                    .into_iter()
                    .enumerate()
                    .map(|(i, r)| {
                        r.unwrap_or(OrderResult {
                            order_id: None,
                            client_id: amends[i].new_order.client_id.clone(),
                            success: false,
                            error: Some("amend not sent".to_string()),
                        })
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

fn build_amend_args(amend: &AmendOrder, incr: Increments) -> Result<AmendOrderArgs, String> {
    let order_id = amend.cancel.order_id.parse::<u64>().ok().map(OrderId);
    let client_order_id =
        amend.cancel.client_id.as_deref().and_then(|s| s.parse::<u64>().ok()).map(ClientOrderId);
    let cancel = match (order_id, client_order_id) {
        (Some(oid), _) => CancelOrderArgs { order_id: Some(oid), client_order_id: None },
        (None, Some(cid)) => CancelOrderArgs { order_id: None, client_order_id: Some(cid) },
        (None, None) => {
            return Err("amend not sent: Bullet requires numeric order_id or client_id".to_string());
        }
    };

    let o = &amend.new_order;
    let price_strategy = match o.side {
        Side::Buy => RoundingStrategy::ToZero,
        Side::Sell => RoundingStrategy::AwayFromZero,
    };
    let price = BulletBroker::snap(o.price, incr.tick_size, price_strategy);
    let qty = BulletBroker::snap(o.quantity, incr.step_size, RoundingStrategy::ToZero);
    let price = PositiveDecimal::try_from(price).map_err(|e| format!("Invalid price: {e}"))?;
    let qty_pd = PositiveDecimal::try_from(qty).map_err(|e| format!("Invalid quantity: {e}"))?;
    let side = match o.side {
        Side::Buy => BulletSide::Bid,
        Side::Sell => BulletSide::Ask,
    };
    let order_type = match o.order_type {
        OrderType::Limit => BulletOrderType::Limit,
        OrderType::PostOnly => BulletOrderType::PostOnly,
        OrderType::Market => BulletOrderType::ImmediateOrCancel,
    };
    let new_client_order_id = o
        .client_id
        .as_deref()
        .map(|s| {
            s.parse::<u64>()
                .map(ClientOrderId)
                .map_err(|e| format!("client_id '{s}' must be a u64 for Bullet: {e}"))
        })
        .transpose()?;
    Ok(AmendOrderArgs {
        cancel,
        place: NewOrderArgs {
            price,
            size: qty_pd,
            side,
            order_type,
            reduce_only: o.reduce_only,
            client_order_id: new_client_order_id,
            pending_tpsl_pair: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn increments() -> Increments {
        Increments { tick_size: Decimal::new(1, 2), step_size: Decimal::new(1, 3) }
    }

    fn amend(
        cancel_order_id: &str,
        cancel_client_id: Option<&str>,
        new_client_id: Option<&str>,
    ) -> AmendOrder {
        AmendOrder {
            cancel: CancelOrder {
                symbol: "BTC-USD".into(),
                order_id: cancel_order_id.into(),
                client_id: cancel_client_id.map(str::to_string),
            },
            new_order: NewOrder {
                symbol: "BTC-USD".into(),
                side: Side::Buy,
                order_type: OrderType::PostOnly,
                price: Decimal::from(100),
                quantity: Decimal::new(1, 3),
                client_id: new_client_id.map(str::to_string),
                reduce_only: false,
            },
        }
    }

    #[test]
    fn build_amend_rejects_missing_cancel_identifier() {
        let err = build_amend_args(&amend("", None, Some("1")), increments()).unwrap_err();
        assert!(err.contains("numeric order_id or client_id"));
    }

    #[test]
    fn build_amend_rejects_non_numeric_new_client_id() {
        let err = build_amend_args(&amend("123", None, Some("abc")), increments()).unwrap_err();
        assert!(err.contains("must be a u64"));
    }
}
