//! `HyperliquidBroker` — REST-side implementation of `bb_core::broker::Broker`.
//!
//! Meaningful fixes vs. the pre-harness adapter:
//!
//!   - `client_id` (string) is mapped to a deterministic `Uuid` via `Uuid::v5` under a fixed
//!     namespace, so repeated calls for the same caller id produce the same `cloid` and
//!     cancel-by-cloid works end-to-end.
//!   - `bulk_order` response is parsed: `Resting { oid }` and `Filled { oid }` are captured and
//!     returned in `OrderResult.order_id` so strategies can track orders without waiting for a WS
//!     lifecycle update.
//!   - `cancel_orders` now uses `bulk_cancel_by_cloid` when only a `client_id` is present, lifting
//!     the old "dropped cancels" gotcha.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bb_core::broker::Broker;
use bb_core::error::BotError;
use bb_core::types::{
    AmendOrder, Balance, CancelOrder, CancelResult, NewOrder, Order, OrderBook, OrderResult,
    OrderStatus, OrderType, Position, Side,
};
use ethers::types::H160;
use hyperliquid_rust_sdk::{
    ClientCancelRequest, ClientCancelRequestCloid, ClientLimit, ClientModifyRequest, ClientOrder,
    ClientOrderRequest, ExchangeClient, ExchangeDataStatus, ExchangeResponseStatus, InfoClient,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use uuid::Uuid;

use crate::convert;

/// A v5 namespace UUID — any fixed value works; using a one-off UUID here so
/// it's unambiguous that the choice is arbitrary.
const CLOID_NAMESPACE: Uuid = Uuid::from_u128(0x8f3e_5b09_4f78_43d5_9b21_6c2a_44b6_1f1e);

fn client_id_to_cloid(client_id: &str) -> Uuid {
    Uuid::new_v5(&CLOID_NAMESPACE, client_id.as_bytes())
}

pub(crate) type ClientIdMap = Arc<RwLock<HashMap<String, String>>>;

pub(crate) fn new_client_id_map() -> ClientIdMap {
    Arc::new(RwLock::new(HashMap::new()))
}

pub(crate) fn register_client_id(map: &ClientIdMap, client_id: &str) -> Uuid {
    let cloid = client_id_to_cloid(client_id);
    let mut guard = map.write().unwrap_or_else(|e| e.into_inner());
    guard.insert(cloid.to_string(), client_id.to_string());
    cloid
}

pub(crate) fn original_client_id(map: &ClientIdMap, cloid: &str) -> String {
    let guard = map.read().unwrap_or_else(|e| e.into_inner());
    guard.get(cloid).cloned().unwrap_or_else(|| cloid.to_string())
}

/// Cross-thread health state shared with the WS muxer task. Mirrors the
/// Bullet adapter — `reconcile_pending` flips on every WS reconnect (so
/// strategies can resync immediately rather than wait for the periodic
/// sweep), and `disconnected` flips once the SDK's reconnect loop exits.
#[derive(Debug, Default)]
pub(crate) struct ConnectionHealth {
    pub reconcile_pending: AtomicBool,
    pub disconnected: AtomicBool,
}

pub struct HyperliquidBroker {
    exchange: ExchangeClient,
    info: InfoClient,
    address: H160,
    health: Arc<ConnectionHealth>,
    client_ids: ClientIdMap,
}

impl HyperliquidBroker {
    pub(crate) fn new(
        exchange: ExchangeClient,
        info: InfoClient,
        address: H160,
        health: Arc<ConnectionHealth>,
        client_ids: ClientIdMap,
    ) -> Self {
        Self { exchange, info, address, health, client_ids }
    }
}

#[async_trait]
impl Broker for HyperliquidBroker {
    fn name(&self) -> &str {
        "hyperliquid"
    }

    fn take_reconcile_signal(&self) -> bool {
        self.health.reconcile_pending.swap(false, Ordering::AcqRel)
    }

    fn is_disconnected(&self) -> bool {
        self.health.disconnected.load(Ordering::Acquire)
    }

    async fn get_orderbook(&self, symbol: &str, _depth: usize) -> Result<OrderBook, BotError> {
        let coin = convert::to_hl_coin(symbol);
        let resp = self.info.l2_snapshot(coin).await.map_err(|e| BotError::exchange(e, true))?;
        Ok(convert::l2_snapshot_to_orderbook(&resp))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>, BotError> {
        let state =
            self.info.user_state(self.address).await.map_err(|e| BotError::exchange(e, true))?;
        Ok(convert::user_state_to_balances(&state))
    }

    async fn get_positions(&self) -> Result<Vec<Position>, BotError> {
        let state =
            self.info.user_state(self.address).await.map_err(|e| BotError::exchange(e, true))?;
        Ok(convert::user_state_to_positions(&state))
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>, BotError> {
        let coin = convert::to_hl_coin(symbol);
        let resp =
            self.info.open_orders(self.address).await.map_err(|e| BotError::exchange(e, true))?;
        Ok(resp
            .iter()
            .filter(|o| o.coin == coin)
            .map(|o| {
                let side = match o.side.as_str() {
                    "B" | "Buy" | "buy" => Side::Buy,
                    _ => Side::Sell,
                };
                Order {
                    id: o.oid.to_string(),
                    // `open_orders` does not include cloid; callers using
                    // `client_id`-based reconcile should rely on the WS
                    // `OrderLifecycle` stream (which does carry cloid).
                    client_id: None,
                    symbol: convert::to_bb_symbol(&o.coin),
                    side,
                    order_type: OrderType::Limit,
                    price: convert::parse_dec(&o.limit_px),
                    quantity: convert::parse_dec(&o.sz),
                    filled_quantity: Decimal::ZERO,
                    status: OrderStatus::Open,
                }
            })
            .collect())
    }

    async fn place_orders(&self, orders: &[NewOrder]) -> Result<Vec<OrderResult>, BotError> {
        if orders.is_empty() {
            return Ok(vec![]);
        }

        let requests: Vec<ClientOrderRequest> = orders
            .iter()
            .map(|o| -> Result<ClientOrderRequest, BotError> {
                let coin = convert::to_hl_coin(&o.symbol);
                let tif = match o.order_type {
                    OrderType::Limit => "Gtc",
                    OrderType::PostOnly => "Alo",
                    OrderType::Market => "Ioc",
                };
                let price_f64 = o.price.to_f64().ok_or_else(|| {
                    BotError::strategy(format!("HL: cannot convert price {} to f64", o.price))
                })?;
                let sz_f64 = o.quantity.to_f64().ok_or_else(|| {
                    BotError::strategy(format!("HL: cannot convert quantity {} to f64", o.quantity))
                })?;
                let cloid =
                    o.client_id.as_deref().map(|id| register_client_id(&self.client_ids, id));

                Ok(ClientOrderRequest {
                    asset: coin,
                    is_buy: o.side == Side::Buy,
                    reduce_only: o.reduce_only,
                    limit_px: price_f64,
                    sz: sz_f64,
                    cloid,
                    order_type: ClientOrder::Limit(ClientLimit { tif: tif.to_string() }),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        match self.exchange.bulk_order(requests, None).await {
            Ok(response) => Ok(parse_bulk_order_response(&response, orders)),
            Err(e) => {
                // Tx may have actually committed even though the HTTP layer
                // errored. Treat as outcome-unknown — strategies trust the WS
                // user-data stream + periodic reconcile to surface the truth.
                tracing::warn!(error = %e, "HL bulk_order errored — outcome unknown until WS confirms");
                Ok(orders
                    .iter()
                    .map(|o| OrderResult {
                        order_id: None,
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

        // Split by which id is available. Prefer oid (cheaper API path).
        let mut by_oid: Vec<(usize, ClientCancelRequest)> = Vec::new();
        let mut by_cloid: Vec<(usize, ClientCancelRequestCloid)> = Vec::new();
        for (i, c) in cancels.iter().enumerate() {
            let coin = convert::to_hl_coin(&c.symbol);
            if let Ok(oid) = c.order_id.parse::<u64>() {
                by_oid.push((i, ClientCancelRequest { asset: coin, oid }));
                continue;
            }
            if let Some(cid) = c.client_id.as_deref() {
                by_cloid.push((
                    i,
                    ClientCancelRequestCloid { asset: coin, cloid: client_id_to_cloid(cid) },
                ));
            }
        }

        let mut results: Vec<Option<CancelResult>> = (0..cancels.len()).map(|_| None).collect();

        if !by_oid.is_empty() {
            let (indices, reqs): (Vec<_>, Vec<_>) = by_oid.into_iter().unzip();
            match self.exchange.bulk_cancel(reqs, None).await {
                Ok(_) => {
                    for i in indices {
                        results[i] = Some(CancelResult {
                            order_id: cancels[i].order_id.clone(),
                            success: true,
                            error: None,
                        });
                    }
                }
                Err(e) => {
                    for i in indices {
                        results[i] = Some(CancelResult {
                            order_id: cancels[i].order_id.clone(),
                            success: false,
                            error: Some(e.to_string()),
                        });
                    }
                }
            }
        }

        if !by_cloid.is_empty() {
            let (indices, reqs): (Vec<_>, Vec<_>) = by_cloid.into_iter().unzip();
            match self.exchange.bulk_cancel_by_cloid(reqs, None).await {
                Ok(_) => {
                    for i in indices {
                        results[i] = Some(CancelResult {
                            order_id: cancels[i].order_id.clone(),
                            success: true,
                            error: None,
                        });
                    }
                }
                Err(e) => {
                    for i in indices {
                        results[i] = Some(CancelResult {
                            order_id: cancels[i].order_id.clone(),
                            success: false,
                            error: Some(e.to_string()),
                        });
                    }
                }
            }
        }

        Ok(results
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                r.unwrap_or(CancelResult {
                    order_id: cancels[i].order_id.clone(),
                    success: false,
                    error: Some("cancel dropped: neither order_id nor client_id set".to_string()),
                })
            })
            .collect())
    }

    /// Native atomic amend via HL's `batch_modify` endpoint.
    ///
    /// Amends whose `cancel.order_id` parses as a `u64` are sent as a single
    /// `bulk_modify` call (atomic on the venue side). Amends that only carry a
    /// `client_id` fall back to cancel-then-place: HL's modify API requires a
    /// numeric oid, and we cannot derive one from a cloid alone.
    async fn amend_orders(&self, amends: &[AmendOrder]) -> Result<Vec<OrderResult>, BotError> {
        if amends.is_empty() {
            return Ok(vec![]);
        }

        // Partition into native-modify-eligible (have numeric oid) vs. fallback.
        let mut modify_indices: Vec<usize> = Vec::new();
        let mut modify_reqs: Vec<ClientModifyRequest> = Vec::new();
        let mut fallback_indices: Vec<usize> = Vec::new();

        for (i, amend) in amends.iter().enumerate() {
            if let Ok(oid) = amend.cancel.order_id.parse::<u64>() {
                let coin = convert::to_hl_coin(&amend.new_order.symbol);
                let tif = match amend.new_order.order_type {
                    OrderType::Limit => "Gtc",
                    OrderType::PostOnly => "Alo",
                    OrderType::Market => "Ioc",
                };
                let price_f64 = amend.new_order.price.to_f64().ok_or_else(|| {
                    BotError::strategy(format!(
                        "HL amend: cannot convert price {} to f64",
                        amend.new_order.price
                    ))
                })?;
                let sz_f64 = amend.new_order.quantity.to_f64().ok_or_else(|| {
                    BotError::strategy(format!(
                        "HL amend: cannot convert quantity {} to f64",
                        amend.new_order.quantity
                    ))
                })?;
                let cloid = amend
                    .new_order
                    .client_id
                    .as_deref()
                    .map(|id| register_client_id(&self.client_ids, id));
                modify_reqs.push(ClientModifyRequest {
                    oid,
                    order: ClientOrderRequest {
                        asset: coin,
                        is_buy: amend.new_order.side == Side::Buy,
                        reduce_only: amend.new_order.reduce_only,
                        limit_px: price_f64,
                        sz: sz_f64,
                        cloid,
                        order_type: ClientOrder::Limit(ClientLimit { tif: tif.to_string() }),
                    },
                });
                modify_indices.push(i);
            } else {
                fallback_indices.push(i);
            }
        }

        let mut results: Vec<Option<OrderResult>> = (0..amends.len()).map(|_| None).collect();

        // Native bulk_modify for orders with numeric oids.
        if !modify_reqs.is_empty() {
            match self.exchange.bulk_modify(modify_reqs, None).await {
                Ok(resp) => {
                    for (pos, &orig_idx) in modify_indices.iter().enumerate() {
                        results[orig_idx] = Some(parse_amend_result(&resp, pos, &amends[orig_idx]));
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "HL bulk_modify errored — outcome unknown until WS confirms");
                    for &orig_idx in &modify_indices {
                        results[orig_idx] = Some(OrderResult {
                            order_id: None,
                            client_id: amends[orig_idx].new_order.client_id.clone(),
                            success: false,
                            error: Some(e.to_string()),
                        });
                    }
                }
            }
        }

        // Fallback cancel-then-place for orders without a parseable oid.
        if !fallback_indices.is_empty() {
            let fallback_cancels: Vec<CancelOrder> =
                fallback_indices.iter().map(|&i| amends[i].cancel.clone()).collect();
            let fallback_orders: Vec<NewOrder> =
                fallback_indices.iter().map(|&i| amends[i].new_order.clone()).collect();
            self.cancel_orders(&fallback_cancels).await?;
            let place_results = self.place_orders(&fallback_orders).await?;
            for (pos, &orig_idx) in fallback_indices.iter().enumerate() {
                results[orig_idx] = Some(place_results[pos].clone());
            }
        }

        Ok(results
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                r.unwrap_or(OrderResult {
                    order_id: None,
                    client_id: amends[i].new_order.client_id.clone(),
                    success: false,
                    error: Some("amend dropped: no oid or client_id".to_string()),
                })
            })
            .collect())
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), BotError> {
        let open = self.get_open_orders(symbol).await?;
        if open.is_empty() {
            return Ok(());
        }
        let cancels: Vec<CancelOrder> = open
            .iter()
            .map(|o| CancelOrder {
                symbol: symbol.to_string(),
                order_id: o.id.clone(),
                client_id: None,
            })
            .collect();
        let _ = self.cancel_orders(&cancels).await?;
        Ok(())
    }
}

/// Extract one `OrderResult` at position `pos` from a `bulk_modify` response.
fn parse_amend_result(
    resp: &ExchangeResponseStatus,
    pos: usize,
    amend: &AmendOrder,
) -> OrderResult {
    let client_id = amend.new_order.client_id.clone();
    match resp {
        ExchangeResponseStatus::Err(msg) => {
            OrderResult { order_id: None, client_id, success: false, error: Some(msg.clone()) }
        }
        ExchangeResponseStatus::Ok(response) => {
            let statuses = response.data.as_ref().map(|d| d.statuses.as_slice()).unwrap_or(&[]);
            let (oid, success, error) = match statuses.get(pos) {
                Some(ExchangeDataStatus::Resting(r)) => (Some(r.oid.to_string()), true, None),
                Some(ExchangeDataStatus::Filled(f)) => (Some(f.oid.to_string()), true, None),
                Some(ExchangeDataStatus::Success) => (None, true, None),
                Some(ExchangeDataStatus::WaitingForFill) => (None, true, None),
                Some(ExchangeDataStatus::WaitingForTrigger) => (None, true, None),
                Some(ExchangeDataStatus::Error(msg)) => (None, false, Some(msg.clone())),
                None => (None, false, Some("no status returned".to_string())),
            };
            OrderResult { order_id: oid, client_id, success, error }
        }
    }
}

fn parse_bulk_order_response(
    resp: &ExchangeResponseStatus,
    orders: &[NewOrder],
) -> Vec<OrderResult> {
    match resp {
        ExchangeResponseStatus::Err(msg) => orders
            .iter()
            .map(|o| OrderResult {
                order_id: None,
                client_id: o.client_id.clone(),
                success: false,
                error: Some(msg.clone()),
            })
            .collect(),
        ExchangeResponseStatus::Ok(response) => {
            let statuses = response.data.as_ref().map(|d| d.statuses.as_slice()).unwrap_or(&[]);
            if statuses.len() != orders.len() {
                tracing::warn!(
                    sent = orders.len(),
                    received = statuses.len(),
                    "HL bulk_order: status count mismatch"
                );
            }
            orders
                .iter()
                .enumerate()
                .map(|(i, o)| {
                    let (oid, success, error) = match statuses.get(i) {
                        Some(ExchangeDataStatus::Resting(r)) => {
                            (Some(r.oid.to_string()), true, None)
                        }
                        Some(ExchangeDataStatus::Filled(f)) => {
                            (Some(f.oid.to_string()), true, None)
                        }
                        Some(ExchangeDataStatus::Success) => (None, true, None),
                        Some(ExchangeDataStatus::WaitingForFill) => (None, true, None),
                        Some(ExchangeDataStatus::WaitingForTrigger) => (None, true, None),
                        Some(ExchangeDataStatus::Error(msg)) => (None, false, Some(msg.clone())),
                        None => (None, false, Some("no status returned".to_string())),
                    };
                    OrderResult { order_id: oid, client_id: o.client_id.clone(), success, error }
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_id_map_round_trips_original_id() {
        let ids = new_client_id_map();
        let cloid = register_client_id(&ids, "42");

        assert_eq!(original_client_id(&ids, &cloid.to_string()), "42");
    }

    #[test]
    fn unknown_cloid_falls_back_to_uuid_string() {
        let ids = new_client_id_map();
        let cloid = Uuid::nil().to_string();

        assert_eq!(original_client_id(&ids, &cloid), cloid);
    }
}
