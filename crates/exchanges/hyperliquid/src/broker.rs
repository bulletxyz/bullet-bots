//! `HyperliquidBroker` — REST-side implementation of `bb_core::broker::Broker`.
//!
//! Meaningful fixes vs. the pre-harness adapter:
//!
//!   - `client_id` (string) is mapped to a deterministic `Uuid` via `Uuid::v5`
//!     under a fixed namespace, so repeated calls for the same caller id
//!     produce the same `cloid` and cancel-by-cloid works end-to-end.
//!   - `bulk_order` response is parsed: `Resting { oid }` and `Filled { oid }`
//!     are captured and returned in `OrderResult.order_id` so strategies can
//!     track orders without waiting for a WS lifecycle update.
//!   - `cancel_orders` now uses `bulk_cancel_by_cloid` when only a `client_id`
//!     is present, lifting the old "dropped cancels" gotcha.

use async_trait::async_trait;
use bb_core::broker::Broker;
use bb_core::error::BotError;
use bb_core::types::{
    Balance, CancelOrder, CancelResult, NewOrder, Order, OrderBook, OrderResult, OrderStatus,
    OrderType, Position, Side,
};
use ethers::signers::LocalWallet;
use ethers::types::H160;
use hyperliquid_rust_sdk::{
    ClientCancelRequest, ClientCancelRequestCloid, ClientLimit, ClientOrder, ClientOrderRequest,
    ExchangeClient, ExchangeDataStatus, ExchangeResponseStatus, InfoClient,
};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::convert;

/// A v5 namespace UUID — any fixed value works; using a one-off UUID here so
/// it's unambiguous that the choice is arbitrary.
const CLOID_NAMESPACE: Uuid = Uuid::from_u128(0x8f3e_5b09_4f78_43d5_9b21_6c2a_44b6_1f1e);

fn client_id_to_cloid(client_id: &str) -> Uuid {
    Uuid::new_v5(&CLOID_NAMESPACE, client_id.as_bytes())
}

pub struct HyperliquidBroker {
    exchange: ExchangeClient,
    info: InfoClient,
    address: H160,
    _wallet: LocalWallet,
}

impl HyperliquidBroker {
    pub(crate) fn new(
        exchange: ExchangeClient,
        info: InfoClient,
        wallet: LocalWallet,
        address: H160,
    ) -> Self {
        Self { exchange, info, address, _wallet: wallet }
    }
}

#[async_trait]
impl Broker for HyperliquidBroker {
    fn name(&self) -> &str {
        "hyperliquid"
    }

    async fn get_orderbook(&self, symbol: &str, _depth: usize) -> Result<OrderBook, BotError> {
        let coin = convert::to_hl_coin(symbol);
        let resp = self
            .info
            .l2_snapshot(coin)
            .await
            .map_err(|e| BotError::exchange(e, true))?;
        Ok(convert::l2_snapshot_to_orderbook(&resp))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>, BotError> {
        let state = self
            .info
            .user_state(self.address)
            .await
            .map_err(|e| BotError::exchange(e, true))?;
        Ok(convert::user_state_to_balances(&state))
    }

    async fn get_positions(&self) -> Result<Vec<Position>, BotError> {
        let state = self
            .info
            .user_state(self.address)
            .await
            .map_err(|e| BotError::exchange(e, true))?;
        Ok(convert::user_state_to_positions(&state))
    }

    async fn get_open_orders(&self, _symbol: &str) -> Result<Vec<Order>, BotError> {
        let resp = self
            .info
            .open_orders(self.address)
            .await
            .map_err(|e| BotError::exchange(e, true))?;
        Ok(resp
            .iter()
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
            .map(|o| {
                let coin = convert::to_hl_coin(&o.symbol);
                let tif = match o.order_type {
                    OrderType::Limit => "Gtc",
                    OrderType::PostOnly => "Alo",
                    OrderType::Market => "Ioc",
                };
                let price_f64 = o.price.to_string().parse::<f64>().unwrap_or(0.0);
                let sz_f64 = o.quantity.to_string().parse::<f64>().unwrap_or(0.0);
                let cloid = o.client_id.as_deref().map(client_id_to_cloid);

                ClientOrderRequest {
                    asset: coin,
                    is_buy: o.side == Side::Buy,
                    reduce_only: o.reduce_only,
                    limit_px: price_f64,
                    sz: sz_f64,
                    cloid,
                    order_type: ClientOrder::Limit(ClientLimit { tif: tif.to_string() }),
                }
            })
            .collect();

        match self.exchange.bulk_order(requests, None).await {
            Ok(response) => Ok(parse_bulk_order_response(&response, orders)),
            Err(e) => {
                tracing::error!(error = %e, "HL bulk_order error");
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

fn parse_bulk_order_response(
    resp: &ExchangeResponseStatus,
    orders: &[NewOrder],
) -> Vec<OrderResult> {
    match resp {
        ExchangeResponseStatus::Err(msg) => orders
            .iter()
            .map(|o| OrderResult {
                order_id: String::new(),
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
                        Some(ExchangeDataStatus::Resting(r)) => (r.oid.to_string(), true, None),
                        Some(ExchangeDataStatus::Filled(f)) => (f.oid.to_string(), true, None),
                        Some(ExchangeDataStatus::Success) => (String::new(), true, None),
                        Some(ExchangeDataStatus::WaitingForFill) => (String::new(), true, None),
                        Some(ExchangeDataStatus::WaitingForTrigger) => (String::new(), true, None),
                        Some(ExchangeDataStatus::Error(msg)) => {
                            (String::new(), false, Some(msg.clone()))
                        }
                        None => (String::new(), false, Some("no status returned".to_string())),
                    };
                    OrderResult {
                        order_id: oid,
                        client_id: o.client_id.clone(),
                        success,
                        error,
                    }
                })
                .collect()
        }
    }
}
