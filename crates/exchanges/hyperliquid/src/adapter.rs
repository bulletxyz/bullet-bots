use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::exchange::Exchange;
use bb_core::types::*;
use ethers::signers::{LocalWallet, Signer};
use ethers::types::H160;
use hyperliquid_rust_sdk::{
    BaseUrl, ClientCancelRequest, ClientLimit, ClientOrder, ClientOrderRequest, ExchangeClient,
    InfoClient, Message, Subscription,
};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::config::HyperliquidConfig;
use crate::convert;

/// Hyperliquid exchange adapter.
///
/// Uses `ExchangeClient` for order management and `InfoClient` for market data
/// and WebSocket subscriptions. WS events are bridged through an mpsc channel.
pub struct HyperliquidExchange {
    config: HyperliquidConfig,
    wallet: Option<LocalWallet>,
    address: Option<H160>,
    exchange_client: Option<ExchangeClient>,
    info_client: Option<InfoClient>,
    event_rx: Option<mpsc::UnboundedReceiver<ExchangeEvent>>,
}

impl HyperliquidExchange {
    pub fn new(config: HyperliquidConfig) -> Self {
        Self {
            config,
            wallet: None,
            address: None,
            exchange_client: None,
            info_client: None,
            event_rx: None,
        }
    }

    fn base_url(&self) -> BaseUrl {
        match self.config.network.as_str() {
            "mainnet" => BaseUrl::Mainnet,
            "testnet" => BaseUrl::Testnet,
            _ => BaseUrl::Testnet,
        }
    }

    fn exchange_client(&self) -> Result<&ExchangeClient, BotError> {
        self.exchange_client.as_ref().ok_or_else(|| BotError::not_connected("hyperliquid"))
    }

    fn info_client(&self) -> Result<&InfoClient, BotError> {
        self.info_client.as_ref().ok_or_else(|| BotError::not_connected("hyperliquid"))
    }

    fn address(&self) -> Result<H160, BotError> {
        self.address.ok_or_else(|| BotError::not_connected("hyperliquid"))
    }

}

#[async_trait]
impl Exchange for HyperliquidExchange {
    fn name(&self) -> &str {
        "hyperliquid"
    }

    async fn connect(&mut self) -> Result<(), BotError> {
        let key_hex = self.config.private_key_hex.strip_prefix("0x").unwrap_or(&self.config.private_key_hex);
        let wallet: LocalWallet = key_hex
            .parse()
            .map_err(|e| BotError::config(format!("Invalid HL private key: {e}")))?;

        let address = wallet.address();
        let base_url = self.base_url();

        let exchange_client = ExchangeClient::new(None, wallet.clone(), Some(base_url), None, None)
            .await
            .map_err(|e| BotError::exchange(e, true))?;

        let info_client = InfoClient::new(None, Some(base_url))
            .await
            .map_err(|e| BotError::exchange(e, true))?;

        tracing::info!(
            address = %format!("{address:?}"),
            network = %self.config.network,
            "Hyperliquid: connected"
        );

        self.wallet = Some(wallet);
        self.address = Some(address);
        self.exchange_client = Some(exchange_client);
        self.info_client = Some(info_client);
        Ok(())
    }

    async fn get_orderbook(&self, symbol: &str, _depth: usize) -> Result<OrderBook, BotError> {
        let coin = convert::to_hl_coin(symbol);
        let info = self.info_client()?;
        let resp =
            info.l2_snapshot(coin).await.map_err(|e| BotError::exchange(e, true))?;
        Ok(convert::l2_snapshot_to_orderbook(&resp))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>, BotError> {
        let info = self.info_client()?;
        let address = self.address()?;
        let state = info.user_state(address).await.map_err(|e| BotError::exchange(e, true))?;
        Ok(convert::user_state_to_balances(&state))
    }

    async fn get_positions(&self) -> Result<Vec<Position>, BotError> {
        let info = self.info_client()?;
        let address = self.address()?;
        let state = info.user_state(address).await.map_err(|e| BotError::exchange(e, true))?;
        Ok(convert::user_state_to_positions(&state))
    }

    async fn get_open_orders(&self, _symbol: &str) -> Result<Vec<Order>, BotError> {
        let info = self.info_client()?;
        let address = self.address()?;
        let resp =
            info.open_orders(address).await.map_err(|e| BotError::exchange(e, true))?;

        Ok(resp
            .iter()
            .map(|o| {
                let side = match o.side.as_str() {
                    "B" | "Buy" | "buy" => Side::Buy,
                    _ => Side::Sell,
                };
                Order {
                    id: o.oid.to_string(),
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

        let exchange = self.exchange_client()?;

        let requests: Vec<ClientOrderRequest> = orders
            .iter()
            .map(|o| {
                let coin = convert::to_hl_coin(&o.symbol);
                let tif = match o.order_type {
                    OrderType::Limit => "Gtc",
                    OrderType::PostOnly => "Alo",
                    OrderType::Market => "Ioc",
                };
                let price_f64 =
                    o.price.to_string().parse::<f64>().unwrap_or(0.0);
                let sz_f64 =
                    o.quantity.to_string().parse::<f64>().unwrap_or(0.0);

                ClientOrderRequest {
                    asset: coin,
                    is_buy: o.side == Side::Buy,
                    reduce_only: o.reduce_only,
                    limit_px: price_f64,
                    sz: sz_f64,
                    cloid: None,
                    order_type: ClientOrder::Limit(ClientLimit { tif: tif.to_string() }),
                }
            })
            .collect();

        match exchange.bulk_order(requests, None).await {
            Ok(resp) => {
                tracing::debug!(?resp, "HL orders placed");
                // The response is an ExchangeResponseStatus; on success we don't get
                // individual order IDs from the bulk endpoint, so we mark all as success.
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
                tracing::error!(error = %e, "HL failed to place orders");
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

        let exchange = self.exchange_client()?;

        // Hyperliquid's SDK only supports cancel-by-exchange-order-id. Entries
        // with an empty `order_id` (e.g. client_id-only cancels produced by
        // the grid strategy's soft reconciler) would be silently dropped
        // here, so warn loudly if we see any — the caller's intent will
        // not be honored.
        let mut dropped_client_only = 0usize;
        let requests: Vec<ClientCancelRequest> = cancels
            .iter()
            .filter_map(|c| {
                let Some(oid) = c.order_id.parse::<u64>().ok() else {
                    if c.client_id.is_some() {
                        dropped_client_only += 1;
                    }
                    return None;
                };
                let coin = convert::to_hl_coin(&c.symbol);
                Some(ClientCancelRequest { asset: coin, oid })
            })
            .collect();
        if dropped_client_only > 0 {
            tracing::warn!(
                dropped = dropped_client_only,
                "Hyperliquid cancel_orders: dropped client_id-only cancels (SDK has no client_id path). \
                 These orders will not be cancelled until their exchange order_id is known."
            );
        }

        match exchange.bulk_cancel(requests, None).await {
            Ok(_) => Ok(cancels
                .iter()
                .map(|c| CancelResult { order_id: c.order_id.clone(), success: true, error: None })
                .collect()),
            Err(e) => {
                tracing::error!(error = %e, "HL failed to cancel orders");
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
        // HL doesn't have a "cancel all" endpoint, so we fetch open orders and cancel them.
        let open = self.get_open_orders(symbol).await?;
        if open.is_empty() {
            return Ok(());
        }
        let cancels: Vec<CancelOrder> =
            open.iter()
                .map(|o| CancelOrder {
                    symbol: symbol.to_string(),
                    order_id: o.id.clone(),
                    client_id: None,
                })
                .collect();
        self.cancel_orders(&cancels).await?;
        Ok(())
    }

    async fn subscribe(&mut self, symbol: &str) -> Result<(), BotError> {
        let coin = convert::to_hl_coin(symbol);
        let address = self.address()?;

        // Create a fresh InfoClient with reconnect support for WS.
        let base_url = self.base_url();
        let mut ws_info = InfoClient::with_reconnect(None, Some(base_url))
            .await
            .map_err(|e| BotError::exchange(e, true))?;

        // Channel for the HL SDK to push raw Messages into.
        let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<Message>();

        // Subscribe to L2 book, order updates, user fills, and active asset context (funding).
        ws_info
            .subscribe(Subscription::L2Book { coin: coin.clone() }, ws_tx.clone())
            .await
            .map_err(|e| BotError::exchange(e, false))?;
        ws_info
            .subscribe(Subscription::OrderUpdates { user: address }, ws_tx.clone())
            .await
            .map_err(|e| BotError::exchange(e, false))?;
        ws_info
            .subscribe(Subscription::UserFills { user: address }, ws_tx.clone())
            .await
            .map_err(|e| BotError::exchange(e, false))?;
        ws_info
            .subscribe(Subscription::AllMids, ws_tx.clone())
            .await
            .map_err(|e| BotError::exchange(e, false))?;

        tracing::info!(symbol, coin = %coin, "Hyperliquid: subscribed to WS feeds");

        // Bridge: background task converts HL Messages -> ExchangeEvents.
        let (event_tx, event_rx) = mpsc::unbounded_channel::<ExchangeEvent>();
        let target_coin = coin.clone();

        tokio::spawn(async move {
            // Keep ws_info alive so the WS connection persists.
            let _ws_info = ws_info;

            while let Some(msg) = ws_rx.recv().await {
                let events = match msg {
                    Message::L2Book(ref book) => {
                        if book.data.coin == target_coin {
                            vec![convert::l2_book_to_event(&book.data)]
                        } else {
                            vec![]
                        }
                    }
                    Message::OrderUpdates(ref updates) => updates
                        .data
                        .iter()
                        .filter(|u| u.order.coin == target_coin)
                        .map(convert::order_update_to_event)
                        .collect(),
                    Message::UserFills(ref fills) => fills
                        .data
                        .fills
                        .iter()
                        .filter(|f| f.coin == target_coin)
                        .map(convert::fill_to_event)
                        .collect(),
                    Message::AllMids(ref mids) => {
                        // Extract mid price for our coin. AllMids doesn't include
                        // funding rates directly, so we emit mark price with zero
                        // funding rate. Funding rates come from ActiveAssetCtx or
                        // REST queries by the strategy.
                        if let Some(mid_str) = mids.data.mids.get(&target_coin) {
                            let mark_price =
                                mid_str.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                            vec![ExchangeEvent::MarkPrice {
                                exchange: "hyperliquid".to_string(),
                                symbol: convert::to_bb_symbol(&target_coin),
                                mark_price,
                                funding_rate: Decimal::ZERO,
                            }]
                        } else {
                            vec![]
                        }
                    }
                    _ => vec![],
                };

                for event in events {
                    if event_tx.send(event).is_err() {
                        return; // receiver dropped, shut down
                    }
                }
            }

            tracing::warn!("Hyperliquid: WS bridge task ended");
            let _ = event_tx.send(ExchangeEvent::Disconnected {
                exchange: "hyperliquid".to_string(),
            });
        });

        self.event_rx = Some(event_rx);
        Ok(())
    }

    async fn recv_event(&mut self) -> Option<ExchangeEvent> {
        let rx = self.event_rx.as_mut()?;
        rx.recv().await
    }
}
