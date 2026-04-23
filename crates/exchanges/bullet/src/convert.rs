use std::collections::BTreeMap;
use std::str::FromStr;

use bb_core::types::{ExchangeEvent, Order, OrderBook, OrderStatus, OrderType, Side};
use bullet_rust_sdk::types::{
    AggTradeMessage, BookTickerMessage, DepthUpdate, MarkPriceMessage, OrderUpdateData,
    OrderUpdateMessage, PriceLevel,
};
use rust_decimal::Decimal;

fn parse_level(level: &PriceLevel) -> Option<(Decimal, Decimal)> {
    let price = Decimal::from_str(&level.0).ok()?;
    let qty = Decimal::from_str(&level.1).ok()?;
    Some((price, qty))
}

const EXCHANGE_NAME: &str = "bullet";

/// Convert a `DepthUpdate` WS message to an `ExchangeEvent::BookUpdate`.
pub fn depth_to_event(depth: &DepthUpdate) -> ExchangeEvent {
    let bids = depth.bids.iter().filter_map(parse_level).collect::<BTreeMap<_, _>>();
    let asks = depth.asks.iter().filter_map(parse_level).collect::<BTreeMap<_, _>>();

    ExchangeEvent::BookUpdate {
        exchange: EXCHANGE_NAME.to_string(),
        symbol: depth.symbol.clone(),
        orderbook: OrderBook { bids, asks, last_update_id: depth.last_update_id },
    }
}

/// Convert a `BookTickerMessage` to an `ExchangeEvent::BookUpdate` (top of book only).
pub fn book_ticker_to_event(bt: &BookTickerMessage) -> ExchangeEvent {
    let mut bids = BTreeMap::new();
    let mut asks = BTreeMap::new();

    if let (Ok(price), Ok(qty)) =
        (Decimal::from_str(&bt.best_bid_price), Decimal::from_str(&bt.best_bid_qty))
    {
        bids.insert(price, qty);
    }
    if let (Ok(price), Ok(qty)) =
        (Decimal::from_str(&bt.best_ask_price), Decimal::from_str(&bt.best_ask_qty))
    {
        asks.insert(price, qty);
    }

    ExchangeEvent::BookUpdate {
        exchange: EXCHANGE_NAME.to_string(),
        symbol: bt.symbol.clone(),
        orderbook: OrderBook { bids, asks, last_update_id: bt.update_id },
    }
}

/// Convert a `MarkPriceMessage` to an `ExchangeEvent::MarkPrice`.
pub fn mark_price_to_event(mp: &MarkPriceMessage) -> ExchangeEvent {
    ExchangeEvent::MarkPrice {
        exchange: EXCHANGE_NAME.to_string(),
        symbol: mp.symbol.clone(),
        mark_price: Decimal::from_str(&mp.mark_price).unwrap_or_default(),
        funding_rate: Decimal::from_str(&mp.funding_rate).unwrap_or_default(),
    }
}

/// Convert an `AggTradeMessage` to an `ExchangeEvent::Trade`.
/// Only emits if the trade involves our account address.
pub fn agg_trade_to_event(trade: &AggTradeMessage, account_address: &str) -> Option<ExchangeEvent> {
    if trade.user_address != account_address {
        return None;
    }

    let side = if trade.side == "BUY" { Side::Buy } else { Side::Sell };

    Some(ExchangeEvent::Trade {
        exchange: EXCHANGE_NAME.to_string(),
        symbol: trade.symbol.clone(),
        order_id: trade.order_id.to_string(),
        client_id: trade.client_order_id.as_ref().map(|c| c.to_string()),
        side,
        price: Decimal::from_str(&trade.price).unwrap_or_default(),
        quantity: Decimal::from_str(&trade.quantity).unwrap_or_default(),
        is_maker: trade.is_buyer_maker,
    })
}

/// Convert an `OrderUpdateMessage` to an `ExchangeEvent::OrderUpdate`.
pub fn order_update_to_event(msg: &OrderUpdateMessage) -> ExchangeEvent {
    let (symbol, order_id, client_id, status_str, side_str, price, quantity, filled_quantity) =
        match &msg.order {
            OrderUpdateData::TradeFill(data) => (
                data.common.symbol.clone(),
                data.common.order_id.to_string(),
                data.common.client_order_id.as_ref().map(|c| c.to_string()),
                data.common.status.clone(),
                data.side.clone(),
                // On a TRADE fill, `data.price` / `data.quantity` are the
                // order's limit price / size (often omitted). The actual
                // execution price/size live in the `l` / `L` fields.
                data.last_filled_price.parse().unwrap_or_default(),
                data.last_filled_qty.parse().unwrap_or_default(),
                data.last_filled_qty.parse().unwrap_or_default(),
            ),
            OrderUpdateData::PlaceOrder(data) => (
                data.common.symbol.clone(),
                data.common.order_id.to_string(),
                data.common.client_order_id.as_ref().map(|c| c.to_string()),
                data.common.status.clone(),
                data.side.clone(),
                data.price.parse().unwrap_or_default(),
                data.quantity.parse().unwrap_or_default(),
                Decimal::ZERO,
            ),
            OrderUpdateData::Cancel(data) => (
                data.common.symbol.clone(),
                data.common.order_id.to_string(),
                data.common.client_order_id.as_ref().map(|c| c.to_string()),
                data.common.status.clone(),
                String::new(),
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            ),
        };

    let status = match status_str.as_str() {
        "NEW" => OrderStatus::Open,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "EXPIRED" | "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Open,
    };

    let side = match side_str.as_str() {
        "BUY" => Side::Buy,
        _ => Side::Sell,
    };

    ExchangeEvent::OrderUpdate {
        exchange: EXCHANGE_NAME.to_string(),
        order: Order {
            id: order_id,
            client_id,
            symbol,
            side,
            order_type: OrderType::Limit,
            price,
            quantity,
            filled_quantity,
            status,
        },
    }
}

/// Parse a REST orderbook (`Vec<Vec<String>>`) into a `BTreeMap<Decimal, Decimal>`.
pub fn parse_orderbook_levels(raw: &[Vec<String>]) -> BTreeMap<Decimal, Decimal> {
    raw.iter()
        .filter_map(|lvl| {
            let price = Decimal::from_str(lvl.first()?).ok()?;
            let qty = Decimal::from_str(lvl.get(1)?).ok()?;
            Some((price, qty))
        })
        .collect()
}
