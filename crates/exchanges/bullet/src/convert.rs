//! Shared value-conversion helpers used by the Bullet broker + connection.

use std::collections::BTreeMap;
use std::str::FromStr;

use bb_core::events::{BookUpdate, MarkPriceUpdate, OrderLifecycle, Trade};
use bb_core::helpers::parse_decimal_or_warn;
use bb_core::types::{Order, OrderBook, OrderStatus, OrderType, Side};
use bullet_rust_sdk::types::{
    BookTickerMessage, DepthUpdate, MarkPriceMessage, OrderUpdateData, OrderUpdateMessage,
    PriceLevel,
};
use rust_decimal::Decimal;

const EXCHANGE: &str = "bullet";

/// Parse a `[price, qty]` price level into typed decimals. Returns `None` if
/// either string fails to parse.
pub fn parse_level_tuple(level: &PriceLevel) -> Option<(Decimal, Decimal)> {
    let price = Decimal::from_str(&level.0).ok()?;
    let qty = Decimal::from_str(&level.1).ok()?;
    Some((price, qty))
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

pub fn depth_to_event(depth: &DepthUpdate) -> BookUpdate {
    let bids = depth.bids.iter().filter_map(parse_level_tuple).collect();
    let asks = depth.asks.iter().filter_map(parse_level_tuple).collect();
    BookUpdate {
        exchange: EXCHANGE.into(),
        symbol: depth.symbol.clone(),
        orderbook: OrderBook { bids, asks, last_update_id: depth.last_update_id },
    }
}

pub fn book_ticker_to_event(bt: &BookTickerMessage) -> BookUpdate {
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
    BookUpdate {
        exchange: EXCHANGE.into(),
        symbol: bt.symbol.clone(),
        orderbook: OrderBook { bids, asks, last_update_id: bt.update_id },
    }
}

/// Returns `None` if `mark_price` fails to parse (skip the update entirely).
/// A bad mark price would silently corrupt PnL accounting downstream.
pub fn mark_price_to_event(mp: &MarkPriceMessage) -> Option<MarkPriceUpdate> {
    let mark_price = parse_decimal_or_warn(&mp.mark_price, "mark_price")?;
    // funding_rate: None means parse failed / field absent — not the same as zero funding
    let funding_rate = parse_decimal_or_warn(&mp.funding_rate, "funding_rate");
    Some(MarkPriceUpdate {
        exchange: EXCHANGE.into(),
        symbol: mp.symbol.clone(),
        mark_price,
        funding_rate,
    })
}

/// Extract a `Trade` from `OrderUpdateData::TradeFill`. Returns `None` for
/// PlaceOrder / Cancel variants (no execution) or unknown sides (logged).
pub fn order_update_to_trade(msg: &OrderUpdateMessage) -> Option<Trade> {
    let OrderUpdateData::TradeFill(data) = &msg.order else { return None };
    let side = match data.side.as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        other => {
            tracing::warn!(side = other, "Bullet: unknown side in TradeFill — skipping trade");
            return None;
        }
    };
    let price = parse_decimal_or_warn(&data.last_filled_price, "last_filled_price")?;
    let quantity = parse_decimal_or_warn(&data.last_filled_qty, "last_filled_qty")?;
    Some(Trade {
        exchange: EXCHANGE.into(),
        symbol: data.common.symbol.clone(),
        order_id: data.common.order_id.to_string(),
        trade_id: None, // Bullet SDK does not expose a per-fill trade ID
        client_id: data.common.client_order_id.as_ref().map(|c| c.to_string()),
        side,
        price,
        quantity,
        timestamp: None, // Bullet SDK does not expose a per-fill timestamp
    })
}

/// Extract an `OrderLifecycle` update from any `OrderUpdate` variant.
///
/// `price` / `quantity` on the resulting `Order` always carry the *original*
/// order's limit price and size — never per-fill execution values.
///
/// Note: Cancel events do not carry the original side in the current SDK.
/// We default to `Side::Buy` for Cancel; strategies should not rely on the
/// side field of a Cancel lifecycle event.
pub fn order_update_to_lifecycle(msg: &OrderUpdateMessage) -> OrderLifecycle {
    let (symbol, order_id, client_id, status_str, side_str, price, quantity, filled_quantity) =
        match &msg.order {
            OrderUpdateData::TradeFill(data) => (
                data.common.symbol.clone(),
                data.common.order_id.to_string(),
                data.common.client_order_id.as_ref().map(|c| c.to_string()),
                data.common.status.clone(),
                data.side.clone(),
                data.price.as_deref().and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO),
                data.quantity.as_deref().and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO),
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
                String::new(), // SDK Cancel does not surface the original side
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
        "SELL" => Side::Sell,
        "" => Side::Buy, // Cancel events — no side in SDK response
        other => {
            tracing::warn!(
                side = other,
                "Bullet: unknown side in OrderLifecycle — defaulting to Buy"
            );
            Side::Buy
        }
    };
    // TODO(P1-15): read the actual order type from the SDK update when the field is available.
    // PostOnly orders currently surface as Limit, which affects fee accounting.
    OrderLifecycle {
        exchange: EXCHANGE.into(),
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
