use std::collections::BTreeMap;

use bb_core::types::{
    Balance, ExchangeEvent, Order, OrderBook, OrderStatus, OrderType, Position, Side,
};
use hyperliquid_rust_sdk::{L2BookData, L2SnapshotResponse, OrderUpdate, TradeInfo, UserStateResponse};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

const EXCHANGE_NAME: &str = "hyperliquid";

/// HL uses bare coin names ("BTC"), we use "BTC-USD".
pub fn to_bb_symbol(hl_coin: &str) -> String {
    format!("{hl_coin}-USD")
}

/// Strip "-USD" suffix to get HL coin name.
pub fn to_hl_coin(bb_symbol: &str) -> String {
    bb_symbol.strip_suffix("-USD").unwrap_or(bb_symbol).to_string()
}

/// Parse a string to Decimal, returning ZERO on failure.
pub fn parse_dec(s: &str) -> Decimal {
    s.parse().unwrap_or(Decimal::ZERO)
}

/// Parse an f64 to Decimal using `from_f64_retain` for precision.
pub fn f64_to_dec(v: f64) -> Decimal {
    Decimal::from_f64(v).unwrap_or(Decimal::ZERO)
}

// -- REST response conversions --

/// Convert an L2SnapshotResponse to an OrderBook.
pub fn l2_snapshot_to_orderbook(resp: &L2SnapshotResponse) -> OrderBook {
    let bids = resp
        .levels
        .first()
        .map(|levels| levels.iter().map(|l| (parse_dec(&l.px), parse_dec(&l.sz))).collect())
        .unwrap_or_default();
    let asks = resp
        .levels
        .get(1)
        .map(|levels| levels.iter().map(|l| (parse_dec(&l.px), parse_dec(&l.sz))).collect())
        .unwrap_or_default();
    OrderBook { bids, asks, last_update_id: resp.time }
}

/// Convert UserStateResponse to balances.
pub fn user_state_to_balances(resp: &UserStateResponse) -> Vec<Balance> {
    vec![Balance {
        asset: "USD".to_string(),
        available: parse_dec(&resp.withdrawable),
        total: parse_dec(&resp.margin_summary.account_value),
    }]
}

/// Convert UserStateResponse to positions.
pub fn user_state_to_positions(resp: &UserStateResponse) -> Vec<Position> {
    resp.asset_positions
        .iter()
        .filter_map(|ap| {
            let pos = &ap.position;
            let size = parse_dec(&pos.szi);
            if size.is_zero() {
                return None;
            }
            let side = if size > Decimal::ZERO { Some(Side::Buy) } else { Some(Side::Sell) };
            Some(Position {
                symbol: to_bb_symbol(&pos.coin),
                side,
                size: size.abs(),
                entry_price: pos.entry_px.as_deref().map(parse_dec).unwrap_or(Decimal::ZERO),
                unrealized_pnl: parse_dec(&pos.unrealized_pnl),
            })
        })
        .collect()
}

// -- WS event conversions --

/// Convert L2BookData (WS) to an ExchangeEvent::BookUpdate.
pub fn l2_book_to_event(data: &L2BookData) -> ExchangeEvent {
    let bids: BTreeMap<Decimal, Decimal> = data
        .levels
        .first()
        .map(|levels| levels.iter().map(|l| (parse_dec(&l.px), parse_dec(&l.sz))).collect())
        .unwrap_or_default();
    let asks: BTreeMap<Decimal, Decimal> = data
        .levels
        .get(1)
        .map(|levels| levels.iter().map(|l| (parse_dec(&l.px), parse_dec(&l.sz))).collect())
        .unwrap_or_default();

    ExchangeEvent::BookUpdate {
        exchange: EXCHANGE_NAME.to_string(),
        symbol: to_bb_symbol(&data.coin),
        orderbook: OrderBook { bids, asks, last_update_id: data.time },
    }
}

/// Convert a fill (TradeInfo) to an ExchangeEvent::Trade.
pub fn fill_to_event(fill: &TradeInfo) -> ExchangeEvent {
    let side = match fill.side.as_str() {
        "B" | "Buy" | "buy" => Side::Buy,
        _ => Side::Sell,
    };

    ExchangeEvent::Trade {
        exchange: EXCHANGE_NAME.to_string(),
        symbol: to_bb_symbol(&fill.coin),
        order_id: fill.oid.to_string(),
        client_id: fill.cloid.as_ref().map(|c| c.to_string()),
        side,
        price: parse_dec(&fill.px),
        quantity: parse_dec(&fill.sz),
        is_maker: !fill.crossed,
    }
}

/// Convert an OrderUpdate (WS) to an ExchangeEvent::OrderUpdate.
pub fn order_update_to_event(update: &OrderUpdate) -> ExchangeEvent {
    let order = &update.order;
    let side = match order.side.as_str() {
        "B" | "Buy" | "buy" => Side::Buy,
        _ => Side::Sell,
    };

    let status = match update.status.as_str() {
        "open" | "new" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "partiallyFilled" => OrderStatus::PartiallyFilled,
        _ => OrderStatus::Open,
    };

    // Infer filled quantity from orig_sz - sz (remaining).
    let orig = parse_dec(&order.orig_sz);
    let remaining = parse_dec(&order.sz);
    let filled = orig - remaining;

    ExchangeEvent::OrderUpdate {
        exchange: EXCHANGE_NAME.to_string(),
        order: Order {
            id: order.oid.to_string(),
            client_id: order.cloid.clone(),
            symbol: to_bb_symbol(&order.coin),
            side,
            order_type: OrderType::Limit,
            price: parse_dec(&order.limit_px),
            quantity: orig,
            filled_quantity: filled.max(Decimal::ZERO),
            status,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_roundtrip() {
        assert_eq!(to_bb_symbol("BTC"), "BTC-USD");
        assert_eq!(to_hl_coin("BTC-USD"), "BTC");
        assert_eq!(to_hl_coin("ETH-USD"), "ETH");
        // Edge case: no suffix
        assert_eq!(to_hl_coin("SOL"), "SOL");
    }

    #[test]
    fn parse_dec_handles_garbage() {
        assert_eq!(parse_dec("not_a_number"), Decimal::ZERO);
        assert_eq!(parse_dec("123.456"), Decimal::new(123_456, 3));
    }

    #[test]
    fn f64_to_dec_basic() {
        let d = f64_to_dec(50000.5);
        assert_eq!(d, Decimal::new(500_005, 1));
    }
}
