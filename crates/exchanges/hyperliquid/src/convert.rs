//! Shared value-conversion helpers used by the Hyperliquid broker + connection.

use bb_core::events::{BookUpdate, MarkPriceUpdate, OrderLifecycle, Trade};
use bb_core::helpers::parse_decimal_or_warn;
use bb_core::types::{Balance, Order, OrderBook, OrderStatus, OrderType, Position, Side};
use hyperliquid_rust_sdk::{
    ActiveAssetCtxData, AssetCtx, L2BookData, L2SnapshotResponse, OrderUpdate, TradeInfo,
    UserStateResponse, UserTokenBalanceResponse,
};
use rust_decimal::Decimal;

use crate::broker::{ClientIdMap, original_client_id};

const EXCHANGE: &str = "hyperliquid";

/// HL uses bare coin names ("BTC"); we use "BTC-USD".
pub fn to_bb_symbol(hl_coin: &str) -> String {
    format!("{hl_coin}-USD")
}

/// Strip "-USD" suffix to get the HL coin name.
pub fn to_hl_coin(bb_symbol: &str) -> String {
    bb_symbol.strip_suffix("-USD").unwrap_or(bb_symbol).to_string()
}

/// Parse a decimal string, returning `Decimal::ZERO` on failure. Used at the
/// HL SDK boundary where prices and sizes arrive as strings.
pub fn parse_dec(s: &str) -> Decimal {
    s.parse().unwrap_or(Decimal::ZERO)
}

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

pub fn user_state_to_balances(resp: &UserStateResponse) -> Vec<Balance> {
    vec![Balance {
        asset: "USD".to_string(),
        available: parse_dec(&resp.withdrawable),
        total: parse_dec(&resp.margin_summary.account_value),
    }]
}

/// Balances from the spot clearinghouse — used for unified accounts, where the
/// USDC collateral lives in the spot balance rather than the perp account.
/// `available = total - hold` (hold = margin locked in open positions).
pub fn spot_state_to_balances(resp: &UserTokenBalanceResponse) -> Vec<Balance> {
    resp.balances
        .iter()
        .map(|b| {
            let total = parse_dec(&b.total);
            let hold = parse_dec(&b.hold);
            Balance { asset: b.coin.clone(), available: total - hold, total }
        })
        .filter(|b| !b.total.is_zero())
        .collect()
}

pub fn user_state_to_positions(resp: &UserStateResponse) -> Vec<Position> {
    resp.asset_positions
        .iter()
        .filter_map(|ap| {
            let pos = &ap.position;
            let net_sz = parse_dec(&pos.szi);
            if net_sz.is_zero() {
                return None;
            }
            let side = if net_sz > Decimal::ZERO { Some(Side::Buy) } else { Some(Side::Sell) };
            Some(Position {
                symbol: to_bb_symbol(&pos.coin),
                side,
                size: net_sz.abs(),
                entry_price: pos.entry_px.as_deref().map_or(Decimal::ZERO, parse_dec),
                unrealized_pnl: parse_dec(&pos.unrealized_pnl),
            })
        })
        .collect()
}

pub fn l2_book_to_event(data: &L2BookData) -> BookUpdate {
    use std::collections::BTreeMap;
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
    BookUpdate {
        exchange: EXCHANGE.into(),
        symbol: to_bb_symbol(&data.coin),
        orderbook: OrderBook { bids, asks, last_update_id: data.time },
    }
}

pub fn fill_to_trade(fill: &TradeInfo, client_ids: &ClientIdMap) -> Option<Trade> {
    let side = match fill.side.as_str() {
        "B" | "Buy" | "buy" => Side::Buy,
        "A" | "Sell" | "sell" => Side::Sell,
        other => {
            tracing::warn!(side = other, "HL: unknown side in UserFill — skipping trade");
            return None;
        }
    };
    let price = parse_decimal_or_warn(&fill.px, "px")?;
    let quantity = parse_decimal_or_warn(&fill.sz, "sz")?;
    Some(Trade {
        exchange: EXCHANGE.into(),
        symbol: to_bb_symbol(&fill.coin),
        order_id: fill.oid.to_string(),
        trade_id: Some(fill.tid.to_string()),
        client_id: fill.cloid.as_ref().map(|c| original_client_id(client_ids, c)),
        side,
        price,
        quantity,
        timestamp: Some(fill.time),
    })
}

pub fn order_update_to_lifecycle(update: &OrderUpdate, client_ids: &ClientIdMap) -> OrderLifecycle {
    let order = &update.order;
    let side = match order.side.as_str() {
        "B" | "Buy" | "buy" => Side::Buy,
        "A" | "Sell" | "sell" => Side::Sell,
        other => {
            tracing::warn!(side = other, "HL: unknown side in OrderUpdate — defaulting to Buy");
            Side::Buy
        }
    };
    let status = match update.status.as_str() {
        "open" | "new" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "partiallyFilled" => OrderStatus::PartiallyFilled,
        // HL may surface terminal statuses we haven't enumerated (e.g.
        // liquidation-cancelled, margin-cancelled). We default to `Open`
        // so the strategy doesn't act on it — periodic REST reconcile will
        // catch the discrepancy. Surface the status so we know to add the
        // proper mapping.
        other => {
            tracing::warn!(
                status = other,
                oid = order.oid,
                "Unknown HL order status — defaulting to Open"
            );
            OrderStatus::Open
        }
    };
    let orig = parse_dec(&order.orig_sz);
    let remaining = parse_dec(&order.sz);
    let filled = (orig - remaining).max(Decimal::ZERO);

    // TODO(P1-15): read the actual order type from the SDK update when the field is available.
    // PostOnly orders currently surface as Limit, which affects fee accounting.
    OrderLifecycle {
        exchange: EXCHANGE.into(),
        order: Order {
            id: order.oid.to_string(),
            client_id: order.cloid.as_ref().map(|c| original_client_id(client_ids, c)),
            symbol: to_bb_symbol(&order.coin),
            side,
            order_type: OrderType::Limit,
            price: parse_dec(&order.limit_px),
            quantity: orig,
            filled_quantity: filled,
            status,
        },
    }
}

pub fn active_asset_ctx_to_mark(data: &ActiveAssetCtxData) -> Option<MarkPriceUpdate> {
    let (mark, funding) = match &data.ctx {
        AssetCtx::Perps(p) => (
            parse_decimal_or_warn(&p.shared.mark_px, "mark_px")?,
            parse_decimal_or_warn(&p.funding, "funding"), // None = parse failed, not zero funding
        ),
        AssetCtx::Spot(s) => (parse_decimal_or_warn(&s.shared.mark_px, "mark_px")?, None),
    };
    Some(MarkPriceUpdate {
        exchange: EXCHANGE.into(),
        symbol: to_bb_symbol(&data.coin),
        mark_price: mark,
        funding_rate: funding,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_roundtrip() {
        assert_eq!(to_bb_symbol("BTC"), "BTC-USD");
        assert_eq!(to_hl_coin("BTC-USD"), "BTC");
        assert_eq!(to_hl_coin("SOL"), "SOL");
    }

    #[test]
    fn parse_dec_handles_garbage() {
        assert_eq!(parse_dec("not_a_number"), Decimal::ZERO);
        assert_eq!(parse_dec("123.456"), Decimal::new(123_456, 3));
    }

    #[test]
    fn spot_balances_use_total_minus_hold_and_drop_zero() {
        use hyperliquid_rust_sdk::{UserTokenBalance, UserTokenBalanceResponse};
        let resp = UserTokenBalanceResponse {
            balances: vec![
                UserTokenBalance {
                    coin: "USDC".to_string(),
                    hold: "2.04".to_string(),
                    total: "997.59".to_string(),
                    entry_ntl: "0.0".to_string(),
                },
                UserTokenBalance {
                    coin: "TZERO".to_string(),
                    hold: "0.0".to_string(),
                    total: "0.0".to_string(),
                    entry_ntl: "0.0".to_string(),
                },
            ],
        };
        let bals = spot_state_to_balances(&resp);
        assert_eq!(bals.len(), 1, "zero-total balances dropped");
        assert_eq!(bals[0].asset, "USDC");
        assert_eq!(bals[0].total, Decimal::new(99759, 2));
        assert_eq!(bals[0].available, Decimal::new(99555, 2)); // 997.59 - 2.04
    }
}
