//! Shared value-conversion helpers used by the Hyperliquid broker + connection.

use bb_core::types::{Balance, OrderBook, Position, Side};
use hyperliquid_rust_sdk::{L2SnapshotResponse, UserStateResponse};
use rust_decimal::Decimal;

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
}
