//! Resolve a signer's account address via the Bullet `delegateOf` endpoint.
//!
//! A delegate (API) key has no account of its own — all balances, positions,
//! and orders live on the master ("parent") account. Reads and the user-orders
//! subscription must target that master address. A non-delegate key is its own
//! account, which the endpoint reports as `404`.

use bb_core::error::BotError;
use serde::Deserialize;

#[derive(Deserialize)]
struct DelegateOf {
    parent: String,
}

/// Resolve the account address to use for reads/subscriptions.
///
/// `base_url` is the REST base (e.g. `Client::url()`), `signer` the signer's
/// own base58 address. Returns the master `parent` for a delegate key, or
/// `signer` unchanged if it is not a delegate.
pub async fn resolve_account_address(base_url: &str, signer: &str) -> Result<String, BotError> {
    let url = format!("{}/api/v1/delegateOf", base_url.trim_end_matches('/'));
    let resp = reqwest::Client::new()
        .get(&url)
        .query(&[("address", signer)])
        .send()
        .await
        .map_err(|e| BotError::exchange(format!("delegateOf request failed: {e}"), true))?;
    let status = resp.status().as_u16();
    let body = resp
        .text()
        .await
        .map_err(|e| BotError::exchange(format!("delegateOf body read failed: {e}"), true))?;
    account_address_from(signer, status, &body)
}

/// Pure mapping from an HTTP response to an account address.
fn account_address_from(signer: &str, status: u16, body: &str) -> Result<String, BotError> {
    match status {
        200 => {
            let parsed: DelegateOf = serde_json::from_str(body).map_err(|e| {
                BotError::exchange(format!("delegateOf response parse error: {e}"), false)
            })?;
            Ok(parsed.parent)
        }
        404 => Ok(signer.to_string()),
        other => {
            // 5xx and 429 are transient (server/rate-limit); other 4xx are not.
            let retryable = other >= 500 || other == 429;
            Err(BotError::exchange(format!("delegateOf returned HTTP {other}: {body}"), retryable))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::account_address_from;

    #[test]
    fn delegate_resolves_to_parent() {
        let body = r#"{"parent":"MASTER_ADDR","name":"bot","flags":1,"expiresAt":null}"#;
        let got = account_address_from("DELEGATE_ADDR", 200, body).expect("200");
        assert_eq!(got, "MASTER_ADDR");
    }

    #[test]
    fn not_a_delegate_resolves_to_self() {
        let got = account_address_from("SELF_ADDR", 404, "{}").expect("404");
        assert_eq!(got, "SELF_ADDR");
    }

    #[test]
    fn server_error_is_retryable() {
        let err = account_address_from("X", 500, "boom").expect_err("500");
        assert!(err.is_retryable(), "5xx should be retryable");
        let err = account_address_from("X", 429, "slow down").expect_err("429");
        assert!(err.is_retryable(), "429 should be retryable");
    }

    #[test]
    fn client_error_is_not_retryable() {
        let err = account_address_from("X", 400, "bad").expect_err("400");
        assert!(!err.is_retryable(), "4xx (non-429) should not be retryable");
    }
}
