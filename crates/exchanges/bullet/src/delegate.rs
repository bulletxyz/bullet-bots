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
/// One `delegateOf` attempt: HTTP GET + map the response to an account address.
async fn resolve_account_once(base_url: &str, signer: &str) -> Result<String, BotError> {
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

pub async fn resolve_account_address(base_url: &str, signer: &str) -> Result<String, BotError> {
    // Retry transient failures (network / 5xx / 429) so a brief API blip at
    // startup doesn't block a delegate key; non-retryable errors fail fast.
    let mut attempt = 0;
    let account = loop {
        attempt += 1;
        match resolve_account_once(base_url, signer).await {
            Ok(a) => break a,
            Err(e) if e.is_retryable() && attempt < 3 => {
                tracing::warn!(attempt, error = %e, "delegateOf transient failure; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => return Err(e),
        }
    };
    if account == signer {
        // Not a registered delegate on this network. Valid for a main-wallet
        // key, but it's also what a delegate key looks like when the bot is
        // pointed at the wrong network — surface a hint so the otherwise opaque
        // "account not found" failures downstream are easier to diagnose.
        tracing::info!(
            signer,
            base_url,
            "Bullet: signer is not a registered delegate here; using it as its own account. \
             If you meant to use a delegate key, check the network matches where it was \
             registered."
        );
    } else {
        tracing::info!(signer, master = %account, "Bullet: resolved delegate to master account");
    }
    Ok(account)
}

/// Pure mapping from an HTTP response to an account address.
///
/// "Not a delegate" (i.e. the key is its own account) is reported by the API in
/// two ways: the published API spec documents `404`, but the live mainnet/testnet
/// servers return `400` with an `"is not a delegate"` message. Both mean self.
/// Note `400` is also used for a malformed address (`"invalid address"`), which
/// is a genuine error — so we key off the message, not the bare status.
fn account_address_from(signer: &str, status: u16, body: &str) -> Result<String, BotError> {
    if status == 200 {
        let parsed: DelegateOf = serde_json::from_str(body).map_err(|e| {
            BotError::exchange(format!("delegateOf response parse error: {e}"), false)
        })?;
        return Ok(parsed.parent);
    }
    if status == 404 || (status == 400 && body.contains("is not a delegate")) {
        return Ok(signer.to_string());
    }
    // 5xx and 429 are transient (server/rate-limit); other statuses are not.
    let retryable = status >= 500 || status == 429;
    Err(BotError::exchange(format!("delegateOf returned HTTP {status}: {body}"), retryable))
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
        // Spec says 404; live mainnet/testnet return 400 "is not a delegate".
        let got = account_address_from("SELF_ADDR", 404, "{}").expect("404");
        assert_eq!(got, "SELF_ADDR");
        let body = r#"{"status":400,"message":"Bad request: SELF_ADDR is not a delegate"}"#;
        let got = account_address_from("SELF_ADDR", 400, body).expect("400 not-a-delegate");
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
    fn malformed_address_400_is_error() {
        // A 400 that is NOT "not a delegate" (e.g. invalid address) is a real
        // error, not a resolve-to-self.
        let body = r#"{"status":400,"message":"Bad request: invalid address: xyz"}"#;
        let err = account_address_from("X", 400, body).expect_err("400 invalid");
        assert!(!err.is_retryable(), "4xx (non-429) should not be retryable");
    }
}
