use secrecy::SecretString;
use serde::Deserialize;

/// Configuration for the Hyperliquid exchange adapter.
///
/// `private_key` is wrapped in [`SecretString`] so that `Debug` formatting
/// emits `"[REDACTED alloc::string::String]"` instead of the raw key, and the
/// hex string is zeroed on drop. To read the value, call
/// `config.private_key.expose_secret()`.
#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidConfig {
    /// Network to connect to: "mainnet" or "testnet".
    pub network: String,

    /// Ethereum private key as hex string (secp256k1, with or without "0x" prefix).
    /// Can be overridden via environment variable.
    #[serde(default = "default_secret")]
    pub private_key: SecretString,

    /// Master/main account address (`0x`-prefixed H160 hex) to read positions,
    /// balances, and fills from, and to subscribe to.
    ///
    /// Set this when `private_key` is an **API / agent wallet** key: the
    /// agent signs orders (the exchange attributes them to the master account
    /// on-chain), but all account state lives on the master account, not the
    /// agent address. Leave unset when the key *is* the main wallet — reads
    /// then default to the wallet's own address. Env:
    /// `BB_HYPERLIQUID_ACCOUNT_ADDRESS`.
    #[serde(default)]
    pub account_address: Option<String>,
}

fn default_secret() -> SecretString {
    SecretString::new(String::new())
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    const FAKE_KEY: &str = "0xdeadbeefcafef00d0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn debug_redacts_private_key() {
        let cfg = HyperliquidConfig {
            network: "testnet".into(),
            private_key: SecretString::new(FAKE_KEY.to_string()),
            account_address: None,
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains(FAKE_KEY), "Debug output must not contain key: {dbg}");
        assert!(dbg.to_lowercase().contains("redacted"), "Debug should show REDACTED: {dbg}");
    }

    #[test]
    fn deserializes_from_toml_string() {
        let toml_src = format!(
            r#"
            network = "testnet"
            private_key = "{FAKE_KEY}"
        "#
        );
        let cfg: HyperliquidConfig = toml::from_str(&toml_src).expect("parse");
        assert_eq!(cfg.private_key.expose_secret(), FAKE_KEY);
    }

    #[test]
    fn missing_key_defaults_to_empty() {
        let cfg: HyperliquidConfig = toml::from_str(r#"network = "testnet""#).expect("parse");
        assert_eq!(cfg.private_key.expose_secret(), "");
        assert!(cfg.account_address.is_none());
    }

    #[test]
    fn deserializes_account_address() {
        let toml_src = r#"
            network = "testnet"
            account_address = "0x1111111111111111111111111111111111111111"
        "#;
        let cfg: HyperliquidConfig = toml::from_str(toml_src).expect("parse");
        assert_eq!(
            cfg.account_address.as_deref(),
            Some("0x1111111111111111111111111111111111111111")
        );
    }
}
