use secrecy::SecretString;
use serde::Deserialize;

/// Configuration for the Bullet exchange adapter.
///
/// `private_key_hex` is wrapped in [`SecretString`] so that `Debug` formatting
/// emits `"[REDACTED alloc::string::String]"` instead of the raw key, and the
/// hex string is zeroed on drop. To read the value, call
/// `config.private_key_hex.expose_secret()`.
#[derive(Debug, Clone, Deserialize)]
pub struct BulletConfig {
    /// Network to connect to: "mainnet" or "testnet".
    pub network: String,

    /// Ed25519 private key as hex string (with or without "0x" prefix).
    /// Can be overridden via environment variable.
    #[serde(default = "default_secret")]
    pub private_key_hex: SecretString,
}

fn default_secret() -> SecretString {
    SecretString::new(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    const FAKE_KEY: &str = "deadbeefcafef00d0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn debug_redacts_private_key() {
        let cfg = BulletConfig {
            network: "testnet".into(),
            private_key_hex: SecretString::new(FAKE_KEY.to_string()),
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains(FAKE_KEY), "Debug output must not contain key: {dbg}");
        assert!(dbg.to_lowercase().contains("redacted"), "Debug should show REDACTED: {dbg}");
    }

    #[test]
    fn deserializes_from_toml_string() {
        let toml_src = format!(r#"
            network = "testnet"
            private_key_hex = "{FAKE_KEY}"
        "#);
        let cfg: BulletConfig = toml::from_str(&toml_src).expect("parse");
        assert_eq!(cfg.private_key_hex.expose_secret(), FAKE_KEY);
    }

    #[test]
    fn missing_key_defaults_to_empty() {
        let cfg: BulletConfig = toml::from_str(r#"network = "testnet""#).expect("parse");
        assert_eq!(cfg.private_key_hex.expose_secret(), "");
    }
}
