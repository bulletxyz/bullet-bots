use std::path::PathBuf;

use secrecy::SecretString;
use serde::Deserialize;

/// Configuration for the Bullet exchange adapter.
///
/// Key material can be supplied two ways, in this preference order:
///
/// 1. **`key_file`** — path to a Solana-compatible JSON keystore (as produced by `bb-bot keygen` or
///    `solana-keygen`). Preferred: the key lives on disk with whatever permissions the filesystem
///    enforces, never hits the shell history, and isn't trivially exfiltrated via a process
///    environment dump.
/// 2. **`private_key_hex`** — Ed25519 secret as a hex string. Wrapped in [`SecretString`] so it's
///    redacted in `Debug` output and zeroed on drop. Still supported for CI / ephemeral contexts;
///    the env var `BB_BULLET_PRIVATE_KEY_HEX` populates this field.
///
/// If both are set, `key_file` wins.
#[derive(Debug, Clone, Deserialize)]
pub struct BulletConfig {
    /// Network to connect to: "mainnet" or "testnet".
    pub network: String,

    /// Path to a Solana-compatible JSON keystore file. Takes precedence over
    /// `private_key_hex` when set.
    #[serde(default)]
    pub key_file: Option<PathBuf>,

    /// Ed25519 private key as hex string (with or without "0x" prefix).
    /// Only used if `key_file` is not set.
    #[serde(default = "default_secret")]
    pub private_key_hex: SecretString,
}

fn default_secret() -> SecretString {
    SecretString::new(String::new())
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    const FAKE_KEY: &str = "deadbeefcafef00d0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn debug_redacts_private_key() {
        let cfg = BulletConfig {
            network: "testnet".into(),
            key_file: None,
            private_key_hex: SecretString::new(FAKE_KEY.to_string()),
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
            private_key_hex = "{FAKE_KEY}"
        "#
        );
        let cfg: BulletConfig = toml::from_str(&toml_src).expect("parse");
        assert_eq!(cfg.private_key_hex.expose_secret(), FAKE_KEY);
        assert!(cfg.key_file.is_none());
    }

    #[test]
    fn deserializes_key_file_path() {
        let toml_src = r#"
            network = "testnet"
            key_file = "/tmp/my-keypair.json"
        "#;
        let cfg: BulletConfig = toml::from_str(toml_src).expect("parse");
        assert_eq!(cfg.key_file.as_deref(), Some(std::path::Path::new("/tmp/my-keypair.json")));
        assert_eq!(cfg.private_key_hex.expose_secret(), "");
    }

    #[test]
    fn missing_key_defaults_to_empty() {
        let cfg: BulletConfig = toml::from_str(r#"network = "testnet""#).expect("parse");
        assert_eq!(cfg.private_key_hex.expose_secret(), "");
        assert!(cfg.key_file.is_none());
    }
}
