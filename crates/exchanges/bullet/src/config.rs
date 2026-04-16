use serde::Deserialize;

/// Configuration for the Bullet exchange adapter.
#[derive(Debug, Clone, Deserialize)]
pub struct BulletConfig {
    /// Network to connect to: "mainnet" or "testnet".
    pub network: String,

    /// Ed25519 private key as hex string (with or without "0x" prefix).
    /// Can be overridden via environment variable.
    #[serde(default)]
    pub private_key_hex: String,
}
