use serde::Deserialize;

/// Configuration for the Hyperliquid exchange adapter.
#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidConfig {
    /// Network to connect to: "mainnet" or "testnet".
    pub network: String,

    /// Ethereum private key as hex string (secp256k1, with or without "0x" prefix).
    /// Can be overridden via environment variable.
    #[serde(default)]
    pub private_key_hex: String,
}
