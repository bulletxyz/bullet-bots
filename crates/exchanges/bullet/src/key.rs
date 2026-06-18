//! Parse an Ed25519 signer secret from hex or base58 into a `Keypair`.

use bb_core::error::BotError;
use bullet_rust_sdk::Keypair;

/// Parse a Bullet signer secret into a [`Keypair`].
///
/// Accepted formats:
/// - **Hex**: 64 hex chars, optionally `0x`-prefixed (Bullet/Solana-CLI hex).
/// - **Base58**: decodes to 32 bytes (raw seed) or 64 bytes (Phantom / Solana full keypair, where
///   the first 32 bytes are the secret seed).
pub fn keypair_from_secret(secret: &str) -> Result<Keypair, BotError> {
    let s = secret.trim();
    let hex_body = s.strip_prefix("0x").unwrap_or(s);
    if hex_body.len() == 64 && hex_body.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Keypair::from_hex(hex_body)
            .map_err(|e| BotError::config(format!("Invalid hex private key: {e}")));
    }

    let bytes = bs58::decode(s).into_vec().map_err(|e| {
        BotError::config(format!("Key is neither 64-char hex nor valid base58: {e}"))
    })?;

    // Phantom/Solana export is 64 bytes (seed ++ pubkey); a raw seed is 32.
    let seed: [u8; 32] = bytes
        .get(..32)
        .filter(|_| matches!(bytes.len(), 32 | 64))
        .and_then(|s| <[u8; 32]>::try_from(s).ok())
        .ok_or_else(|| {
            BotError::config(format!(
                "base58 secret decoded to {} bytes; expected 32 or 64",
                bytes.len()
            ))
        })?;
    Ok(Keypair::from_bytes(seed))
}

#[cfg(test)]
mod tests {
    use bullet_rust_sdk::Keypair;

    use super::keypair_from_secret;

    /// A fixed 32-byte seed expressed as hex, base58-32, and base58-64 must all
    /// resolve to the same address.
    #[test]
    fn hex_and_base58_resolve_to_same_address() {
        let seed = [7u8; 32];
        let want = Keypair::from_bytes(seed).address();
        let pubkey = Keypair::from_bytes(seed).public_key(); // 32 bytes

        let hex = "07".repeat(32);
        assert_eq!(keypair_from_secret(&hex).expect("hex").address(), want);
        assert_eq!(keypair_from_secret(&format!("0x{hex}")).expect("0x hex").address(), want);

        let b58_32 = bs58::encode(seed).into_string();
        assert_eq!(keypair_from_secret(&b58_32).expect("b58-32").address(), want);

        let mut full = seed.to_vec();
        full.extend_from_slice(&pubkey);
        let b58_64 = bs58::encode(full).into_string();
        assert_eq!(keypair_from_secret(&b58_64).expect("b58-64").address(), want);
    }

    #[test]
    fn rejects_invalid_input() {
        // '!' and '0' are not in the base58 alphabet, and it isn't 64-char hex.
        assert!(keypair_from_secret("not-a-key!!!").is_err());
        // Valid base58 but wrong length (1 byte).
        assert!(keypair_from_secret("2").is_err());
    }
}
