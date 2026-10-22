//! Shared key-material resolution used by exchange adapters.
//!
//! Every adapter resolves a key the same way: a `key_file` (a file containing
//! the key *string*) takes precedence over an inline `private_key`. Only the
//! final parse into the venue's native key type differs — base58 ed25519 for
//! Bullet, hex secp256k1 for Hyperliquid — so that lives in each adapter.

use std::path::Path;

use crate::error::BotError;

/// Read a key string from a file, trimming surrounding whitespace.
pub fn read_key_file(path: &Path) -> Result<String, BotError> {
    let contents = std::fs::read_to_string(path).map_err(|e| {
        BotError::config(format!("Failed to read key file {}: {e}", path.display()))
    })?;
    Ok(contents.trim().to_string())
}

/// Resolve the key string for an adapter: `key_file` (preferred) → inline
/// `private_key`. Returns `Ok(None)` when neither yields a non-empty value, so
/// the caller can emit a venue-specific "no key material" error.
pub fn resolve_key_string(
    key_file: Option<&Path>,
    inline: &str,
) -> Result<Option<String>, BotError> {
    // An empty key_file path (e.g. `BB_BULLET_KEY_FILE=` in .env) is treated as
    // absent so it doesn't shadow the inline key.
    if let Some(path) = key_file.filter(|p| !p.as_os_str().is_empty()) {
        return read_key_file(path).map(Some);
    }
    let trimmed = inline.trim();
    Ok((!trimmed.is_empty()).then(|| trimmed.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_used_when_no_file_and_trimmed() {
        let got = resolve_key_string(None, "  abc  ").expect("resolve");
        assert_eq!(got.as_deref(), Some("abc"));
    }

    #[test]
    fn empty_inline_is_none() {
        assert_eq!(resolve_key_string(None, "   ").expect("blank"), None);
        assert_eq!(resolve_key_string(None, "").expect("empty"), None);
    }

    #[test]
    fn file_takes_precedence_over_inline_and_trims() {
        // Unique per process so parallel test runs don't collide.
        let path = std::env::temp_dir()
            .join(format!("bb_core_keys_precedence_{}.key", std::process::id()));
        std::fs::write(&path, "  filekey\n").expect("write");
        let got = resolve_key_string(Some(&path), "inlinekey").expect("resolve");
        assert_eq!(got.as_deref(), Some("filekey"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn missing_file_errors() {
        assert!(read_key_file(Path::new("/nonexistent/bb/key")).is_err());
    }

    #[test]
    fn empty_key_file_path_falls_back_to_inline() {
        let got = resolve_key_string(Some(Path::new("")), "inlinekey").expect("resolve");
        assert_eq!(got.as_deref(), Some("inlinekey"));
    }
}
