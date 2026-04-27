use std::fmt;

/// Core error type for the bot framework.
#[derive(Debug, thiserror::Error)]
pub enum BotError {
    #[error("Exchange error: {message}")]
    Exchange { message: String, retryable: bool },

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Strategy error: {0}")]
    Strategy(String),

    #[error("Not connected to exchange: {0}")]
    NotConnected(String),

    #[error("Unknown exchange: {0}")]
    UnknownExchange(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Shutdown requested")]
    Shutdown,
}

impl BotError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, BotError::Exchange { retryable: true, .. })
    }

    /// A fatal error triggers harness shutdown. Non-retryable exchange errors,
    /// strategy errors, and configuration errors are all fatal — there is no
    /// sensible way to continue after them.
    pub fn is_fatal(&self) -> bool {
        match self {
            BotError::Config(_) | BotError::Shutdown | BotError::Strategy(_) => true,
            BotError::Exchange { retryable, .. } => !retryable,
            _ => false,
        }
    }

    pub fn exchange(e: impl fmt::Display, retryable: bool) -> Self {
        BotError::Exchange { message: e.to_string(), retryable }
    }

    pub fn strategy(e: impl fmt::Display) -> Self {
        BotError::Strategy(e.to_string())
    }

    pub fn config(e: impl fmt::Display) -> Self {
        BotError::Config(e.to_string())
    }

    pub fn not_connected(name: impl fmt::Display) -> Self {
        BotError::NotConnected(name.to_string())
    }
}
