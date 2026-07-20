use std::{error::Error as StdError, fmt};

use crate::{LimitPeriod, Money};

/// The result type returned by this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// A local validation, accounting, transport, provider, or protocol failure.
#[derive(Debug)]
pub enum Error {
    /// The API key was empty or invalid as an HTTP header.
    InvalidApiKey,
    /// The caller supplied an invalid request.
    InvalidInput(String),
    /// The in-memory session accounting state could not be used.
    Accounting(String),
    /// The Gemini service could not be reached or timed out.
    Transport(String),
    /// Gemini rejected the request.
    Provider {
        /// HTTP status returned by Gemini.
        status: u16,
        /// Sanitized provider classification, when present.
        code: Option<String>,
        /// Sanitized provider message.
        message: String,
    },
    /// A successful HTTP response did not satisfy the Gemini protocol.
    Protocol(String),
    /// A local spending window has reached its configured limit.
    SpendingLimitReached {
        /// UTC accounting period that blocked the request.
        period: LimitPeriod,
        /// Configured limit.
        limit: Money,
        /// Accounted spend in the current period.
        spent: Money,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidApiKey => f.write_str("the Gemini API key is empty or invalid"),
            Self::InvalidInput(message) => write!(f, "invalid Gemini request: {message}"),
            Self::Accounting(message) => write!(f, "Gemini accounting failed: {message}"),
            Self::Transport(message) => write!(f, "Gemini transport failed: {message}"),
            Self::Provider {
                status,
                code,
                message,
            } => match code {
                Some(code) => write!(f, "Gemini returned HTTP {status} ({code}): {message}"),
                None => write!(f, "Gemini returned HTTP {status}: {message}"),
            },
            Self::Protocol(message) => write!(f, "invalid Gemini response: {message}"),
            Self::SpendingLimitReached {
                period,
                limit,
                spent,
            } => write!(
                f,
                "Gemini {period} spending limit reached (spent {spent}, limit {limit})"
            ),
        }
    }
}

impl StdError for Error {}

pub(crate) fn transport(error: impl fmt::Display) -> Error {
    let message = error.to_string();
    Error::Transport(if message.to_ascii_lowercase().contains("timed out") {
        "the request timed out".into()
    } else {
        "the service could not be reached".into()
    })
}

pub(crate) fn clean_message(value: &str, limit: usize) -> String {
    let value = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(limit)
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if value.is_empty() {
        "unknown error".into()
    } else {
        value
    }
}
