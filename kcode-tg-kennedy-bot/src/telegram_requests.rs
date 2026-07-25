use std::{future::Future, time::Duration};

use teloxide::{DownloadError, RequestError};

const MAX_ATTEMPTS: usize = 5;
const INITIAL_NETWORK_BACKOFF_MILLIS: u64 = 250;
const MAX_NETWORK_BACKOFF_MILLIS: u64 = 2_000;

pub(super) fn request_error_class(error: &RequestError) -> &'static str {
    match error {
        RequestError::Api(_) => "telegram_api",
        RequestError::MigrateToChatId(_) => "telegram_migrate",
        RequestError::RetryAfter(_) => "telegram_rate_limit",
        RequestError::Network(network) if network.is_timeout() => "telegram_network_timeout",
        RequestError::Network(network) if network.is_connect() => "telegram_network_connect",
        RequestError::Network(_) => "telegram_network",
        RequestError::InvalidJson { .. } => "telegram_invalid_json",
        RequestError::Io(error) => io_error_class(error.kind()),
    }
}

fn download_error_class(error: &DownloadError) -> &'static str {
    match error {
        DownloadError::Network(network) if network.is_timeout() => {
            "telegram_download_network_timeout"
        }
        DownloadError::Network(network) if network.is_connect() => {
            "telegram_download_network_connect"
        }
        DownloadError::Network(_) => "telegram_download_network",
        DownloadError::Io(error) => io_error_class(error.kind()),
    }
}

fn transient_io(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
    )
}

fn network_backoff(attempt: usize) -> Duration {
    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
    let multiplier = 2_u64.checked_pow(exponent).unwrap_or(u64::MAX);
    Duration::from_millis(
        INITIAL_NETWORK_BACKOFF_MILLIS
            .saturating_mul(multiplier)
            .min(MAX_NETWORK_BACKOFF_MILLIS),
    )
}

fn request_retry_delay(error: &RequestError, attempt: usize) -> Option<Duration> {
    match error {
        RequestError::RetryAfter(delay) => Some(delay.duration()),
        RequestError::Network(_) => Some(network_backoff(attempt)),
        RequestError::Io(error) if transient_io(error.kind()) => Some(network_backoff(attempt)),
        RequestError::Api(_)
        | RequestError::MigrateToChatId(_)
        | RequestError::InvalidJson { .. }
        | RequestError::Io(_) => None,
    }
}

fn download_retry_delay(error: &DownloadError, attempt: usize) -> Option<Duration> {
    match error {
        DownloadError::Network(_) => Some(network_backoff(attempt)),
        DownloadError::Io(error) if transient_io(error.kind()) => Some(network_backoff(attempt)),
        DownloadError::Io(_) => None,
    }
}

async fn retry_operation<T, E, Attempt, AttemptFuture, Classify, Delay, Sleep, SleepFuture>(
    operation: &'static str,
    mut attempt_operation: Attempt,
    classify: Classify,
    delay: Delay,
    sleep: Sleep,
) -> Result<T, E>
where
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = Result<T, E>>,
    Classify: Fn(&E) -> &'static str,
    Delay: Fn(&E, usize) -> Option<Duration>,
    Sleep: Fn(Duration) -> SleepFuture,
    SleepFuture: Future<Output = ()>,
{
    for attempt in 1..=MAX_ATTEMPTS {
        match attempt_operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let Some(wait) = delay(&error, attempt) else {
                    return Err(error);
                };
                if attempt == MAX_ATTEMPTS {
                    return Err(error);
                }
                tracing::debug!(
                    operation,
                    attempt,
                    error_class = classify(&error),
                    "Transient Telegram operation failed; retrying"
                );
                sleep(wait).await;
            }
        }
    }
    unreachable!("the bounded Telegram retry loop always returns")
}

pub(super) async fn retry_request<T, Attempt, AttemptFuture>(
    operation: &'static str,
    attempt: Attempt,
) -> Result<T, RequestError>
where
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = Result<T, RequestError>>,
{
    retry_operation(
        operation,
        attempt,
        request_error_class,
        request_retry_delay,
        tokio::time::sleep,
    )
    .await
}

pub(super) async fn retry_download<T, Attempt, AttemptFuture>(
    operation: &'static str,
    attempt: Attempt,
) -> Result<T, DownloadError>
where
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = Result<T, DownloadError>>,
{
    retry_operation(
        operation,
        attempt,
        download_error_class,
        download_retry_delay,
        tokio::time::sleep,
    )
    .await
}

fn io_error_class(kind: std::io::ErrorKind) -> &'static str {
    match kind {
        std::io::ErrorKind::TimedOut => "io_timeout",
        std::io::ErrorKind::ConnectionRefused => "io_connection_refused",
        std::io::ErrorKind::ConnectionReset => "io_connection_reset",
        std::io::ErrorKind::ConnectionAborted => "io_connection_aborted",
        std::io::ErrorKind::NotConnected => "io_not_connected",
        std::io::ErrorKind::BrokenPipe => "io_broken_pipe",
        std::io::ErrorKind::UnexpectedEof => "io_unexpected_eof",
        std::io::ErrorKind::PermissionDenied => "io_permission_denied",
        std::io::ErrorKind::NotFound => "io_not_found",
        _ => "io_other",
    }
}

pub(super) fn anyhow_error_class(error: &anyhow::Error) -> &'static str {
    for cause in error.chain() {
        if let Some(request_error) = cause.downcast_ref::<RequestError>() {
            return request_error_class(request_error);
        }
        if let Some(download_error) = cause.downcast_ref::<DownloadError>() {
            return download_error_class(download_error);
        }
        if let Some(sqlite_error) = cause.downcast_ref::<rusqlite::Error>() {
            return match sqlite_error {
                rusqlite::Error::SqliteFailure(_, _) => "sqlite_failure",
                rusqlite::Error::QueryReturnedNoRows => "sqlite_not_found",
                rusqlite::Error::InvalidQuery => "sqlite_invalid_query",
                _ => "sqlite_other",
            };
        }
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            return io_error_class(io_error.kind());
        }
    }
    "local_processing"
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use teloxide::types::{ChatId, Seconds};

    use super::*;

    fn transient_error() -> RequestError {
        RequestError::Io(Arc::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "test-only transient failure",
        )))
    }

    #[tokio::test]
    async fn transient_operation_succeeds_within_five_total_attempts() {
        let attempts = AtomicUsize::new(0);
        let value = retry_operation(
            "test",
            || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if attempt < 3 {
                        Err(transient_error())
                    } else {
                        Ok("ok")
                    }
                }
            },
            request_error_class,
            request_retry_delay,
            |_| async {},
        )
        .await
        .unwrap();
        assert_eq!(value, "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn transient_operation_stops_after_five_total_attempts() {
        let attempts = AtomicUsize::new(0);
        let result = retry_operation(
            "test",
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(transient_error()) }
            },
            request_error_class,
            request_retry_delay,
            |_| async {},
        )
        .await;
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn permanent_operation_is_attempted_once() {
        let attempts = AtomicUsize::new(0);
        let result = retry_operation(
            "test",
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(RequestError::MigrateToChatId(ChatId(-100))) }
            },
            request_error_class,
            request_retry_delay,
            |_| async {},
        )
        .await;
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn retry_after_is_respected_exactly() {
        let delay = request_retry_delay(&RequestError::RetryAfter(Seconds::from_seconds(17)), 1);
        assert_eq!(delay, Some(Duration::from_secs(17)));
    }
}
