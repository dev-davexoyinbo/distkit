use crate::counter::CounterError;

/// Top-level error type for all distkit operations.
#[derive(Debug, thiserror::Error, PartialEq, Clone)]
pub enum DistkitError {
    /// A [`RedisKey`](crate::RedisKey) failed validation (empty, too long, or
    /// contains a colon).
    #[error("Invalid Redis key: {0}")]
    InvalidRedisKey(String),
    /// A counter-specific error. See [`CounterError`].
    #[cfg(feature = "counter")]
    #[error("Counter Error: {0}")]
    CounterError(#[from] CounterError),
    /// A Redis operation failed (connection lost, script error, etc.).
    #[error("Redis Error: {0}")]
    RedisError(#[from] redis::RedisError),
    /// An internal mutex was poisoned by a prior panic.
    #[error("Mutex poisoned: {0}")]
    MutexPoisoned(&'static str),
    /// Catch-all for internal errors such as batch flush failures.
    #[error("Custom Error: {0}")]
    CustomError(String),
}
