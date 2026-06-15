use std::time::Duration;

use crate::DistkitError;

/// Lock-specific error type.
///
/// Surfaced through [`DistkitError::LockError`](crate::DistkitError::LockError).
/// Redis transport failures continue to surface as
/// [`DistkitError::RedisError`](crate::DistkitError::RedisError).
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum LockError {
    /// The lock was held by another process.
    #[error("failed to acquire lock (would block)")]
    AcquireFail,
    /// A bounded acquire exceeded its deadline without acquiring the lock.
    #[error("timed out after {waited:?}")]
    Timeout {
        /// How long the acquire waited before giving up.
        waited: Duration,
    },
    /// The caller is not the recorded owner of the lock.
    #[error("not the lock owner")]
    NotOwner,
    /// The provided `ttl_ms` was not positive.
    #[error("ttl_ms must be positive, got {0}")]
    InvalidTtl(i64),
    /// The provided owner id was empty.
    #[error("owner id must not be empty")]
    InvalidOwner,
}

/// Validates a `ttl_ms` argument shared by the mutex and rwlock backends.
pub(crate) fn validate_ttl(ttl_ms: i64) -> Result<(), DistkitError> {
    if ttl_ms <= 0 {
        return Err(LockError::InvalidTtl(ttl_ms).into());
    }
    Ok(())
}

/// Validates an `owner` argument shared by the mutex and rwlock backends.
pub(crate) fn validate_owner(owner: &str) -> Result<(), DistkitError> {
    if owner.is_empty() {
        return Err(LockError::InvalidOwner.into());
    }
    Ok(())
}
