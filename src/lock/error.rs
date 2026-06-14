use std::time::Duration;

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
}
