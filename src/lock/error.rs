use std::time::Duration;

/// Lock-specific error type.
///
/// Surfaced through [`DistkitError::LockError`](crate::DistkitError::LockError).
/// Redis transport failures continue to surface as
/// [`DistkitError::RedisError`](crate::DistkitError::RedisError).
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum LockError {
    /// The lock is currently held and a non-blocking attempt was made.
    #[error("lock is held (would block)")]
    WouldBlock,
    /// A bounded acquire exceeded its deadline without acquiring the lock.
    #[error("timed out after {waited:?}")]
    Timeout {
        /// How long the acquire waited before giving up.
        waited: Duration,
    },
    /// The lease was lost (a background refresh failed before release).
    #[error("lock lease lost (refresh failed)")]
    LockLost,
    /// The caller is not the recorded owner of the lock.
    #[error("not the lock owner")]
    NotOwner,
}
