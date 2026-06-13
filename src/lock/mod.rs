//! Distributed lock primitives.
//!
//! This module provides `DistMutex` (mutual exclusion) and `DistRwLock`
//! (reader-writer), Redis-backed locks whose surfaces mirror
//! [`tokio::sync::Mutex`] / [`tokio::sync::RwLock`] as closely as a network lock
//! allows. Both are constructed from [`LockOptions`]. Enable the `lock` feature
//! to use this module.
//!
//! The guards guard no inner data — they are pure mutual exclusion (like
//! `tokio::Mutex<()>`); a guard is a release token. Acquire is fallible and async
//! over the network; release is best-effort on `Drop` plus an explicit awaitable
//! `release()`.

mod error;
pub use error::*;

mod backend;

mod mutex;
// Re-export is empty until Stage 2 adds `DistMutex` / `DistMutexGuard`.
#[allow(unused_imports)]
pub use mutex::*;

mod rwlock;
// Re-export is empty until Stage 5 adds `DistRwLock` + its guards.
#[allow(unused_imports)]
pub use rwlock::*;

use std::time::Duration;

use redis::aio::ConnectionManager;

use crate::DistkitRedisKey;

#[cfg(test)]
mod tests;

/// Which side of a lock to acquire.
///
/// Backs the internal `acquire(mode, ..)` core shared by all acquire forms.
/// `DistMutex` always acquires [`Exclusive`](LockMode::Exclusive);
/// `DistRwLock` selects per call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Shared (read) access — multiple holders allowed concurrently.
    Shared,
    /// Exclusive (write) access — at most one holder.
    Exclusive,
}

/// Configuration for distributed-lock construction.
///
/// One [`LockOptions`] describes exactly one resource: the `key` and `owner_id`
/// are bound at construction, matching `tokio::Mutex::new(x)`.
#[derive(Debug, Clone)]
pub struct LockOptions {
    /// Redis key identifying the locked resource (namespaced under the crate's
    /// `{prefix}:` convention by the backend).
    pub key: DistkitRedisKey,
    /// Redis connection manager for executing lock commands.
    pub connection_manager: ConnectionManager,
    /// Lease length — how long an acquired lock survives without refresh
    /// (default 30 s).
    pub ttl: Duration,
    /// Owner identity recorded in Redis. Defaults to a fresh UUID v4 per
    /// [`LockOptions::new`].
    pub owner_id: Option<String>,
    /// Upper bound for the waiting acquire forms (`lock`/`read`/`write`).
    /// `None` waits until acquired (default `None`).
    pub max_wait: Option<Duration>,
    /// Poll gap between acquire attempts for the waiting forms (default 50 ms).
    pub retry_interval: Duration,
    /// When `true` (default), a background task renews the lease every `ttl/3`.
    pub auto_refresh: bool,
}

impl LockOptions {
    /// Creates lock options with the documented defaults: `ttl` 30 s,
    /// `owner_id` a fresh UUID v4, `max_wait` `None`, `retry_interval` 50 ms,
    /// `auto_refresh` `true`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{DistkitRedisKey, lock::LockOptions};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let key = DistkitRedisKey::try_from("my_resource".to_string())?;
    /// let options = LockOptions::new(key, conn);
    /// // options.ttl == std::time::Duration::from_secs(30)
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(key: DistkitRedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            key,
            connection_manager,
            ttl: Duration::from_secs(30),
            owner_id: Some(uuid::Uuid::new_v4().to_string()),
            max_wait: None,
            retry_interval: Duration::from_millis(50),
            auto_refresh: true,
        }
    }
}
