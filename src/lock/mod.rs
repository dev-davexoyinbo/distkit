//! Distributed lock primitives.
//!
//! This module provides `Mutex` (mutual exclusion) and `RwLock`
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

mod mutex_backend;

mod mutex;
pub use mutex::*;

mod rwlock;
// Re-export is empty until Stage 5 adds `RwLock` + its guards.
#[allow(unused_imports)]
pub use rwlock::*;

use std::time::Duration;

use redis::aio::ConnectionManager;

use crate::DistkitRedisKey;

#[cfg(test)]
mod tests;

/// Default key prefix for locks. The full Redis key is `{namespace}:{key}`.
const DEFAULT_LOCK_NAMESPACE: &str = "distkit-locks";

/// Which side of a lock to acquire.
///
/// Backs the internal `acquire(mode, ..)` core shared by all acquire forms.
/// `Mutex` always acquires [`Exclusive`](LockMode::Exclusive);
/// `RwLock` selects per call.
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
    /// Redis key identifying the locked resource. The effective Redis key is
    /// `{namespace}:{key}`.
    pub key: DistkitRedisKey,
    /// Redis connection manager for executing lock commands.
    pub connection_manager: ConnectionManager,
    /// Key prefix applied to every lock key (default `distkit-locks`). The full
    /// Redis key is `{namespace}:{key}`.
    pub namespace: DistkitRedisKey,
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
}

impl LockOptions {
    /// Creates lock options with the documented defaults: `namespace`
    /// `distkit-locks`, `ttl` 30 s, `owner_id` a fresh UUID v4, `max_wait`
    /// `None`, `retry_interval` 50 ms.
    ///
    /// A held lock always renews its lease in the background (every `ttl/3`);
    /// this is unconditional and not configurable.
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
            namespace: DistkitRedisKey::new_or_panic(DEFAULT_LOCK_NAMESPACE.to_string()),
            ttl: Duration::from_secs(30),
            owner_id: Some(uuid::Uuid::new_v4().to_string()),
            max_wait: None,
            retry_interval: Duration::from_millis(50),
        }
    }

    /// Starts a [`LockOptionsBuilder`] seeded with the same defaults as
    /// [`LockOptions::new`]. Equivalent to [`LockOptionsBuilder::new`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use distkit::{DistkitRedisKey, lock::LockOptions};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let key = DistkitRedisKey::try_from("my_resource".to_string())?;
    /// let options = LockOptions::builder(key, conn)
    ///     .ttl(Duration::from_secs(10))
    ///     .retry_interval(Duration::from_millis(20))
    ///     .build();
    /// // options.ttl == Duration::from_secs(10)
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(
        key: DistkitRedisKey,
        connection_manager: ConnectionManager,
    ) -> LockOptionsBuilder {
        LockOptionsBuilder::new(key, connection_manager)
    }
}

/// Fluent builder for [`LockOptions`].
///
/// Construct one with [`LockOptionsBuilder::new`] or [`LockOptions::builder`],
/// override any defaults via the chainable setters, then call
/// [`build`](LockOptionsBuilder::build). Every field starts at the
/// [`LockOptions::new`] default.
#[derive(Debug, Clone)]
pub struct LockOptionsBuilder {
    options: LockOptions,
}

impl LockOptionsBuilder {
    /// Creates a builder seeded with the [`LockOptions::new`] defaults.
    pub fn new(key: DistkitRedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            options: LockOptions::new(key, connection_manager),
        }
    }

    /// Sets the key prefix (default `distkit-locks`). The full Redis key is
    /// `{namespace}:{key}`.
    pub fn namespace(mut self, namespace: DistkitRedisKey) -> Self {
        self.options.namespace = namespace;
        self
    }

    /// Sets the lease length (default 30 s).
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.options.ttl = ttl;
        self
    }

    /// Sets the owner identity recorded in Redis (default a fresh UUID v4).
    pub fn owner_id(mut self, owner_id: impl Into<String>) -> Self {
        self.options.owner_id = Some(owner_id.into());
        self
    }

    /// Sets the upper bound for the waiting acquire forms (default `None` —
    /// wait until acquired).
    pub fn max_wait(mut self, max_wait: Duration) -> Self {
        self.options.max_wait = Some(max_wait);
        self
    }

    /// Sets the poll gap between acquire attempts for the waiting forms
    /// (default 50 ms).
    pub fn retry_interval(mut self, retry_interval: Duration) -> Self {
        self.options.retry_interval = retry_interval;
        self
    }

    /// Consumes the builder and returns the assembled [`LockOptions`].
    pub fn build(self) -> LockOptions {
        self.options
    }
}
