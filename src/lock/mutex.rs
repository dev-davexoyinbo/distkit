//! Distributed mutual-exclusion lock: [`Mutex`] and its RAII release guard
//! [`MutexGuard`].
//!
//! Mirrors the surface of [`tokio::sync::Mutex`] over Redis. The guard guards no
//! inner data — it is a pure release token. Acquire is fallible and async over
//! the network; release is best-effort on `Drop` plus an explicit awaitable
//! [`MutexGuard::release`]. Auto-refresh lands in Stage 3.

use std::sync::Arc;
use std::time::{Duration, Instant};

use redis::aio::ConnectionManager;
use tokio::time::{MissedTickBehavior, interval};

use crate::DistkitError;
use crate::lock::{LockError, LockOptions, backend};

/// A distributed mutual-exclusion lock backed by Redis.
///
/// One `Mutex` describes exactly one resource: its key and owner are fixed at
/// construction (via [`LockOptions`]), mirroring `tokio::sync::Mutex::new(x)`.
/// Acquiring returns a [`MutexGuard`]; dropping the guard releases the lock
/// (best-effort), or call [`MutexGuard::release`] to await and observe errors.
#[derive(Debug)]
pub struct Mutex {
    connection_manager: ConnectionManager,
    full_key: String,
    owner: String,
    ttl_ms: i64,
    max_wait: Option<Duration>,
    retry_interval: Duration,
}

impl Mutex {
    /// Creates a new distributed mutex from the given options.
    ///
    /// The effective Redis key is `{namespace}:{key}`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{DistkitRedisKey, lock::{Mutex, LockOptions}};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let key = DistkitRedisKey::try_from("my_resource".to_string())?;
    /// let mutex = Mutex::new(LockOptions::new(key, conn));
    /// let guard = mutex.try_lock().await?;
    /// guard.release().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(options: LockOptions) -> Arc<Self> {
        let LockOptions {
            key,
            connection_manager,
            namespace,
            ttl,
            owner_id,
            max_wait,
            retry_interval,
            ..
        } = options;

        let full_key = format!("{}:{}", *namespace, *key);
        let owner = owner_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let ttl_ms = ttl.as_millis() as i64;

        Arc::new(Self {
            connection_manager,
            full_key,
            owner,
            ttl_ms,
            max_wait,
            retry_interval,
        })
    }

    /// Acquires the lock, waiting up to `max_wait` (or forever if `max_wait` is
    /// `None`), polling every `retry_interval`.
    pub async fn lock(&self) -> Result<MutexGuard, DistkitError> {
        self.acquire_core(self.max_wait, self.retry_interval).await
    }

    /// Tries to acquire the lock in a single attempt without waiting. Returns
    /// [`LockError::AcquireFail`] if the lock is already held.
    pub async fn try_lock(&self) -> Result<MutexGuard, DistkitError> {
        self.acquire_core(Some(Duration::ZERO), Duration::ZERO)
            .await
    }

    /// Tries to acquire the lock, waiting up to `timeout` and polling every
    /// `retry_interval`. Returns [`LockError::Timeout`] if the deadline passes
    /// first. A `retry_interval` of zero is a tight spin.
    pub async fn try_lock_for(
        &self,
        timeout: Duration,
        retry_interval: Duration,
    ) -> Result<MutexGuard, DistkitError> {
        self.acquire_core(Some(timeout), retry_interval).await
    }

    /// Shared acquire retry-loop backing every public form.
    ///
    /// `timeout == None` waits forever; `Some(ZERO)` is a single shot;
    /// `Some(duration)` is a bounded wait.
    async fn acquire_core(
        &self,
        timeout: Option<Duration>,
        retry_interval: Duration,
    ) -> Result<MutexGuard, DistkitError> {
        let start = Instant::now();
        let mut connection = self.connection_manager.clone();
        let mut retry_interval = interval(retry_interval);
        retry_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            // First tick is immediately, subsequent ticks are delayed.
            retry_interval.tick().await;
            let acquired =
                backend::acquire(&mut connection, &self.full_key, &self.owner, self.ttl_ms).await?;

            if acquired {
                return Ok(MutexGuard {
                    connection_manager: self.connection_manager.clone(),
                    full_key: self.full_key.clone(),
                    owner: self.owner.clone(),
                    is_released: false,
                });
            }

            if let Some(ttl) = timeout {
                if ttl.is_zero() {
                    return Err(LockError::AcquireFail.into());
                }

                let waited = start.elapsed();
                if waited >= ttl {
                    return Err(LockError::Timeout { waited }.into());
                }
            }
        }
    }
}

/// RAII release token for a held [`Mutex`].
///
/// Dropping the guard releases the lock best-effort (fire-and-forget). Call
/// [`MutexGuard::release`] instead to await the release and observe errors.
/// The guard guards no inner data.
#[derive(Debug)]
pub struct MutexGuard {
    connection_manager: ConnectionManager,
    full_key: String,
    owner: String,
    is_released: bool,
}

impl MutexGuard {
    /// Releases the lock, awaiting the round-trip so callers can observe errors.
    pub async fn release(mut self) -> Result<(), DistkitError> {
        let mut connection = self.connection_manager.clone();
        backend::release(&mut connection, &self.full_key, &self.owner).await?;

        self.is_released = true;

        Ok(())
    }
}

impl Drop for MutexGuard {
    fn drop(&mut self) {
        if self.is_released {
            return;
        }

        let mut connection = self.connection_manager.clone();
        let full_key = self.full_key.clone();
        let owner = self.owner.clone();

        tokio::spawn(async move {
            if let Err(error) = backend::release(&mut connection, &full_key, &owner).await {
                tracing::error!(?error, full_key, "Error releasing distributed lock on drop");
            }
        });
    }
}
