//! Distributed mutual-exclusion lock: [`Mutex`] and its RAII release guard
//! [`MutexGuard`].
//!
//! Mirrors the surface of [`tokio::sync::Mutex`] over Redis. The guard guards no
//! inner data — it is a pure release token. Acquire is fallible and async over
//! the network; release is best-effort on `Drop` plus an explicit awaitable
//! [`MutexGuard::release`]. A held lock renews its lease in the background every
//! `ttl/3`; a failed renewal marks the lease [`LockState::Lost`], but the task
//! keeps retrying and clears the mark if ownership is later regained. The current
//! state is observable via [`MutexGuard::get_state`] and is also returned by
//! [`MutexGuard::release`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use redis::aio::ConnectionManager;
use tokio::task::JoinHandle;
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
    ttl_duration: Duration,
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

        Arc::new(Self {
            connection_manager,
            full_key,
            owner,
            ttl_ms: ttl.as_millis() as i64,
            ttl_duration: ttl,
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
                let lost = Arc::new(AtomicBool::new(false));
                let refresh_handle = self.spawn_refresh(lost.clone());

                return Ok(MutexGuard {
                    connection_manager: self.connection_manager.clone(),
                    full_key: self.full_key.clone(),
                    owner: self.owner.clone(),
                    refresh_handle: Some(refresh_handle),
                    lost,
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

    /// Spawns the background lease-renewal task for a held lock.
    ///
    /// Ticks every `ttl/3`, refreshing the lease while we still own it. The first
    /// (immediate) tick is skipped since `acquire` just set the lease. On any failed
    /// renewal — lost ownership (`Ok(false)`) or a transport error (`Err`) — it sets
    /// the shared `lost` flag but keeps ticking; if a later refresh succeeds it
    /// clears the flag again. The flag is surfaced via [`MutexGuard::get_state`] /
    /// [`MutexGuard::release`] as [`LockState::Lost`]. The task runs until the guard
    /// aborts it on release or drop.
    fn spawn_refresh(&self, lost: Arc<AtomicBool>) -> JoinHandle<()> {
        let mut connection_manager = self.connection_manager.clone();
        let full_key = self.full_key.clone();
        let owner = self.owner.clone();
        let ttl_ms = self.ttl_ms;
        let ttl_duration = self.ttl_duration;

        tokio::spawn(async move {
            let mut ticker = interval(ttl_duration / 3);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            // Skip the first immediate tick — acquire just set the lease.
            ticker.tick().await;

            loop {
                ticker.tick().await;

                match backend::refresh(&mut connection_manager, &full_key, &owner, ttl_ms).await {
                    Ok(true) => {
                        // Refresh succeeded; if the lease had been marked lost, we
                        // just regained ownership — clear the flag.
                        if lost.swap(false, Ordering::AcqRel) {
                            tracing::debug!(
                                full_key,
                                owner,
                                "Lost distributed lock reaquired during refresh"
                            );
                        }
                    }
                    Ok(false) => {
                        tracing::debug!(
                            full_key,
                            owner,
                            "Lost distributed lock lease during refresh"
                        );
                        lost.store(true, Ordering::Release);
                    }
                    Err(error) => {
                        tracing::debug!(
                            ?error,
                            full_key,
                            owner,
                            "Error refreshing distributed lock lease"
                        );
                        lost.store(true, Ordering::Release);
                    }
                }
            }
        })
    } // end spawn_refresh
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
    refresh_handle: Option<JoinHandle<()>>,
    lost: Arc<AtomicBool>,
}

impl MutexGuard {
    /// Returns the state of the lock.
    pub async fn get_state(&self) -> MutexLockState {
        if self.refresh_handle.is_none() {
            return MutexLockState::Released;
        }

        if self.lost.load(Ordering::Acquire) {
            return MutexLockState::Lost;
        }

        MutexLockState::Acquired
    }

    /// Releases the lock, awaiting the round-trip so callers can observe errors. Returns the state
    /// of the lock.
    pub async fn release(mut self) -> Result<MutexLockState, DistkitError> {
        if let Some(handle) = self.refresh_handle.take() {
            handle.abort();
        }

        if self.lost.load(Ordering::Acquire) {
            return Ok(MutexLockState::Lost);
        }

        let mut connection = self.connection_manager.clone();
        backend::release(&mut connection, &self.full_key, &self.owner).await?;

        Ok(MutexLockState::Released)
    }
}

impl Drop for MutexGuard {
    fn drop(&mut self) {
        let Some(handle) = self.refresh_handle.take() else {
            // Already released
            return;
        };

        handle.abort();

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

/// The state of a distributed mutex lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutexLockState {
    /// The lock was successfully released.
    Released,
    /// The lock was lost and could not be released.
    Lost,
    /// The lock was acquired
    Acquired,
}
