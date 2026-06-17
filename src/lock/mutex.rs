//! Distributed mutual-exclusion lock: [`Mutex`] and its RAII release guard
//! [`MutexGuard`].
//!
//! Mirrors the surface of [`tokio::sync::Mutex`] over Redis. The guard guards no
//! inner data — it is a pure release token. Acquire is fallible and async over
//! the network; release is best-effort on `Drop` plus an explicit awaitable
//! [`MutexGuard::release`]. A held lock renews its lease in the background every
//! `ttl/3`; a failed renewal marks the lease [`LockGuardState::Lost`], but the task
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
use crate::lock::{LockError, LockGuardState, LockOptions, mutex_backend};

/// A distributed mutual-exclusion lock backed by Redis.
///
/// One `Mutex` stands for exactly one resource: its Redis key and owner id are
/// fixed when you build it from [`LockOptions`], much like
/// `tokio::sync::Mutex::new(x)` binds the value it protects. The guard it hands
/// back protects no data of its own — it is purely a token that says "you hold
/// this lock right now."
///
/// # How a lock is held
///
/// Acquiring writes `key -> owner` in Redis with a TTL (the lease) and only
/// succeeds when nobody else holds the key, so you never receive a [`MutexGuard`]
/// without a confirmed acquisition. While the guard is alive a background task
/// renews the lease every `ttl / 3` so it never quietly expires under you.
/// Releasing — explicitly through [`MutexGuard::release`] or implicitly on
/// `Drop` — stops that task and deletes the key, but only while we still own it.
///
/// # When a held lock can still be lost
///
/// The lease lives in Redis, not in this process, so holding a guard is a strong
/// signal that you own the lock rather than an ironclad guarantee. Each refresh
/// asks Redis to confirm ownership and extend the lease. When a refresh can't be
/// confirmed — Redis is unreachable, the round-trip errors, or the key no longer
/// points at us — the lock is marked [`Lost`](LockGuardState::Lost). The refresh
/// task keeps going after that, and the next refresh it *can* confirm clears the
/// mark and returns the lock to [`Acquired`](LockGuardState::Acquired); a short
/// network blip usually heals itself this way.
///
/// A long partition is the case to watch. If we can't reach Redis for longer than
/// the TTL, the lease expires there and another owner is free to take the key —
/// at that point the lock really is gone, a later refresh has nothing to reclaim,
/// and it stays `Lost`. So if correctness depends on the lock, don't lean on the
/// guard's existence alone: call [`MutexGuard::get_state`] to re-check what we
/// currently believe before entering the critical section.
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

    /// Tries to acquire the lock, waiting up to `timeout` and polling at the
    /// lock's configured `retry_interval` (see [`LockOptions::retry_interval`]).
    /// Returns [`LockError::Timeout`] if the deadline passes first.
    pub async fn try_lock_until(&self, timeout: Duration) -> Result<MutexGuard, DistkitError> {
        self.acquire_core(Some(timeout), self.retry_interval).await
    }

    /// Tries to acquire the lock, waiting up to `timeout` and polling every
    /// `retry_interval`. Returns [`LockError::Timeout`] if the deadline passes
    /// first. A `retry_interval` of zero is a tight spin.
    #[deprecated(
        since = "0.5.3",
        note = "use `try_lock_until`, which sources the retry interval from the lock's configuration"
    )]
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
        let mut retry_interval = if retry_interval.is_zero() {
            None
        } else {
            let mut retry_interval = interval(retry_interval);
            retry_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            Some(retry_interval)
        };

        let mut attempt = 0usize;
        loop {
            if let Some(retry_interval) = &mut retry_interval {
                // First tick is immediately, subsequent ticks are delayed.
                retry_interval.tick().await;
            }

            let acquired =
                mutex_backend::acquire(&mut connection, &self.full_key, &self.owner, self.ttl_ms)
                    .await?;

            if acquired {
                let lost = Arc::new(AtomicBool::new(false));
                let refresh_handle = self.spawn_refresh(lost.clone());

                return Ok(MutexGuard {
                    connection_manager: self.connection_manager.clone(),
                    full_key: self.full_key.clone(),
                    owner: self.owner.clone(),
                    refresh_handle: Some(refresh_handle),
                    lost,
                    on_attempt: attempt,
                });
            }

            if retry_interval.is_none() {
                return Err(LockError::AcquireFail.into());
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

            attempt += 1;
        }
    }

    /// Spawns the background lease-renewal task for a held lock.
    ///
    /// Ticks every `ttl/3`, refreshing the lease while we still own it. The first
    /// (immediate) tick is skipped since `acquire` just set the lease. On any failed
    /// renewal — lost ownership (`Ok(false)`) or a transport error (`Err`) — it sets
    /// the shared `lost` flag but keeps ticking; if a later refresh succeeds it
    /// clears the flag again. The flag is surfaced via [`MutexGuard::get_state`] /
    /// [`MutexGuard::release`] as [`LockGuardState::Lost`]. The task runs until the guard
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

                match mutex_backend::refresh(&mut connection_manager, &full_key, &owner, ttl_ms)
                    .await
                {
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

/// A token proving the current task holds a [`Mutex`].
///
/// You only ever get a `MutexGuard` from a confirmed acquisition, so its very
/// existence means the lock was yours at acquire time. It stays good for as long
/// as the background refresh keeps renewing the lease — see [`Mutex`] for the
/// cases where that can fail and the lock turns up [`Lost`](LockGuardState::Lost).
///
/// Dropping the guard releases the lock on a best-effort, fire-and-forget basis.
/// Reach for [`release`](MutexGuard::release) when you want to wait for the
/// release to land and learn the final [`LockGuardState`], and for
/// [`get_state`](MutexGuard::get_state) when you want to re-check ownership while
/// still holding it. The guard carries no inner data.
#[derive(Debug)]
pub struct MutexGuard {
    connection_manager: ConnectionManager,
    full_key: String,
    owner: String,
    refresh_handle: Option<JoinHandle<()>>,
    lost: Arc<AtomicBool>,
    /// Zero-based index of the acquire attempt that obtained this lock.
    on_attempt: usize,
}

impl MutexGuard {
    /// Re-checks what we currently believe the lock's state to be.
    ///
    /// Returns [`Acquired`](LockGuardState::Acquired) while the lease is being
    /// renewed normally, [`Lost`](LockGuardState::Lost) when a recent refresh
    /// couldn't confirm ownership (see [`Mutex`] for why that happens and when it
    /// recovers), and [`Released`](LockGuardState::Released) once the guard has
    /// been released.
    ///
    /// This reads the latest result the background refresh recorded — it does not
    /// make its own Redis round-trip. Reach for it before a critical section when
    /// you need more confidence than "I'm still holding the guard."
    pub async fn get_state(&self) -> LockGuardState {
        if self.refresh_handle.is_none() {
            return LockGuardState::Released;
        }

        if self.lost.load(Ordering::Acquire) {
            return LockGuardState::Lost;
        }

        LockGuardState::Acquired
    }

    /// The zero-based acquire attempt that obtained this lock: `0` if the very
    /// first poll succeeded, `n` after `n` retries. A one-shot `try_lock` is
    /// always `0`. Useful for contention metrics.
    pub async fn get_on_attempt(&self) -> usize {
        self.on_attempt
    }

    /// Releases the lock and reports its final [`LockGuardState`].
    ///
    /// Stops the background refresh, then deletes the key if we still own it,
    /// awaiting the round-trip so the caller can act on the outcome. Returns
    /// [`Released`](LockGuardState::Released) on a clean release, or
    /// [`Lost`](LockGuardState::Lost) if the lease had already slipped away — in
    /// that case no delete is issued, since the key may now belong to another
    /// owner. A failed Redis round-trip surfaces as `Err`.
    pub async fn release(mut self) -> Result<LockGuardState, DistkitError> {
        if let Some(handle) = self.refresh_handle.take() {
            handle.abort();
        }

        if self.lost.load(Ordering::Acquire) {
            return Ok(LockGuardState::Lost);
        }

        let mut connection = self.connection_manager.clone();
        mutex_backend::release(&mut connection, &self.full_key, &self.owner).await?;

        Ok(LockGuardState::Released)
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
            if let Err(error) = mutex_backend::release(&mut connection, &full_key, &owner).await {
                tracing::error!(?error, full_key, "Error releasing distributed lock on drop");
            }
        });
    }
}
