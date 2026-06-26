//! Distributed reader-writer lock: [`RwLock`] and its RAII release guards
//! [`RwLockReadGuard`] / [`RwLockWriteGuard`].
//!
//! Mirrors the surface of [`tokio::sync::RwLock`] over Redis. The guards guard no
//! inner data — they are pure access tokens. Acquire is fallible and async over
//! the network; release is best-effort on `Drop` plus an explicit awaitable
//! `release()`. A held lock renews its lease in the background every `ttl/3`; a
//! failed renewal marks the lease [`LockGuardState::Lost`], but the task keeps
//! retrying and clears the mark if ownership is later regained. The current state
//! is observable via `get_state` and is also returned by `release`.
//!
//! The lock is **writer-preferring**: once a writer is waiting, later readers may
//! not jump ahead of it (see [`rwlock_backend`](crate::lock::rwlock_backend) for
//! the Redis data model). This can starve readers under constant writers — the
//! inverse of the read-preferring trade-off.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use redis::aio::ConnectionManager;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};

use crate::DistkitError;
use crate::lock::rwlock_backend::AcquireOptions;
use crate::lock::{LockError, LockGuardState, LockMode, LockOptions, rwlock_backend};

/// A distributed reader-writer lock backed by Redis.
///
/// One `RwLock` stands for exactly one resource: its Redis keys and owner id are
/// fixed when you build it from [`LockOptions`], much like
/// `tokio::sync::RwLock::new(x)` binds the value it protects. The guards it hands
/// back protect no data of their own — they are tokens that say "you hold this
/// lock (shared or exclusive) right now."
///
/// Many readers may hold the lock at once; a writer holds it alone. The lock is
/// **writer-preferring**: while a writer is waiting, new readers are turned away
/// so the writer is not starved, and waiting writers are served in FIFO arrival
/// order. The flip side is possible reader starvation under constant writers.
///
/// # How a lock is held
///
/// Acquiring records ownership in Redis with a TTL (the lease) and only succeeds
/// when the access is compatible, so you never receive a guard without a
/// confirmed acquisition. While a guard is alive a background task renews the
/// lease every `ttl / 3` so it never quietly expires under you. Releasing —
/// explicitly through `release` or implicitly on `Drop` — stops that task and
/// drops our ownership record, but only while we still own it.
///
/// # When a held lock can still be lost
///
/// The lease lives in Redis, not in this process, so holding a guard is a strong
/// signal rather than an ironclad guarantee. Each refresh asks Redis to confirm
/// ownership and extend the lease; when a refresh can't be confirmed the lock is
/// marked [`Lost`](LockGuardState::Lost). The refresh task keeps going, and the next
/// refresh it *can* confirm clears the mark — a short network blip usually heals
/// itself. A partition longer than the TTL is the real hazard: the lease expires,
/// another owner can take the resource, and the lock stays `Lost`. If correctness
/// depends on the lock, re-check `get_state` before the critical section rather
/// than trusting the guard's existence alone.
#[derive(Debug)]
pub struct RwLock {
    connection_manager: ConnectionManager,
    writer_key: String,
    readers_key: String,
    pending_key: String,
    pending_heartbeat_key: String,
    owner: String,
    ttl_ms: i64,
    ttl_duration: Duration,
    max_wait: Option<Duration>,
    retry_interval: Duration,
}

impl RwLock {
    /// Creates a new distributed reader-writer lock from the given options.
    ///
    /// The effective Redis keys are derived from `{namespace}:{key}` with the
    /// suffixes `:w` (writer), `:r` (readers), `:pw` / `:pwh` (waiting writers).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{DistkitRedisKey, lock::{RwLock, LockOptions}};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let key = DistkitRedisKey::try_from("my_resource".to_string())?;
    /// let rw = RwLock::new(LockOptions::new(key, conn));
    /// let guard = rw.try_read().await?;
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

        let base = format!("{}:{}", *namespace, *key);
        let owner = owner_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        Arc::new(Self {
            connection_manager,
            writer_key: format!("{base}:w"),
            readers_key: format!("{base}:r"),
            pending_key: format!("{base}:pw"),
            pending_heartbeat_key: format!("{base}:pwh"),
            owner,
            ttl_ms: ttl.as_millis() as i64,
            ttl_duration: ttl,
            max_wait,
            retry_interval,
        })
    }

    /// Acquires shared (read) access, waiting up to `max_wait` (or forever if
    /// `max_wait` is `None`), polling every `retry_interval`.
    pub async fn read(&self) -> Result<RwLockReadGuard, DistkitError> {
        self.acquire_shared(self.max_wait, None, self.retry_interval)
            .await
    }

    /// Tries to acquire shared (read) access in a single attempt without waiting.
    /// Returns [`LockError::AcquireFail`] if a writer holds or is waiting.
    pub async fn try_read(&self) -> Result<RwLockReadGuard, DistkitError> {
        self.acquire_shared(Some(Duration::ZERO), None, Duration::ZERO)
            .await
    }

    /// Tries to acquire shared (read) access, waiting up to `timeout` and polling
    /// at the lock's configured `retry_interval` (see
    /// [`LockOptions::retry_interval`]). Returns [`LockError::Timeout`] if the
    /// deadline passes first.
    pub async fn try_read_with_timeout(&self, timeout: Duration) -> Result<RwLockReadGuard, DistkitError> {
        self.acquire_shared(Some(timeout), None, self.retry_interval)
            .await
    }

    /// Tries to acquire shared (read) access, making up to `max_retries` retries
    /// after the initial attempt (`max_retries + 1` attempts total), polling at
    /// the lock's configured `retry_interval` (see [`LockOptions::retry_interval`]).
    /// Returns [`LockError::RetriesExhausted`] if every attempt fails.
    ///
    /// Relies on a non-zero configured `retry_interval` (the default); a lock
    /// configured with a zero `retry_interval` collapses to a single attempt.
    pub async fn try_read_with_retries(
        &self,
        max_retries: usize,
    ) -> Result<RwLockReadGuard, DistkitError> {
        self.acquire_shared(None, Some(max_retries), self.retry_interval)
            .await
    }

    /// Tries to acquire shared (read) access, waiting up to `timeout` and polling
    /// every `retry_interval`. Returns [`LockError::Timeout`] if the deadline
    /// passes first. A `retry_interval` of zero is a tight spin.
    #[deprecated(
        since = "0.5.3",
        note = "use `try_read_with_timeout`, which sources the retry interval from the lock's configuration"
    )]
    pub async fn try_read_for(
        &self,
        timeout: Duration,
        retry_interval: Duration,
    ) -> Result<RwLockReadGuard, DistkitError> {
        self.acquire_shared(Some(timeout), None, retry_interval).await
    }

    /// Acquires exclusive (write) access, waiting up to `max_wait` (or forever if
    /// `max_wait` is `None`), polling every `retry_interval`.
    pub async fn write(&self) -> Result<RwLockWriteGuard, DistkitError> {
        self.acquire_exclusive(self.max_wait, None, self.retry_interval)
            .await
    }

    /// Tries to acquire exclusive (write) access in a single attempt without
    /// waiting. Returns [`LockError::AcquireFail`] if any reader or writer holds,
    /// or a writer is already waiting ahead. A one-shot `try_write` never enqueues
    /// itself, so a failed attempt does not block readers.
    pub async fn try_write(&self) -> Result<RwLockWriteGuard, DistkitError> {
        self.acquire_exclusive(Some(Duration::ZERO), None, Duration::ZERO)
            .await
    }

    /// Tries to acquire exclusive (write) access, waiting up to `timeout` and
    /// polling at the lock's configured `retry_interval` (see
    /// [`LockOptions::retry_interval`]). Returns [`LockError::Timeout`] if the
    /// deadline passes first.
    pub async fn try_write_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<RwLockWriteGuard, DistkitError> {
        self.acquire_exclusive(Some(timeout), None, self.retry_interval)
            .await
    }

    /// Tries to acquire exclusive (write) access, making up to `max_retries`
    /// retries after the initial attempt (`max_retries + 1` attempts total),
    /// polling at the lock's configured `retry_interval` (see
    /// [`LockOptions::retry_interval`]). Returns [`LockError::RetriesExhausted`]
    /// if every attempt fails.
    ///
    /// Relies on a non-zero configured `retry_interval` (the default); a lock
    /// configured with a zero `retry_interval` collapses to a single attempt.
    pub async fn try_write_with_retries(
        &self,
        max_retries: usize,
    ) -> Result<RwLockWriteGuard, DistkitError> {
        self.acquire_exclusive(None, Some(max_retries), self.retry_interval)
            .await
    }

    /// Tries to acquire exclusive (write) access, waiting up to `timeout` and
    /// polling every `retry_interval`. Returns [`LockError::Timeout`] if the
    /// deadline passes first. A `retry_interval` of zero is a tight spin.
    #[deprecated(
        since = "0.5.3",
        note = "use `try_write_with_timeout`, which sources the retry interval from the lock's configuration"
    )]
    pub async fn try_write_for(
        &self,
        timeout: Duration,
        retry_interval: Duration,
    ) -> Result<RwLockWriteGuard, DistkitError> {
        self.acquire_exclusive(Some(timeout), None, retry_interval)
            .await
    }

    /// Acquires shared access via the shared retry loop, then builds the guard.
    async fn acquire_shared(
        &self,
        timeout: Option<Duration>,
        max_retries: Option<usize>,
        retry_interval: Duration,
    ) -> Result<RwLockReadGuard, DistkitError> {
        let on_attempt = self
            .acquire_loop(LockMode::Shared, timeout, max_retries, retry_interval)
            .await?;

        let lost = Arc::new(AtomicBool::new(false));
        let refresh_handle = self.spawn_refresh(LockMode::Shared, lost.clone());

        Ok(RwLockReadGuard {
            connection_manager: self.connection_manager.clone(),
            readers_key: self.readers_key.clone(),
            owner: self.owner.clone(),
            refresh_handle: Some(refresh_handle),
            lost,
            on_attempt,
        })
    }

    /// Acquires exclusive access via the shared retry loop, then builds the guard.
    async fn acquire_exclusive(
        &self,
        timeout: Option<Duration>,
        max_retries: Option<usize>,
        retry_interval: Duration,
    ) -> Result<RwLockWriteGuard, DistkitError> {
        let on_attempt = self
            .acquire_loop(LockMode::Exclusive, timeout, max_retries, retry_interval)
            .await?;

        let lost = Arc::new(AtomicBool::new(false));
        let refresh_handle = self.spawn_refresh(LockMode::Exclusive, lost.clone());

        Ok(RwLockWriteGuard {
            connection_manager: self.connection_manager.clone(),
            writer_key: self.writer_key.clone(),
            pending_key: self.pending_key.clone(),
            pending_heartbeat_key: self.pending_heartbeat_key.clone(),
            owner: self.owner.clone(),
            refresh_handle: Some(refresh_handle),
            lost,
            on_attempt,
        })
    }

    /// Shared acquire retry-loop backing every public form, mirroring
    /// [`Mutex::acquire_core`](crate::lock::Mutex). `timeout == None` waits
    /// forever; `Some(ZERO)` is a single shot; `Some(duration)` is a bounded wait.
    ///
    /// For [`Exclusive`](LockMode::Exclusive), `mark_pending` is true for the
    /// waiting forms (non-zero `retry_interval`) so the writer enqueues and keeps
    /// FIFO preference, and false for a one-shot `try_write`. When a waiting
    /// writer gives up (timeout / retries exhausted / `AcquireFail`) the pending
    /// slot is cleared so it stops blocking readers. `max_retries == Some(n)`
    /// gives up with [`LockError::RetriesExhausted`] after `n` retries.
    async fn acquire_loop(
        &self,
        mode: LockMode,
        timeout: Option<Duration>,
        max_retries: Option<usize>,
        retry_interval: Duration,
    ) -> Result<usize, DistkitError> {
        let start = Instant::now();
        let mut connection = self.connection_manager.clone();
        let mark_pending = !retry_interval.is_zero();
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
                // First tick is immediate, subsequent ticks are delayed.
                retry_interval.tick().await;
            }

            let options = AcquireOptions {
                owner: &self.owner,
                ttl_ms: self.ttl_ms,
                writer_key: &self.writer_key,
                readers_key: &self.readers_key,
                pending_writers_key: &self.pending_key,
                pending_writers_heartbeat_key: &self.pending_heartbeat_key,
            };

            let acquired = match mode {
                LockMode::Shared => rwlock_backend::acquire_read(&mut connection, options).await?,
                LockMode::Exclusive => {
                    rwlock_backend::acquire_write(&mut connection, options, mark_pending).await?
                }
            };

            if acquired {
                return Ok(attempt);
            }

            if retry_interval.is_none() {
                self.clear_pending_on_giveup(mode, mark_pending, &mut connection)
                    .await;
                return Err(LockError::AcquireFail.into());
            }

            if let Some(deadline) = timeout {
                if deadline.is_zero() {
                    self.clear_pending_on_giveup(mode, mark_pending, &mut connection)
                        .await;
                    return Err(LockError::AcquireFail.into());
                }

                let waited = start.elapsed();
                if waited >= deadline {
                    self.clear_pending_on_giveup(mode, mark_pending, &mut connection)
                        .await;
                    return Err(LockError::Timeout { waited }.into());
                }
            }

            if let Some(max_retries) = max_retries
                && attempt >= max_retries
            {
                self.clear_pending_on_giveup(mode, mark_pending, &mut connection)
                    .await;
                return Err(LockError::RetriesExhausted { retries: attempt }.into());
            }

            attempt += 1;
        }
    }

    /// Clears this owner's `:pw`/`:pwh` slot when a waiting writer gives up, so it
    /// stops blocking readers. Reuses `release_write` (it `ZREM`s both pending
    /// ZSETs and no-ops the writer DEL since we don't own `:w`). No-op for readers
    /// or one-shot writes that never enqueued.
    async fn clear_pending_on_giveup(
        &self,
        mode: LockMode,
        mark_pending: bool,
        connection: &mut ConnectionManager,
    ) {
        if mode != LockMode::Exclusive || !mark_pending {
            return;
        }

        if let Err(error) = rwlock_backend::release_write(
            connection,
            &self.writer_key,
            &self.pending_key,
            &self.pending_heartbeat_key,
            &self.owner,
        )
        .await
        {
            tracing::debug!(?error, writer_key = self.writer_key, "Error clearing pending writer slot on give-up");
        }
    }

    /// Spawns the background lease-renewal task for a held lock, mirroring
    /// [`Mutex::spawn_refresh`](crate::lock::Mutex). Ticks every `ttl/3`, calling
    /// the mode-appropriate backend refresh; a failed renewal sets `lost` but the
    /// task keeps ticking and clears `lost` again on a later success.
    fn spawn_refresh(&self, mode: LockMode, lost: Arc<AtomicBool>) -> JoinHandle<()> {
        let mut connection_manager = self.connection_manager.clone();
        let writer_key = self.writer_key.clone();
        let readers_key = self.readers_key.clone();
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

                let refreshed = match mode {
                    LockMode::Shared => {
                        rwlock_backend::refresh_read(&mut connection_manager, &readers_key, &owner, ttl_ms).await
                    }
                    LockMode::Exclusive => {
                        rwlock_backend::refresh_write(&mut connection_manager, &writer_key, &owner, ttl_ms).await
                    }
                };

                match refreshed {
                    Ok(true) => {
                        // Refresh succeeded; if the lease had been marked lost we
                        // just regained ownership — clear the flag.
                        if lost.swap(false, Ordering::AcqRel) {
                            tracing::debug!(owner, "Lost distributed rwlock reacquired during refresh");
                        }
                    }
                    Ok(false) => {
                        tracing::debug!(owner, "Lost distributed rwlock lease during refresh");
                        lost.store(true, Ordering::Release);
                    }
                    Err(error) => {
                        tracing::debug!(?error, owner, "Error refreshing distributed rwlock lease");
                        lost.store(true, Ordering::Release);
                    }
                }
            }
        })
    } // end spawn_refresh
}

/// A token proving the current task holds shared (read) access on a [`RwLock`].
///
/// You only ever get a `RwLockReadGuard` from a confirmed acquisition. It stays
/// good while the background refresh keeps renewing the lease — see [`RwLock`] for
/// the cases where that can fail and the lock turns up [`Lost`](LockGuardState::Lost).
/// Dropping the guard releases the read on a best-effort, fire-and-forget basis;
/// reach for [`release`](RwLockReadGuard::release) to await it and learn the final
/// [`LockGuardState`], and [`get_state`](RwLockReadGuard::get_state) to re-check while
/// still holding it. The guard carries no inner data.
#[derive(Debug)]
pub struct RwLockReadGuard {
    connection_manager: ConnectionManager,
    readers_key: String,
    owner: String,
    refresh_handle: Option<JoinHandle<()>>,
    lost: Arc<AtomicBool>,
    /// Zero-based index of the acquire attempt that obtained this lock.
    on_attempt: usize,
}

impl RwLockReadGuard {
    /// Re-checks what we currently believe the lock's state to be, reading the
    /// latest result the background refresh recorded (no Redis round-trip). See
    /// [`RwLock`] for when a held lock turns up [`Lost`](LockGuardState::Lost).
    pub fn get_state(&self) -> LockGuardState {
        state_from(&self.refresh_handle, &self.lost)
    }

    /// The zero-based acquire attempt that obtained this lock: `0` if the very
    /// first poll succeeded, `n` after `n` retries. A one-shot `try_read` is
    /// always `0`. Useful for contention metrics.
    pub fn get_on_attempt(&self) -> usize {
        self.on_attempt
    }

    /// Releases the read and reports its final [`LockGuardState`]. Stops the refresh,
    /// then removes our reader slot if the lease is still ours; if the lease had
    /// slipped away returns [`Lost`](LockGuardState::Lost) without a round-trip. A
    /// failed Redis round-trip surfaces as `Err`.
    pub async fn release(mut self) -> Result<LockGuardState, DistkitError> {
        if let Some(handle) = self.refresh_handle.take() {
            handle.abort();
        }

        if self.lost.load(Ordering::Acquire) {
            return Ok(LockGuardState::Lost);
        }

        let mut connection = self.connection_manager.clone();
        rwlock_backend::release_read(&mut connection, &self.readers_key, &self.owner).await?;

        Ok(LockGuardState::Released)
    }
}

impl Drop for RwLockReadGuard {
    fn drop(&mut self) {
        let Some(handle) = self.refresh_handle.take() else {
            // Already released
            return;
        };

        handle.abort();

        let mut connection = self.connection_manager.clone();
        let readers_key = self.readers_key.clone();
        let owner = self.owner.clone();

        tokio::spawn(async move {
            if let Err(error) = rwlock_backend::release_read(&mut connection, &readers_key, &owner).await {
                tracing::error!(?error, readers_key, "Error releasing distributed read lock on drop");
            }
        });
    }
}

/// A token proving the current task holds exclusive (write) access on a
/// [`RwLock`].
///
/// You only ever get a `RwLockWriteGuard` from a confirmed acquisition. It stays
/// good while the background refresh keeps renewing the lease — see [`RwLock`] for
/// the cases where that can fail and the lock turns up [`Lost`](LockGuardState::Lost).
/// Dropping the guard releases the write on a best-effort, fire-and-forget basis;
/// reach for [`release`](RwLockWriteGuard::release) to await it and learn the
/// final [`LockGuardState`], and [`get_state`](RwLockWriteGuard::get_state) to
/// re-check while still holding it. The guard carries no inner data.
#[derive(Debug)]
pub struct RwLockWriteGuard {
    connection_manager: ConnectionManager,
    writer_key: String,
    pending_key: String,
    pending_heartbeat_key: String,
    owner: String,
    refresh_handle: Option<JoinHandle<()>>,
    lost: Arc<AtomicBool>,
    /// Zero-based index of the acquire attempt that obtained this lock.
    on_attempt: usize,
}

impl RwLockWriteGuard {
    /// Re-checks what we currently believe the lock's state to be, reading the
    /// latest result the background refresh recorded (no Redis round-trip). See
    /// [`RwLock`] for when a held lock turns up [`Lost`](LockGuardState::Lost).
    pub fn get_state(&self) -> LockGuardState {
        state_from(&self.refresh_handle, &self.lost)
    }

    /// The zero-based acquire attempt that obtained this lock: `0` if the very
    /// first poll succeeded, `n` after `n` retries. A one-shot `try_write` is
    /// always `0`. Useful for contention metrics.
    pub fn get_on_attempt(&self) -> usize {
        self.on_attempt
    }

    /// Releases the write and reports its final [`LockGuardState`]. Stops the
    /// refresh, then deletes the writer key if the lease is still ours (also
    /// clearing any pending slot); if the lease had slipped away returns
    /// [`Lost`](LockGuardState::Lost) without a delete. A failed Redis round-trip
    /// surfaces as `Err`.
    pub async fn release(mut self) -> Result<LockGuardState, DistkitError> {
        if let Some(handle) = self.refresh_handle.take() {
            handle.abort();
        }

        if self.lost.load(Ordering::Acquire) {
            return Ok(LockGuardState::Lost);
        }

        let mut connection = self.connection_manager.clone();
        rwlock_backend::release_write(
            &mut connection,
            &self.writer_key,
            &self.pending_key,
            &self.pending_heartbeat_key,
            &self.owner,
        )
        .await?;

        Ok(LockGuardState::Released)
    }
}

impl Drop for RwLockWriteGuard {
    fn drop(&mut self) {
        let Some(handle) = self.refresh_handle.take() else {
            // Already released
            return;
        };

        handle.abort();

        let mut connection = self.connection_manager.clone();
        let writer_key = self.writer_key.clone();
        let pending_key = self.pending_key.clone();
        let pending_heartbeat_key = self.pending_heartbeat_key.clone();
        let owner = self.owner.clone();

        tokio::spawn(async move {
            if let Err(error) = rwlock_backend::release_write(
                &mut connection,
                &writer_key,
                &pending_key,
                &pending_heartbeat_key,
                &owner,
            )
            .await
            {
                tracing::error!(?error, writer_key, "Error releasing distributed write lock on drop");
            }
        });
    }
}

/// Shared `get_state` body: `None` handle ⇒ released; `lost` set ⇒ lost; else
/// acquired.
fn state_from(refresh_handle: &Option<JoinHandle<()>>, lost: &AtomicBool) -> LockGuardState {
    if refresh_handle.is_none() {
        return LockGuardState::Released;
    }

    if lost.load(Ordering::Acquire) {
        return LockGuardState::Lost;
    }

    LockGuardState::Acquired
}
