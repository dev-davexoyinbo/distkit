//! Lax (buffered) instance-aware distributed counter.
//!
//! Buffers `inc` deltas locally and flushes them in bulk to a backing
//! [`StrictInstanceAwareCounter`] on a configurable interval. Epoch-bumping
//! operations (`set`, `del`, `clear`) flush any pending delta first, then
//! delegate immediately to the strict counter.

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use redis::aio::ConnectionManager;

use crate::{
    ActivityTracker, EPOCH_CHANGE_INTERVAL, RedisKey,
    common::mutex_lock,
    error::DistkitError,
    icounter::{
        InstanceAwareCounterTrait, StrictInstanceAwareCounter, StrictInstanceAwareCounterOptions,
    },
};

const MAX_BATCH_SIZE: usize = 100;

// ---------------------------------------------------------------------------
// Per-key in-memory state
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SingleStore {
    /// When this key was last flushed to the strict counter.
    last_flush: std::sync::Mutex<Instant>,
    /// Pending delta accumulated since the last flush.
    delta: AtomicI64,
    /// Last cumulative total received from the strict counter.
    cumulative: AtomicI64,
    /// Last instance count received from the strict counter.
    instance_count: AtomicI64,
}

impl SingleStore {
    fn new(cumulative: i64, instance_count: i64) -> Self {
        Self {
            last_flush: std::sync::Mutex::new(Instant::now()),
            delta: AtomicI64::new(0),
            cumulative: AtomicI64::new(cumulative),
            instance_count: AtomicI64::new(instance_count),
        }
    }
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// Options for constructing a [`LaxInstanceAwareCounter`].
#[derive(Debug, Clone)]
pub struct LaxInstanceAwareCounterOptions {
    /// Redis key prefix used to namespace all counter keys.
    pub prefix: RedisKey,
    /// Redis connection manager.
    pub connection_manager: ConnectionManager,
    /// Milliseconds without a heartbeat before an instance is considered dead.
    /// Default: 30 000.
    pub dead_instance_threshold_ms: u64,
    /// How often the background flush loop ticks. Default: 20 ms.
    pub flush_interval: Duration,
    /// How old a key's pending delta must be before it is included in a flush.
    /// Default: 20 ms.
    pub allowed_lag: Duration,
}

impl LaxInstanceAwareCounterOptions {
    /// Creates options with sensible defaults.
    ///
    /// Defaults: `dead_instance_threshold_ms = 30_000`, `flush_interval = 20 ms`,
    /// `allowed_lag = 20 ms`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{RedisKey, icounter::LaxInstanceAwareCounterOptions};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let prefix = RedisKey::try_from("my_app".to_string())?;
    /// let opts = LaxInstanceAwareCounterOptions::new(prefix, conn);
    /// assert_eq!(opts.dead_instance_threshold_ms, 30_000);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            prefix,
            connection_manager,
            dead_instance_threshold_ms: 30_000,
            flush_interval: Duration::from_millis(20),
            allowed_lag: Duration::from_millis(20),
        }
    }
}

// ---------------------------------------------------------------------------
// Counter struct
// ---------------------------------------------------------------------------

/// Buffered instance-aware distributed counter.
///
/// `inc` calls are accumulated locally and flushed in bulk to a backing
/// [`StrictInstanceAwareCounter`] every `flush_interval`. Global
/// epoch-bumping operations (`set`, `del`, `clear`) are forwarded
/// immediately after draining any pending delta.
#[derive(Debug)]
pub struct LaxInstanceAwareCounter {
    strict: Arc<StrictInstanceAwareCounter>,
    local_store: DashMap<RedisKey, SingleStore>,
    activity: Arc<ActivityTracker>,
    flush_interval: Duration,
    allowed_lag: Duration,
    reset_locks: DashMap<RedisKey, Arc<tokio::sync::Mutex<()>>>,
    pending_flushed: tokio::sync::Mutex<Vec<(RedisKey, i64)>>,
    instance_wide_lock: Arc<tokio::sync::Mutex<()>>,
}

impl LaxInstanceAwareCounter {
    /// Creates a new lax instance-aware counter and spawns its background flush task.
    ///
    /// `inc` deltas are buffered locally and flushed to the backing
    /// [`StrictInstanceAwareCounter`] every `flush_interval`. The background
    /// task holds a [`Weak`](std::sync::Weak) reference and stops automatically
    /// when the counter is dropped.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{RedisKey, icounter::{LaxInstanceAwareCounter, LaxInstanceAwareCounterOptions, InstanceAwareCounterTrait}};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let prefix = RedisKey::try_from("my_app".to_string())?;
    /// let counter = LaxInstanceAwareCounter::new(LaxInstanceAwareCounterOptions::new(prefix, conn));
    /// assert!(!counter.instance_id().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(options: LaxInstanceAwareCounterOptions) -> Arc<Self> {
        let LaxInstanceAwareCounterOptions {
            prefix,
            connection_manager,
            dead_instance_threshold_ms,
            flush_interval,
            allowed_lag,
        } = options;

        let strict =
            StrictInstanceAwareCounter::new_as_lax_backend(StrictInstanceAwareCounterOptions {
                prefix,
                connection_manager,
                dead_instance_threshold_ms,
            });

        let counter = Arc::new(Self {
            strict,
            local_store: DashMap::default(),
            activity: ActivityTracker::new(EPOCH_CHANGE_INTERVAL),
            flush_interval,
            allowed_lag,
            reset_locks: DashMap::default(),
            pending_flushed: tokio::sync::Mutex::new(Vec::new()),
            instance_wide_lock: Arc::new(tokio::sync::Mutex::new(())),
        });

        counter.run_flush_task();
        counter
    }

    // -----------------------------------------------------------------------
    // Background flush task
    // -----------------------------------------------------------------------

    fn run_flush_task(self: &Arc<Self>) {
        let flush_interval = self.flush_interval;
        let mut is_active_watch = self.activity.subscribe();
        let weak = Arc::downgrade(self);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            interval.tick().await; // skip first immediate tick

            loop {
                let is_active = {
                    let Some(counter) = weak.upgrade() else { break };
                    counter.activity.get_is_active()
                };

                if !is_active && is_active_watch.changed().await.is_err() {
                    break;
                }

                interval.tick().await;

                let Some(counter) = weak.upgrade() else { break };
                if let Err(err) = counter.flush().await {
                    tracing::error!("lax_icounter:flush_task: flush failed: {err}");
                }
            }
        });
    }

    async fn flush(&self) -> Result<(), DistkitError> {
        let mut pending = self.pending_flushed.lock().await;

        // Collect newly stale deltas (delta already swapped to 0 in local_store).
        pending.extend(self.collect_stale_mark_flushed());

        if pending.is_empty() {
            return Ok(());
        }

        let results = self.strict.inc_batch(&mut pending, MAX_BATCH_SIZE).await?;

        for (key_str, cumulative, instance_count) in results {
            if let Ok(key) = RedisKey::try_from(key_str) {
                self.update_local(&key, cumulative, instance_count);
            }
        }

        Ok(())
    }

    /// Acquire (or create) the per-key reset lock and return an `Arc` to it.
    fn get_or_create_reset_lock(&self, key: &RedisKey) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(lock) = self.reset_locks.get(key) {
            return Arc::clone(&lock);
        }

        self.reset_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    } // end function get_or_create_reset_lock

    fn collect_stale_mark_flushed(&self) -> Vec<(RedisKey, i64)> {
        let now = Instant::now();
        self.local_store
            .iter()
            .filter_map(|store| {
                // Deref copies the Instant; the MutexGuard is dropped immediately.
                let last = *mutex_lock(&store.last_flush, "lax_icounter:last_flush").ok()?;

                if now.duration_since(last) < self.allowed_lag {
                    return None;
                }

                let delta = store.delta.swap(0, Ordering::AcqRel);

                if delta != 0 {
                    *mutex_lock(&store.last_flush, "lax_icounter:last_flush").ok()? =
                        Instant::now();
                    Some((store.key().clone(), delta))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Drains the pending delta for a single key by calling `strict.inc`
    /// directly. Used before epoch-bumping operations (`set`, `del`, etc.).
    async fn flush_key(&self, key: &RedisKey) -> Result<(), DistkitError> {
        let Some(store) = self.local_store.get(key) else {
            return Ok(());
        };

        let delta = store.delta.swap(0, Ordering::AcqRel);
        *mutex_lock(&store.last_flush, "lax_icounter:last_flush")? = Instant::now();

        if delta == 0 {
            return Ok(());
        }

        let (cumulative, instance_count) = self.strict.inc(key, delta).await?;
        self.update_local(key, cumulative, instance_count);

        Ok(())
    } // end function flush_key

    /// Drains pending deltas for all keys regardless of staleness.
    /// Used by `clear` / `clear_on_instance` before delegating to strict.
    async fn flush_all_keys(&self) -> Result<(), DistkitError> {
        let mut all: Vec<(RedisKey, i64)> = self
            .local_store
            .iter()
            .filter_map(|store| {
                let delta = store.delta.swap(0, Ordering::AcqRel);
                *mutex_lock(&store.last_flush, "lax_icounter:last_flush").ok()? = Instant::now();

                if delta != 0 {
                    Some((store.key().clone(), delta))
                } else {
                    None
                }
            })
            .collect();

        if all.is_empty() {
            return Ok(());
        }

        let results = self.strict.inc_batch(&mut all, MAX_BATCH_SIZE).await?;

        for (key_str, cumulative, instance_count) in results {
            if let Ok(key) = RedisKey::try_from(key_str) {
                self.update_local(&key, cumulative, instance_count);
            }
        }

        Ok(())
    }

    /// Updates `local_store` with fresh values from the strict counter.
    fn update_local(&self, key: &RedisKey, cumulative: i64, instance_count: i64) {
        match self.local_store.get(key) {
            Some(store) => {
                store.cumulative.store(cumulative, Ordering::Release);
                store
                    .instance_count
                    .store(instance_count, Ordering::Release);
            }
            None => {
                self.local_store
                    .entry(key.clone())
                    .or_insert_with(|| SingleStore::new(cumulative, instance_count));
            }
        }
    } // end function update_local

    async fn refresh_local_if_needed(&self, key: &RedisKey) -> Result<(), DistkitError> {
        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        if let Some(store) = self.local_store.get(key)
            && mutex_lock(&store.last_flush, "lax_icounter:last_flush")?.elapsed()
                < self.allowed_lag
        {
            return Ok(());
        }

        let (cumulative, instance_count) = self.strict.get(key).await?;

        self.update_local(key, cumulative, instance_count);

        Ok(())
    }

    /// Fetches stale/missing keys from the strict counter in a single batched
    /// round-trip, then updates `local_store` for each key.
    async fn batch_refresh_stale(&self, keys: &[&RedisKey]) -> Result<(), DistkitError> {
        if keys.is_empty() {
            return Ok(());
        }

        let keys: Vec<&RedisKey> = keys
            .iter()
            .filter(|key| {
                self.local_store
                    .get(*key)
                    .and_then(|s| {
                        mutex_lock(&s.last_flush, "lax_icounter:last_flush")
                            .ok()
                            .map(|g| g.elapsed() >= self.allowed_lag)
                    })
                    .unwrap_or(true)
            })
            .copied()
            .collect();

        if keys.is_empty() {
            return Ok(());
        }

        let _guard = self.instance_wide_lock.lock().await;

        // recheck if we have stale keys
        let keys: Vec<&RedisKey> = keys
            .iter()
            .filter(|key| {
                self.local_store
                    .get(*key)
                    .and_then(|s| {
                        mutex_lock(&s.last_flush, "lax_icounter:last_flush")
                            .ok()
                            .map(|g| g.elapsed() >= self.allowed_lag)
                    })
                    .unwrap_or(true)
            })
            .copied()
            .collect();

        if keys.is_empty() {
            return Ok(());
        }

        let results = self.strict.get_batch(&keys).await?;

        for (key, cumulative, instance_count) in results {
            self.update_local(key, cumulative, instance_count);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// InstanceAwareCounterTrait
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl InstanceAwareCounterTrait for LaxInstanceAwareCounter {
    /// Returns this instance's unique identifier.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// assert!(!counter.instance_id().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    fn instance_id(&self) -> &str {
        self.strict.instance_id()
    }

    /// Buffers `count` locally and returns the updated local estimate without
    /// a Redis round-trip.
    ///
    /// On the first call for a key, the current value is fetched from the
    /// backing [`StrictInstanceAwareCounter`] to seed the local cache. Subsequent
    /// calls accumulate into a single `inc_batch` that is flushed after
    /// `flush_interval` (default 20 ms). Returns `(cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// // All three calls are sub-microsecond; no Redis round-trip until flush.
    /// let (c1, s1) = counter.inc(&key, 1).await?;
    /// let (c2, s2) = counter.inc(&key, 1).await?;
    /// let (c3, s3) = counter.inc(&key, 1).await?;
    /// assert_eq!(c3, 3);
    /// assert_eq!(s3, 3);
    /// # Ok(())
    /// # }
    /// ```
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let store = match self.local_store.get(key) {
            Some(store)
                if mutex_lock(&store.last_flush, "lax_icounter:last_flush")?.elapsed()
                    < self.allowed_lag =>
            {
                store
            }
            Some(store) => {
                drop(store);

                self.refresh_local_if_needed(key).await?;

                self.local_store
                    .get(key)
                    .expect("key should be in local_store")
            }
            None => {
                self.refresh_local_if_needed(key).await?;

                self.local_store
                    .get(key)
                    .expect("key should be in local_store")
            }
        };

        let delta = store.delta.fetch_add(count, Ordering::AcqRel) + count;
        let cumulative = store.cumulative.load(Ordering::Acquire);
        let instance_count = store.instance_count.load(Ordering::Acquire);

        Ok((cumulative + delta, instance_count + delta))
    }

    /// Decrements the counter locally. Equivalent to `inc(key, -count)`.
    ///
    /// Returns `(cumulative, instance_count)` as a local estimate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// counter.inc(&key, 10).await?;
    /// let (cumulative, slice) = counter.dec(&key, 4).await?;
    /// assert_eq!(cumulative, 6);
    /// assert_eq!(slice, 6);
    /// # Ok(())
    /// # }
    /// ```
    async fn dec(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        self.inc(key, -count).await
    }

    /// Flushes any pending delta for `key` to the backing strict counter,
    /// then calls `strict.set`, which bumps the epoch and makes the change
    /// immediately visible to all instances.
    ///
    /// Returns `(cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// counter.inc(&key, 5).await?; // buffered locally
    /// // Pending delta flushed first; then strict.set takes over.
    /// let (cumulative, slice) = counter.set(&key, 100).await?;
    /// assert_eq!(cumulative, 100);
    /// assert_eq!(slice, 100);
    /// # Ok(())
    /// # }
    /// ```
    async fn set(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        self.flush_key(key).await?;

        let (cumulative, instance_count) = self.strict.set(key, count).await?;

        self.update_local(key, cumulative, instance_count);

        Ok((cumulative, instance_count))
    }

    /// Adjusts the local delta so this instance's contribution reaches `count`,
    /// without a Redis round-trip and without bumping the epoch.
    ///
    /// On the first call for a key the current value is fetched from Redis to
    /// seed the cache. Returns `(cumulative, instance_count)` as a local estimate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_lax_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 10).await?;
    /// server_b.inc(&key, 5).await?;
    /// // Adjusts server_a's local delta to reach 7; no epoch bump.
    /// let (cumulative, slice) = server_a.set_on_instance(&key, 7).await?;
    /// assert_eq!(slice, 7);
    /// // Local estimate only includes server_a's contribution;
    /// // server_b's buffered delta has not been flushed to Redis yet.
    /// assert_eq!(cumulative, 7);
    /// # Ok(())
    /// # }
    /// ```
    async fn set_on_instance(
        &self,
        key: &RedisKey,
        count: i64,
    ) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let store = match self.local_store.get(key) {
            Some(store)
                if mutex_lock(&store.last_flush, "lax_icounter:last_flush")?.elapsed()
                    < self.allowed_lag =>
            {
                store
            }
            Some(store) => {
                drop(store);

                self.refresh_local_if_needed(key).await?;

                self.local_store
                    .get(key)
                    .expect("key should be in local_store")
            }
            None => {
                self.refresh_local_if_needed(key).await?;

                self.local_store
                    .get(key)
                    .expect("key should be in local_store")
            }
        };

        let instance_count = store.instance_count.load(Ordering::Acquire);
        store.delta.store(count - instance_count, Ordering::Release);
        let cumulative = store.cumulative.load(Ordering::Acquire);

        Ok((cumulative - instance_count + count, count))
    }

    /// Returns `(cumulative + pending_delta, instance_count + pending_delta)`.
    ///
    /// On the first call for a key the current value is fetched from Redis to
    /// seed the local cache. Subsequent reads return the local estimate without
    /// a Redis round-trip.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// assert_eq!(counter.get(&key).await?, (0, 0));
    /// counter.inc(&key, 5).await?;
    /// // Returns local estimate (buffered delta included).
    /// assert_eq!(counter.get(&key).await?, (5, 5));
    /// # Ok(())
    /// # }
    /// ```
    async fn get(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let store = match self.local_store.get(key) {
            Some(store)
                if mutex_lock(&store.last_flush, "lax_icounter:last_flush")?.elapsed()
                    < self.allowed_lag =>
            {
                store
            }
            Some(store) => {
                drop(store);

                self.refresh_local_if_needed(key).await?;

                self.local_store
                    .get(key)
                    .expect("key should be in local_store")
            }
            None => {
                self.refresh_local_if_needed(key).await?;

                self.local_store
                    .get(key)
                    .expect("key should be in local_store")
            }
        };

        let delta = store.delta.load(Ordering::Acquire);
        let cumulative = store.cumulative.load(Ordering::Acquire);
        let instance_count = store.instance_count.load(Ordering::Acquire);

        Ok((cumulative + delta, instance_count + delta))
    }

    /// Flushes the pending delta for `key`, then deletes the key globally and
    /// bumps the epoch. Returns `(old_cumulative, old_instance_count)`.
    ///
    /// After deletion, a subsequent `inc` or `get` starts fresh from `0`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// counter.inc(&key, 5).await?; // buffered
    /// let (old_cumulative, _) = counter.del(&key).await?;
    /// assert_eq!(old_cumulative, 5);
    /// assert_eq!(counter.get(&key).await?, (0, 0));
    /// # Ok(())
    /// # }
    /// ```
    async fn del(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();
        self.flush_key(key).await?;
        let result = self.strict.del(key).await?;
        self.local_store.remove(key);
        Ok(result)
    }

    /// Flushes the pending delta for `key`, then removes only this instance's
    /// contribution without bumping the epoch.
    ///
    /// Returns `(new_cumulative, removed_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_lax_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 3).await?;
    /// server_b.inc(&key, 7).await?;
    /// // Flush server_a's pending delta, then remove only its slice.
    /// let (new_cumulative, removed) = server_a.del_on_instance(&key).await?;
    /// assert_eq!(removed, 3);
    /// // Redis cumulative is 0 — server_b's delta is still buffered locally.
    /// assert_eq!(new_cumulative, 0);
    /// // Server_b's local view still shows its own buffered total.
    /// assert_eq!(server_b.get(&key).await?, (7, 7));
    /// # Ok(())
    /// # }
    /// ```
    async fn del_on_instance(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();
        self.flush_key(key).await?;
        let result = self.strict.del_on_instance(key).await?;
        self.update_local(key, result.0, 0);
        Ok(result)
    }

    /// Flushes all pending deltas, then removes all keys and instance state from
    /// Redis. Clears the local store.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let k1 = RedisKey::try_from("a".to_string())?;
    /// let k2 = RedisKey::try_from("b".to_string())?;
    /// counter.inc(&k1, 10).await?;
    /// counter.inc(&k2, 20).await?;
    /// counter.clear().await?;
    /// assert_eq!(counter.get(&k1).await?, (0, 0));
    /// assert_eq!(counter.get(&k2).await?, (0, 0));
    /// # Ok(())
    /// # }
    /// ```
    async fn clear(&self) -> Result<(), DistkitError> {
        self.activity.signal();
        self.flush_all_keys().await?;
        self.strict.clear().await?;
        self.local_store.clear();
        Ok(())
    }

    /// Flushes all pending deltas, then removes only this instance's contributions
    /// from Redis. Clears the local store for this instance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_lax_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 3).await?;
    /// server_b.inc(&key, 7).await?;
    /// // Flush + remove only server_a's contributions; server_b's slice survives.
    /// server_a.clear_on_instance().await?;
    /// assert_eq!(server_b.get(&key).await?, (7, 7));
    /// # Ok(())
    /// # }
    /// ```
    async fn clear_on_instance(&self) -> Result<(), DistkitError> {
        self.activity.signal();
        self.flush_all_keys().await?;
        self.strict.clear_on_instance().await?;
        self.local_store.clear();
        Ok(())
    }

    async fn get_all<'k>(
        &self,
        keys: &[&'k RedisKey],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        self.batch_refresh_stale(keys).await?;

        keys.iter()
            .map(|key| {
                let store = self
                    .local_store
                    .get(*key)
                    .expect("store populated after refresh");
                let delta = store.delta.load(Ordering::Acquire);
                Ok((
                    *key,
                    store.cumulative.load(Ordering::Acquire) + delta,
                    store.instance_count.load(Ordering::Acquire) + delta,
                ))
            })
            .collect()
    } // end function get_all

    async fn get_all_on_instance<'k>(
        &self,
        keys: &[&'k RedisKey],
    ) -> Result<Vec<(&'k RedisKey, i64)>, DistkitError> {
        self.batch_refresh_stale(keys).await?;

        Ok(keys
            .iter()
            .map(|key| {
                let val = self
                    .local_store
                    .get(*key)
                    .map(|s| {
                        s.instance_count.load(Ordering::Acquire) + s.delta.load(Ordering::Acquire)
                    })
                    .unwrap_or(0);
                (*key, val)
            })
            .collect())
    } // end function get_all_on_instance

    async fn set_all<'k>(
        &self,
        updates: &[(&'k RedisKey, i64)],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let _guard = self.instance_wide_lock.lock().await;

        self.flush().await?;

        let batch = self.strict.set_batch(updates).await?;
        for (key, cumulative, instance_count) in &batch {
            self.update_local(key, *cumulative, *instance_count);
        }

        Ok(batch)
    } // end function set_all

    async fn set_all_on_instance<'k>(
        &self,
        updates: &[(&'k RedisKey, i64)],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let keys: Vec<&RedisKey> = updates.iter().map(|(key, _)| *key).collect();

        self.batch_refresh_stale(&keys).await?;

        updates
            .iter()
            .map(|(key, count)| {
                let store = self
                    .local_store
                    .get(*key)
                    .expect("store populated after refresh");
                let instance_count = store.instance_count.load(Ordering::Acquire);
                store.delta.store(count - instance_count, Ordering::Release);
                let cumulative = store.cumulative.load(Ordering::Acquire);
                Ok((*key, cumulative - instance_count + count, *count))
            })
            .collect()
    } // end function set_all_on_instance
}
