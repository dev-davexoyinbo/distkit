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
}

impl LaxInstanceAwareCounter {
    /// Creates a new lax instance-aware counter and spawns its background flush task.
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

            let mut pending: Vec<(RedisKey, i64)> = Vec::new();

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

                // Collect newly stale deltas (delta already swapped to 0 in local_store).
                pending.extend(counter.collect_stale_mark_flushed());

                if pending.is_empty() {
                    continue;
                }

                let results = match counter.strict.inc_batch(&mut pending, MAX_BATCH_SIZE).await {
                    Ok(results) => results,
                    Err(err) => {
                        tracing::error!("lax_icounter:flush_task: inc_batch failed: {err}");
                        continue;
                    }
                };

                for (key_str, cumulative, instance_count) in results {
                    if let Ok(key) = RedisKey::try_from(key_str) {
                        counter.update_local(&key, cumulative, instance_count);
                    }
                }
            }
        });
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
}

// ---------------------------------------------------------------------------
// InstanceAwareCounterTrait
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl InstanceAwareCounterTrait for LaxInstanceAwareCounter {
    fn instance_id(&self) -> &str {
        self.strict.instance_id()
    }

    async fn inc(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let store = match self.local_store.get(key) {
            Some(store) => store,
            None => {
                let lock = self.get_or_create_reset_lock(key);
                let _guard = lock.lock().await;

                if !self.local_store.contains_key(key) {
                    let (cumulative, instance_count) = self.strict.get(key).await?;

                    self.local_store
                        .entry(key.clone())
                        .or_insert_with(|| SingleStore::new(cumulative, instance_count));
                }

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

    async fn set(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        self.flush_key(key).await?;

        let (cumulative, instance_count) = self.strict.set(key, count).await?;

        self.update_local(key, cumulative, instance_count);

        Ok((cumulative, instance_count))
    }

    async fn set_on_instance(
        &self,
        key: &RedisKey,
        count: i64,
    ) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let store = match self.local_store.get(key) {
            Some(store) => store,
            None => {
                let lock = self.get_or_create_reset_lock(key);
                let _guard = lock.lock().await;

                if !self.local_store.contains_key(key) {
                    let (cumulative, instance_count) = self.strict.get(key).await?;

                    self.local_store
                        .entry(key.clone())
                        .or_insert_with(|| SingleStore::new(cumulative, instance_count));
                }

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

    async fn get(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let store = match self.local_store.get(key) {
            Some(store) => store,
            None => {
                let lock = self.get_or_create_reset_lock(key);
                let _guard = lock.lock().await;

                if !self.local_store.contains_key(key) {
                    let (cumulative, instance_count) = self.strict.get(key).await?;

                    self.local_store
                        .entry(key.clone())
                        .or_insert_with(|| SingleStore::new(cumulative, instance_count));
                }

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

    async fn del(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();
        self.flush_key(key).await?;
        let result = self.strict.del(key).await?;
        self.local_store.remove(key);
        Ok(result)
    }

    async fn del_on_instance(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();
        self.flush_key(key).await?;
        let result = self.strict.del_on_instance(key).await?;
        self.update_local(key, result.0, 0);
        Ok(result)
    }

    async fn clear(&self) -> Result<(), DistkitError> {
        self.activity.signal();
        self.flush_all_keys().await?;
        self.strict.clear().await?;
        self.local_store.clear();
        Ok(())
    }

    async fn clear_on_instance(&self) -> Result<(), DistkitError> {
        self.activity.signal();
        self.flush_all_keys().await?;
        self.strict.clear_on_instance().await?;
        self.local_store.clear();
        Ok(())
    }
}
