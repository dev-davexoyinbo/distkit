use std::{
    collections::HashMap,
    ops::Deref,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use dashmap::DashMap;
use redis::{Script, aio::ConnectionManager};
use tokio::time::Instant;

use crate::{
    ActivityTracker, DistkitError, EPOCH_CHANGE_INTERVAL, RedisKey, RedisKeyGenerator,
    RedisKeyGeneratorTypeKey,
    counter::{CounterError, CounterOptions, CounterTrait},
    execute_pipeline_with_script_retry, mutex_lock,
};

const MAX_BATCH_SIZE: usize = 100;

const GET_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]

    return {key, tonumber(redis.call('HGET', container_key, key)) or 0}
"#;

const COMMIT_STATE_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]
    local count = tonumber(ARGV[1]) or 0

    redis.call('HINCRBY', container_key, key, count)
"#;

const DEL_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]

    local total = redis.call('HGET', container_key, key) or 0

    redis.call('HDEL', container_key, key)

    return total
"#;

const CLEAR_LUA: &str = r#"
    local container_key = KEYS[1]
    redis.call('DEL', container_key)
"#;

#[derive(Debug)]
struct Commit {
    key: RedisKey,
    delta: i64,
}

#[derive(Debug)]
struct SingleStore {
    remote_total: AtomicI64,
    delta: AtomicI64,
    last_updated: Mutex<Instant>,
    last_flushed: Mutex<Option<Instant>>,
}

/// Eventually consistent counter with in-memory buffering.
///
/// Writes are buffered in a local [`DashMap`] and flushed to Redis in
/// batched pipelines every `allowed_lag` (default 20 ms). Reads return the
/// local view (`remote_total + pending_delta`), which is always up-to-date
/// within the same process.
///
/// A background Tokio task handles flushing. It holds a [`Weak`](std::sync::Weak)
/// reference to the counter, so it stops automatically when the counter is
/// dropped.
///
/// Construct via [`LaxCounter::new`], which returns an `Arc<LaxCounter>`.
#[derive(Debug)]
pub struct LaxCounter {
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
    store: DashMap<RedisKey, SingleStore>,
    locks: DashMap<RedisKey, Arc<tokio::sync::Mutex<()>>>,
    get_script: Script,
    allowed_lag: Duration,
    commit_state_script: Script,
    del_script: Script,
    clear_script: Script,

    // Flush states
    batch: tokio::sync::Mutex<Vec<Commit>>,

    activity: Arc<ActivityTracker>,
}

impl LaxCounter {
    /// Creates a new lax counter and spawns its background flush task.
    ///
    /// The background task holds a [`Weak`](std::sync::Weak) reference and
    /// stops automatically when the counter is dropped.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{RedisKey, counter::{LaxCounter, CounterOptions}};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let prefix = RedisKey::try_from("my_app".to_string())?;
    /// let counter = LaxCounter::new(CounterOptions::new(prefix, conn));
    /// // The background flush task is now running.
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(options: CounterOptions) -> Arc<Self> {
        let CounterOptions {
            prefix,
            connection_manager,
            allowed_lag,
        } = options;
        let key_generator = RedisKeyGenerator::new(prefix, RedisKeyGeneratorTypeKey::Lax);

        let get_script = Script::new(GET_LUA);
        let del_script = Script::new(DEL_LUA);
        let clear_script = Script::new(CLEAR_LUA);

        let commit_state_script = Script::new(COMMIT_STATE_LUA);

        let counter = Self {
            connection_manager,
            key_generator,
            store: DashMap::default(),
            get_script,
            del_script,
            clear_script,
            allowed_lag,
            locks: DashMap::default(),
            commit_state_script,
            batch: tokio::sync::Mutex::new(Vec::new()),
            activity: ActivityTracker::new(EPOCH_CHANGE_INTERVAL),
        };

        let counter = Arc::new(counter);

        counter.run_flush_task();

        counter
    }

    fn run_flush_task(self: &Arc<Self>) {
        tokio::spawn({
            let allowed_lag = self.allowed_lag;
            let counter = Arc::downgrade(self);
            let mut is_active_watch = self.activity.subscribe();

            async move {
                // let mut batch = Vec::new();
                let mut interval = tokio::time::interval(allowed_lag);
                interval.tick().await;

                loop {
                    let is_active = {
                        let Some(counter) = counter.upgrade() else {
                            break;
                        };

                        counter.activity.get_is_active()
                    };

                    // if not active, wait for the watcher to change
                    if !is_active && is_active_watch.changed().await.is_err() {
                        break;
                    }

                    interval.tick().await;

                    let counter = match counter.upgrade() {
                        Some(counter) => counter,
                        None => break,
                    };

                    let mut batch = counter.batch.lock().await;

                    for entry in counter.store.iter() {
                        let key = entry.key();
                        let store = entry.value();

                        if store.delta.load(Ordering::Acquire) == 0 {
                            continue;
                        }

                        let last_flushed = mutex_lock(&store.last_flushed, "last_flushed")
                            .map(|el| *el)
                            .unwrap_or(None);

                        if let Some(last_flushed) = last_flushed
                            && last_flushed.elapsed() < allowed_lag
                        {
                            continue;
                        }

                        let delta = store.delta.swap(0, Ordering::AcqRel);
                        store.remote_total.fetch_add(delta, Ordering::AcqRel);
                        let Ok(mut last_flushed) = mutex_lock(&store.last_flushed, "last_flushed")
                        else {
                            continue;
                        };

                        *last_flushed = Some(Instant::now());

                        batch.push(Commit {
                            key: key.clone(),
                            delta,
                        });
                    }

                    if let Err(err) = counter.flush_to_redis(&mut batch, MAX_BATCH_SIZE).await {
                        tracing::error!("Failed to flush to redis: {err:?}");
                        continue;
                    }
                }
            }
        });
    }

    async fn flush_to_redis(
        &self,
        batch: &mut Vec<Commit>,
        max_batch_size: usize,
    ) -> Result<(), DistkitError> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut processed = 0;

        while processed < batch.len() {
            let end = (processed + max_batch_size).min(batch.len());
            let chunk = &batch[processed..end];

            self.batch_commit_state(chunk)
                .await
                .map_err(|err| CounterError::CommitToRedisFailed(format!("{err:?}")))?;

            processed = end;
        }

        batch.drain(..processed);

        Ok(())
    } // end method flush_to_redis

    async fn batch_commit_state(&self, commits: &[Commit]) -> Result<(), DistkitError> {
        let mut conn = self.connection_manager.clone();
        let script = &self.commit_state_script;
        execute_pipeline_with_script_retry::<(), _, _>(&mut conn, script, commits, |commit| {
            let mut inv = script.key(self.key_generator.container_key());
            inv.key(commit.key.as_str());
            inv.arg(commit.delta);
            inv
        })
        .await
    } // end method batch_commit_state

    async fn ensure_valid_state(&self, key: &RedisKey) -> Result<(), DistkitError> {
        let lock = self.get_or_create_lock(key).await;
        let _guard = lock.lock().await;

        {
            let store = self.store.get(key);

            if let Some(ref store) = store
                && let SingleStore { last_updated, .. } = store.deref()
                && mutex_lock(last_updated, "last_updated")?.elapsed() < self.allowed_lag
            {
                return Ok(());
            }
        }

        let mut conn = self.connection_manager.clone();

        let (_, remote_total): (String, i64) = self
            .get_script
            .key(self.key_generator.container_key())
            .key(key.as_str())
            .invoke_async(&mut conn)
            .await?;

        let store = match self.store.get(key) {
            Some(store) => store,

            None => {
                self.store
                    .entry(key.clone())
                    .or_insert_with(|| SingleStore {
                        remote_total: AtomicI64::new(remote_total),
                        delta: AtomicI64::new(0),
                        last_updated: Mutex::new(Instant::now()),
                        last_flushed: Mutex::new(None),
                    });

                self.store.get(key).expect("store should be present here")
            }
        };

        store.remote_total.store(remote_total, Ordering::Release);
        *mutex_lock(&store.last_updated, "last_updated")? = Instant::now();

        Ok(())
    } // end function get_remote_total

    async fn get_or_create_lock(&self, key: &RedisKey) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(lock) = self.locks.get(key) {
            return lock.clone();
        }

        self.locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Fetches stale/missing keys from Redis in a single pipeline, then updates
    /// `self.store` with the fresh remote totals.
    async fn batch_refresh_stale(&self, keys: &[&RedisKey]) -> Result<(), DistkitError> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut stale_keys = Vec::with_capacity(keys.len());

        for key in keys {
            let Some(store) = self.store.get(*key) else {
                stale_keys.push(*key);
                continue;
            };

            if let Ok(last_flushed) = mutex_lock(&store.last_flushed, "last_flushed")
                && let Some(last_flushed) = last_flushed.deref()
                && last_flushed.elapsed() < self.allowed_lag
            {
                continue;
            }

            stale_keys.push(*key);
        }

        // To be honest, still contemplating whether to flush to redis here.
        // I'd just flush for now to be safe
        let mut batch = self.batch.lock().await;
        self.flush_to_redis(&mut batch, MAX_BATCH_SIZE).await?;

        let mut conn = self.connection_manager.clone();
        let script = &self.get_script;

        let raw: Vec<(String, i64)> =
            execute_pipeline_with_script_retry(&mut conn, script, &stale_keys, |key| {
                let mut inv = script.key(self.key_generator.container_key());
                inv.key(key.as_str());
                inv
            })
            .await?;

        let map: HashMap<String, i64> = raw.into_iter().collect();

        for key in stale_keys {
            let remote_total = map.get(key.as_str()).copied().unwrap_or(0);

            match self.store.get(key) {
                Some(store) => {
                    store.remote_total.store(remote_total, Ordering::Release);
                    *mutex_lock(&store.last_updated, "last_updated")? = Instant::now();
                }
                None => {
                    let value = self
                        .store
                        .entry((*key).clone())
                        .or_insert_with(|| SingleStore {
                            remote_total: AtomicI64::new(remote_total),
                            delta: AtomicI64::new(0),
                            last_updated: Mutex::new(Instant::now()),
                            last_flushed: Mutex::new(None),
                        });

                    value.remote_total.store(remote_total, Ordering::Release);
                    *mutex_lock(&value.last_updated, "last_updated")? = Instant::now();
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl CounterTrait for LaxCounter {
    /// Buffers `count` locally and returns the updated local estimate without
    /// a Redis round-trip. Multiple `inc` calls accumulate into a single
    /// `HINCRBY` that is flushed after `allowed_lag` (default 20 ms).
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_counter().await?;
    /// let key = RedisKey::try_from("hits".to_string())?;
    /// // All three calls are sub-microsecond; no Redis round-trip until flush.
    /// assert_eq!(counter.inc(&key, 1).await?, 1);
    /// assert_eq!(counter.inc(&key, 1).await?, 2);
    /// assert_eq!(counter.inc(&key, 1).await?, 3);
    /// // After ~20 ms the background task sends a single HINCRBY +3 to Redis.
    /// # Ok(())
    /// # }
    /// ```
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.activity.signal();

        let store = match self.store.get(key) {
            Some(store)
                if mutex_lock(&store.last_updated, "last_updated")?.elapsed()
                    < self.allowed_lag =>
            {
                store
            }
            Some(store) => {
                drop(store);

                self.ensure_valid_state(key).await?;

                self.store.get(key).expect("store should be present here")
            }
            None => {
                self.ensure_valid_state(key).await?;

                self.store.get(key).expect("store should be present here")
            }
        };

        let prev_delta = if count > 0 {
            store.delta.fetch_add(count, Ordering::AcqRel)
        } else {
            store.delta.fetch_sub(count.abs(), Ordering::AcqRel)
        };

        let total = store.remote_total.load(Ordering::Acquire) + prev_delta + count;

        Ok(total)
    } // end function inc

    /// Buffers `-count` locally and returns the updated local estimate without
    /// a Redis round-trip. Equivalent to `inc(key, -count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_counter().await?;
    /// let key = RedisKey::try_from("tokens".to_string())?;
    /// counter.set(&key, 10).await?;
    /// assert_eq!(counter.dec(&key, 3).await?, 7);
    /// # Ok(())
    /// # }
    /// ```
    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.inc(key, -count).await
    } // end function dec

    /// Returns the local view of the counter: the last remote total plus any
    /// pending local delta. If the cached remote total is older than
    /// `allowed_lag`, it is re-fetched from Redis first.
    ///
    /// Reads within the same process are always up-to-date. A separate
    /// process only sees writes after the writing process has flushed and
    /// the reading process's own cache has expired.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_counter().await?;
    /// let key = RedisKey::try_from("hits".to_string())?;
    /// counter.inc(&key, 7).await?;
    /// // Returns remote_total (0) + pending_delta (7) = 7, no Redis round-trip.
    /// assert_eq!(counter.get(&key).await?, 7);
    /// # Ok(())
    /// # }
    /// ```
    async fn get(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        self.activity.signal();
        let store = match self.store.get(key) {
            Some(store)
                if mutex_lock(&store.last_updated, "last_updated")?.elapsed()
                    < self.allowed_lag =>
            {
                store
            }
            Some(store) => {
                drop(store);

                self.ensure_valid_state(key).await?;

                self.store.get(key).expect("store should be present here")
            }
            None => {
                self.ensure_valid_state(key).await?;

                self.store.get(key).expect("store should be present here")
            }
        };

        let delta = store.delta.load(Ordering::Acquire);
        let total = store.remote_total.load(Ordering::Acquire) + delta;

        Ok(total)
    } // end function get

    /// Records a target value locally. The background flush task sends a
    /// corrective `HINCRBY` to Redis so the stored total reaches `count`.
    /// Until flushed, other processes reading from Redis see the old value.
    ///
    /// Returns `count`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_counter().await?;
    /// let key = RedisKey::try_from("inventory".to_string())?;
    /// counter.inc(&key, 1000).await?;
    /// // The write is buffered; this process sees the new value immediately.
    /// assert_eq!(counter.set(&key, 850).await?, 850);
    /// assert_eq!(counter.get(&key).await?, 850);
    /// # Ok(())
    /// # }
    /// ```
    async fn set(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.activity.signal();
        let store = match self.store.get(key) {
            Some(store)
                if mutex_lock(&store.last_updated, "last_updated")?.elapsed()
                    < self.allowed_lag =>
            {
                store
            }
            Some(store) => {
                drop(store);

                self.ensure_valid_state(key).await?;

                self.store.get(key).expect("store should be present here")
            }
            None => {
                self.ensure_valid_state(key).await?;

                self.store.get(key).expect("store should be present here")
            }
        };

        let total = store.remote_total.load(Ordering::Acquire);

        store.delta.store(count - total, Ordering::Release);

        Ok(count)
    } // end function set

    /// Cancels any pending local delta for `key`, then immediately deletes
    /// it from Redis. Returns the final value, including the cancelled delta.
    ///
    /// Unlike `inc` and `set`, `del` is **not** buffered — the Redis write
    /// happens immediately to prevent the key from reappearing on the next
    /// flush.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_counter().await?;
    /// let key = RedisKey::try_from("session".to_string())?;
    /// counter.inc(&key, 10).await?; // buffered, not yet in Redis
    /// // Pending delta (10) is cancelled; Redis is updated immediately.
    /// assert_eq!(counter.del(&key).await?, 10);
    /// assert_eq!(counter.get(&key).await?, 0);
    /// # Ok(())
    /// # }
    /// ```
    async fn del(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        self.activity.signal();

        let lock = self.get_or_create_lock(key).await;
        let _guard = lock.lock().await;

        {
            let mut batch = self.batch.lock().await;
            batch.retain(|commit| commit.key != *key);
        }

        let Some((_key, store)) = self.store.remove(key) else {
            return Ok(0);
        };

        let mut conn = self.connection_manager.clone();

        let total: i64 = self
            .del_script
            .key(self.key_generator.container_key())
            .key(key.as_str())
            .invoke_async(&mut conn)
            .await?;

        let total = total + store.delta.swap(0, Ordering::AcqRel);

        Ok(total)
    } // end function delete

    /// Clears all pending local state and immediately removes all counters
    /// under the current prefix from Redis.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_counter().await?;
    /// let k1 = RedisKey::try_from("a".to_string())?;
    /// let k2 = RedisKey::try_from("b".to_string())?;
    /// counter.inc(&k1, 5).await?;
    /// counter.inc(&k2, 10).await?;
    /// counter.clear().await?;
    /// assert_eq!(counter.get(&k1).await?, 0);
    /// assert_eq!(counter.get(&k2).await?, 0);
    /// # Ok(())
    /// # }
    /// ```
    async fn clear(&self) -> Result<(), DistkitError> {
        self.activity.signal();

        self.store.clear();

        {
            let mut batch = self.batch.lock().await;
            batch.clear();
        }

        let mut conn = self.connection_manager.clone();

        let _: () = self
            .clear_script
            .key(self.key_generator.container_key())
            .invoke_async(&mut conn)
            .await?;

        Ok(())
    } // end function clear

    async fn get_all<'k>(
        &self,
        keys: &[&'k RedisKey],
    ) -> Result<Vec<(&'k RedisKey, i64)>, DistkitError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        self.batch_refresh_stale(keys).await?;

        keys.iter()
            .map(|key| {
                let store = self.store.get(*key).expect("store populated after refresh");
                Ok((
                    *key,
                    store.remote_total.load(Ordering::Acquire)
                        + store.delta.load(Ordering::Acquire),
                ))
            })
            .collect()
    } // end function get_all

    async fn set_all<'k>(
        &self,
        updates: &[(&'k RedisKey, i64)],
    ) -> Result<Vec<(&'k RedisKey, i64)>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let keys: Vec<&RedisKey> = updates.iter().map(|(key, _)| *key).collect();

        self.batch_refresh_stale(&keys).await?;

        updates
            .iter()
            .map(|(key, count)| {
                let store = self.store.get(*key).expect("store populated after refresh");
                let remote_total = store.remote_total.load(Ordering::Acquire);
                store.delta.store(count - remote_total, Ordering::Release);
                Ok((*key, *count))
            })
            .collect()
    } // end function set_all
} // end impl CounterTrait for LaxCounter
