use std::{
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
    mutex_lock,
};

const GET_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]

    return redis.call('HGET', container_key, key) or 0
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

                    if let Err(err) = counter.flush_to_redis(&mut batch, 100).await {
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
        let mut connection_manager = self.connection_manager.clone();

        let pipe = self.build_commit_pipeline(commits, false);

        let _: () = match pipe.query_async(&mut connection_manager).await {
            Ok(results) => results,
            Err(err) => {
                if err.kind() != redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) {
                    return Err(DistkitError::RedisError(err));
                }

                let pipe = self.build_commit_pipeline(commits, true);

                match pipe.query_async::<()>(&mut connection_manager).await {
                    Ok(results) => results,
                    Err(err) => {
                        return Err(DistkitError::RedisError(err));
                    }
                }
            }
        };

        Ok(())
    } // end method batch_commit_state

    #[inline]
    fn build_commit_pipeline(
        &self,
        commits: &[Commit],
        should_load_script: bool,
    ) -> redis::Pipeline {
        let mut pipe = redis::Pipeline::new();
        if should_load_script {
            pipe.load_script(&self.commit_state_script).ignore();
        }

        for commit in commits {
            pipe.invoke_script(
                self.commit_state_script
                    .key(self.key_generator.container_key())
                    .key(commit.key.to_string())
                    .arg(commit.delta),
            );
        }

        pipe
    }

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

        let remote_total: i64 = self
            .get_script
            .key(self.key_generator.container_key())
            .key(key.to_string())
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
}

#[async_trait::async_trait]
impl CounterTrait for LaxCounter {
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

    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.inc(key, -count).await
    } // end function dec

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
            .key(key.to_string())
            .invoke_async(&mut conn)
            .await?;

        let total = total + store.delta.swap(0, Ordering::AcqRel);

        Ok(total)
    } // end function delete

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
} // end impl CounterTrait for LaxCounter
