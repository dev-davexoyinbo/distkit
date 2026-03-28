use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use dashmap::DashMap;
use redis::{Script, aio::ConnectionManager};
use tokio::time::Instant;

use crate::{
    DistkitError, RedisKey, RedisKeyGenerator, RedisKeyGeneratorTypeKey, counter::CounterTrait,
};

const GET_LUA: &str = r#"
    local container_key = KEYS[1]
    local key = KEYS[2]

    return redis.call('HGET', container_key, key) or 0
"#;

#[derive(Debug)]
struct SingleStore {
    remote_total: i64,
    delta: AtomicI64,
    last_updated: Instant,
    last_flushed: Option<Instant>,
}

#[derive(Debug)]
pub struct LaxCounter {
    prefix: RedisKey,
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
    store: DashMap<RedisKey, SingleStore>,
    locks: DashMap<RedisKey, Arc<tokio::sync::Mutex<()>>>,
    get_script: Script,
    allowed_lag: Duration,
}

impl LaxCounter {
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        let key_generator =
            RedisKeyGenerator::new(prefix.clone(), RedisKeyGeneratorTypeKey::LaxCounter);

        let get_script = Script::new(GET_LUA);

        Self {
            prefix,
            connection_manager,
            key_generator,
            store: DashMap::default(),
            get_script,
            allowed_lag: Duration::from_millis(20),
            locks: DashMap::default(),
        }
    }

    async fn ensure_valid_state(&self, key: &RedisKey) -> Result<(), DistkitError> {
        let lock = self.get_or_create_lock(key).await;
        let _guard = lock.lock().await;

        {
            let store = self.store.get(key);

            if let Some(ref store) = store
                && let SingleStore { last_updated, .. } = store.deref()
                && last_updated.elapsed() < self.allowed_lag
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

        let mut store = self
            .store
            .entry(key.clone())
            .or_insert_with(|| SingleStore {
                remote_total,
                delta: AtomicI64::new(0),
                last_updated: Instant::now(),
                last_flushed: None,
            });

        store.remote_total = remote_total;
        store.last_updated = Instant::now();

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
        let store = match self.store.get(key) {
            Some(store) => store,
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

        let total = store.remote_total + prev_delta + count;

        Ok(total)
    } // end function inc

    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        self.inc(key, -count).await
    } // end function dec

    async fn get(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        todo!()
    } // end function get

    async fn set(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        todo!()
    } // end function set

    async fn del(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        todo!()
    } // end function delete

    async fn clear(&self) -> Result<(), DistkitError> {
        todo!()
    }
} // end impl CounterTrait for LaxCounter
