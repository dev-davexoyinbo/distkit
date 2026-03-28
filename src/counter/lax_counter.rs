use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, atomic::AtomicI64},
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
enum SingleStore {
    Undefined,
    Active {
        remote_total: i64,
        delta: AtomicI64,
        last_updated: Instant,
        last_flushed: Option<Instant>,
    },
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

    async fn get_remote_total(&self, key: &RedisKey) -> Result<i64, DistkitError> {
        let lock = self.get_or_create_lock(key).await;
        let _guard = lock.lock().await;

        {
            let store = self.store.get(key);

            if let Some(ref store) = store
                && let SingleStore::Active {
                    last_updated,
                    remote_total,
                    ..
                } = store.deref()
                && last_updated.elapsed() < self.allowed_lag
            {
                return Ok(*remote_total);
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
            .or_insert_with(|| SingleStore::Active {
                remote_total,
                delta: AtomicI64::new(0),
                last_updated: Instant::now(),
                last_flushed: None,
            });

        match store.deref_mut() {
            SingleStore::Undefined => {
                *store = SingleStore::Active {
                    remote_total,
                    delta: AtomicI64::new(0),
                    last_updated: Instant::now(),
                    last_flushed: None,
                };
            }
            SingleStore::Active {
                remote_total: remote_total_old,
                last_updated,
                ..
            } => {
                *remote_total_old = remote_total;
                *last_updated = Instant::now();
            }
        };

        Ok(remote_total)
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
                self.store
                    .entry(key.clone())
                    .or_insert_with(|| SingleStore::Undefined);

                self.store.get(key).expect("store should be present here")
            }
        };

        todo!()
    } // end function inc

    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError> {
        todo!()
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
