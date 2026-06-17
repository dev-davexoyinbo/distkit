// Stub harness for the lock test suite. Helpers are unused until Stage 2+ adds
// the mutex / rwlock test modules.
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::aio::ConnectionManager;

use crate::lock::DEFAULT_LOCK_NAMESPACE;
use crate::{DistkitRedisKey, lock::LockOptions};

static RUN_ID: OnceLock<u128> = OnceLock::new();

fn run_id() -> u128 {
    *RUN_ID.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    })
}

/// Opens a fresh live Redis connection for direct commands in tests.
pub async fn raw_connection() -> ConnectionManager {
    let url = std::env::var("REDIS_URL").expect("REDIS_URL must be set — run via `make test`");
    let client = redis::Client::open(url).expect("valid Redis URL");
    client
        .get_connection_manager()
        .await
        .expect("Redis must be reachable")
}

/// Builds [`LockOptions`] on a process-unique key derived from `name`, so
/// concurrent test runs never collide on the same Redis resource.
pub async fn make_options(name: &str) -> LockOptions {
    make_options_with_key(name).await.0
}

/// Like [`make_options`], but also returns the precomputed full Redis key
/// (`{namespace}:{key}`) so tests can target the exact key directly.
pub async fn make_options_with_key(name: &str) -> (LockOptions, String) {
    let conn = raw_connection().await;
    let unique_key = format!("{}_{}", run_id(), name);
    let full_key = format!("{DEFAULT_LOCK_NAMESPACE}:{unique_key}");
    let options = LockOptions::new(DistkitRedisKey::from(unique_key), conn);
    (options, full_key)
}

/// Like [`make_options`], but with a short 20 ms retry interval for snappy
/// contention tests using the `try_*_until` / `lock`/`read`/`write` forms that
/// source their poll interval from the lock's configuration.
pub async fn make_fast_options(name: &str) -> LockOptions {
    let mut options = make_options(name).await;
    options.retry_interval = Duration::from_millis(20);
    options
}

pub fn key(name: &str) -> DistkitRedisKey {
    DistkitRedisKey::from(name.to_string())
}

/// The four derived Redis keys an `RwLock` uses for one resource.
pub struct RwKeys {
    pub writer: String,
    pub readers: String,
    pub pending: String,
    pub pending_heartbeat: String,
}

/// Like [`make_options_with_key`], but returns the four `:w`/`:r`/`:pw`/`:pwh`
/// keys the rwlock backend operates on, derived from the same base full key.
pub async fn make_options_with_rw_keys(name: &str) -> (LockOptions, RwKeys) {
    let (options, base) = make_options_with_key(name).await;
    let keys = RwKeys {
        writer: format!("{base}:w"),
        readers: format!("{base}:r"),
        pending: format!("{base}:pw"),
        pending_heartbeat: format!("{base}:pwh"),
    };
    (options, keys)
}
