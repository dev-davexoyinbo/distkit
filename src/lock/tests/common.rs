// Stub harness for the lock test suite. Helpers are unused until Stage 2+ adds
// the mutex / rwlock test modules.
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

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

pub fn key(name: &str) -> DistkitRedisKey {
    DistkitRedisKey::from(name.to_string())
}
