// Shared helpers imported via `mod common;` in each bench binary.
// Bench binaries are external to the crate — only the public API is available.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use distkit::DistkitRedisKey;
use distkit::counter::{CounterOptions, LaxCounter, StrictCounter};
use distkit::icounter::{
    LaxInstanceAwareCounter, LaxInstanceAwareCounterOptions, StrictInstanceAwareCounter,
    StrictInstanceAwareCounterOptions,
};
use distkit::lock::{LockOptions, Mutex, RwLock};
use redis::aio::ConnectionManager;

pub async fn make_connection() -> ConnectionManager {
    let url = std::env::var("REDIS_URL").expect("REDIS_URL must be set — run via `make bench`");
    let client = redis::Client::open(url).expect("REDIS_URL is not a valid Redis URL");
    client
        .get_connection_manager()
        .await
        .expect("could not connect to Redis — is it running?")
}

pub async fn make_strict_counter(bench_name: &str) -> Arc<StrictCounter> {
    let conn = make_connection().await;
    StrictCounter::new(CounterOptions::new(bench_prefix(bench_name), conn))
}

pub async fn make_strict_icounter(bench_name: &str) -> Arc<StrictInstanceAwareCounter> {
    let conn = make_connection().await;
    StrictInstanceAwareCounter::new(StrictInstanceAwareCounterOptions::new(
        bench_prefix(bench_name),
        conn,
    ))
}

pub async fn make_lax_icounter(bench_name: &str) -> Arc<LaxInstanceAwareCounter> {
    let conn = make_connection().await;
    LaxInstanceAwareCounter::new(LaxInstanceAwareCounterOptions::new(
        bench_prefix(bench_name),
        conn,
    ))
}

pub async fn make_lax_counter(bench_name: &str) -> Arc<LaxCounter> {
    let conn = make_connection().await;
    LaxCounter::new(CounterOptions::new(bench_prefix(bench_name), conn))
}

pub async fn make_mutex(bench_name: &str) -> Arc<Mutex> {
    let conn = make_connection().await;
    Mutex::new(LockOptions::new(bench_prefix(bench_name), conn))
}

pub async fn make_rwlock(bench_name: &str) -> Arc<RwLock> {
    let conn = make_connection().await;
    RwLock::new(LockOptions::new(bench_prefix(bench_name), conn))
}

/// Two mutexes (distinct owners) bound to the **same** unique key, for measuring
/// the contended `try_lock` fast-fail path.
pub async fn make_contended_mutexes(bench_name: &str) -> (Arc<Mutex>, Arc<Mutex>) {
    let conn = make_connection().await;
    let key = bench_prefix(bench_name);
    let holder = Mutex::new(
        LockOptions::builder(key.clone(), conn.clone())
            .owner_id("holder")
            .build(),
    );
    let contender = Mutex::new(LockOptions::builder(key, conn).owner_id("contender").build());
    (holder, contender)
}

/// Builds a `DistkitRedisKey` from a plain name string.
pub fn key(name: &str) -> DistkitRedisKey {
    DistkitRedisKey::try_from(name.to_string())
        .expect("bench key must be non-empty, ≤255 chars, and colon-free")
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn bench_prefix(bench_name: &str) -> DistkitRedisKey {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    DistkitRedisKey::try_from(format!("bench_{}_{}", ts, bench_name))
        .expect("constructed bench prefix is always valid")
}
