use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use redis::aio::ConnectionManager;

use crate::{
    DistkitRedisKey,
    counter::{CounterOptions, LaxCounter, StrictCounter},
};

static RUN_ID: OnceLock<u128> = OnceLock::new();

fn run_id() -> u128 {
    *RUN_ID.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    })
}

pub async fn make_connection() -> ConnectionManager {
    let url = std::env::var("REDIS_URL").expect("REDIS_URL must be set — run via `make test`");
    let client = redis::Client::open(url).expect("valid Redis URL");
    client
        .get_connection_manager()
        .await
        .expect("Redis must be reachable")
}

pub async fn make_strict_counter(prefix: &str) -> Arc<StrictCounter> {
    let conn = make_connection().await;
    let unique_prefix = format!("{}_{}", run_id(), prefix);
    StrictCounter::new(CounterOptions::new(
        DistkitRedisKey::from(unique_prefix),
        conn,
    ))
}

pub async fn make_lax_counter(prefix: &str) -> Arc<LaxCounter> {
    let conn = make_connection().await;
    let unique_prefix = format!("{}_{}", run_id(), prefix);
    LaxCounter::new(CounterOptions::new(
        DistkitRedisKey::from(unique_prefix),
        conn,
    ))
}

pub fn key(name: &str) -> DistkitRedisKey {
    DistkitRedisKey::from(name.to_string())
}

/// Returns a unique prefix string for the given test name.
pub fn unique_prefix(name: &str) -> String {
    format!("{}_{}", run_id(), name)
}

/// The Redis hash a [`StrictCounter`] with this prefix stores its records in.
pub fn strict_container_key(prefix: &str) -> String {
    format!("{prefix}:strict_counter")
}

/// The Redis hash a [`LaxCounter`] with this prefix stores its records in.
pub fn lax_container_key(prefix: &str) -> String {
    format!("{prefix}:lax_counter")
}

pub async fn make_strict_counter_from_prefix(prefix: &str) -> Arc<StrictCounter> {
    let conn = make_connection().await;
    StrictCounter::new(CounterOptions::new(
        DistkitRedisKey::from(prefix.to_string()),
        conn,
    ))
}

pub async fn make_lax_counter_from_prefix(prefix: &str) -> Arc<LaxCounter> {
    let conn = make_connection().await;
    LaxCounter::new(CounterOptions::new(
        DistkitRedisKey::from(prefix.to_string()),
        conn,
    ))
}

/// Whether the Redis hash at `container_key` holds `field`, read raw so a
/// test can tell a deleted record from one stored as 0.
pub async fn field_exists(container_key: &str, field: &str) -> bool {
    let mut conn = make_connection().await;
    redis::cmd("HEXISTS")
        .arg(container_key)
        .arg(field)
        .query_async(&mut conn)
        .await
        .expect("HEXISTS should succeed")
}

/// Writes a raw hash field, as a pre-0.7.2 deployment would have left it.
pub async fn hset_raw(container_key: &str, field: &str, value: i64) {
    let mut conn = make_connection().await;
    let _: () = redis::cmd("HSET")
        .arg(container_key)
        .arg(field)
        .arg(value)
        .query_async(&mut conn)
        .await
        .expect("HSET should succeed");
}
