use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis::aio::ConnectionManager;

use crate::DistkitRedisKey;
use crate::icounter::{StrictInstanceAwareCounter, StrictInstanceAwareCounterOptions};

static RUN_ID: OnceLock<u128> = OnceLock::new();

pub fn run_id() -> u128 {
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

pub async fn make_counter(prefix: &str) -> Arc<StrictInstanceAwareCounter> {
    let conn = make_connection().await;
    let unique_prefix = format!("{}_{}", run_id(), prefix);
    StrictInstanceAwareCounter::new(StrictInstanceAwareCounterOptions::new(
        DistkitRedisKey::from(unique_prefix),
        conn,
    ))
}

#[allow(dead_code)]
pub async fn make_counter_with_opts(
    prefix: &str,
    threshold_ms: u64,
) -> Arc<StrictInstanceAwareCounter> {
    let conn = make_connection().await;
    let unique_prefix = format!("{}_{}", run_id(), prefix);
    StrictInstanceAwareCounter::new(StrictInstanceAwareCounterOptions {
        prefix: DistkitRedisKey::from(unique_prefix),
        connection_manager: conn,
        dead_instance_threshold_ms: threshold_ms,
        cleanup_interval: None,
    })
}

/// Two counters sharing the same Redis prefix (different instance IDs).
pub async fn make_pair(
    prefix: &str,
) -> (
    Arc<StrictInstanceAwareCounter>,
    Arc<StrictInstanceAwareCounter>,
) {
    let conn1 = make_connection().await;
    let conn2 = make_connection().await;
    let unique_prefix = format!("{}_{}", run_id(), prefix);

    let c1 = StrictInstanceAwareCounter::new(StrictInstanceAwareCounterOptions::new(
        DistkitRedisKey::from(unique_prefix.clone()),
        conn1,
    ));
    let c2 = StrictInstanceAwareCounter::new(StrictInstanceAwareCounterOptions::new(
        DistkitRedisKey::from(unique_prefix),
        conn2,
    ));
    (c1, c2)
}

pub async fn make_pair_with_opts(
    prefix: &str,
    threshold_ms: u64,
) -> (
    Arc<StrictInstanceAwareCounter>,
    Arc<StrictInstanceAwareCounter>,
) {
    let conn1 = make_connection().await;
    let conn2 = make_connection().await;
    let unique_prefix = format!("{}_{}", run_id(), prefix);

    let opts = |conn| StrictInstanceAwareCounterOptions {
        prefix: DistkitRedisKey::from(unique_prefix.clone()),
        connection_manager: conn,
        dead_instance_threshold_ms: threshold_ms,
        cleanup_interval: None,
    };

    let c1 = StrictInstanceAwareCounter::new(opts(conn1));
    let c2 = StrictInstanceAwareCounter::new(opts(conn2));
    (c1, c2)
}

/// Creates `n` counters sharing the same Redis prefix (each with a unique instance ID).
pub async fn make_n_counters(prefix: &str, n: usize) -> Vec<Arc<StrictInstanceAwareCounter>> {
    let unique_prefix = format!("{}_{}", run_id(), prefix);
    let mut counters = Vec::with_capacity(n);
    for _ in 0..n {
        let conn = make_connection().await;
        counters.push(StrictInstanceAwareCounter::new(
            StrictInstanceAwareCounterOptions::new(
                DistkitRedisKey::from(unique_prefix.clone()),
                conn,
            ),
        ));
    }
    counters
}

pub async fn make_n_counters_with_opts(
    prefix: &str,
    n: usize,
    threshold_ms: u64,
) -> Vec<Arc<StrictInstanceAwareCounter>> {
    let unique_prefix = format!("{}_{}", run_id(), prefix);
    let mut counters = Vec::with_capacity(n);
    for _ in 0..n {
        let conn = make_connection().await;
        counters.push(StrictInstanceAwareCounter::new(
            StrictInstanceAwareCounterOptions {
                prefix: DistkitRedisKey::from(unique_prefix.clone()),
                connection_manager: conn,
                dead_instance_threshold_ms: threshold_ms,
                cleanup_interval: None,
            },
        ));
    }
    counters
}

pub fn key(name: &str) -> DistkitRedisKey {
    DistkitRedisKey::from(name.to_string())
}

/// The Redis container of a standalone [`StrictInstanceAwareCounter`].
pub fn icounter_container(prefix: &str) -> String {
    format!("{prefix}:instance_aware_counter")
}

/// The Redis container of the strict counter backing a lax one.
pub fn lax_icounter_container(prefix: &str) -> String {
    format!("{prefix}:lax_instance_aware_counter")
}

/// Whether the Redis hash at `hash_key` holds `field`, read raw so a test
/// can tell a deleted record from one stored as 0.
pub async fn field_exists(hash_key: &str, field: &str) -> bool {
    let mut conn = make_connection().await;
    redis::cmd("HEXISTS")
        .arg(hash_key)
        .arg(field)
        .query_async(&mut conn)
        .await
        .expect("HEXISTS should succeed")
}

/// Whether `redis_key` exists at all.
pub async fn key_exists(redis_key: &str) -> bool {
    let mut conn = make_connection().await;
    redis::cmd("EXISTS")
        .arg(redis_key)
        .query_async(&mut conn)
        .await
        .expect("EXISTS should succeed")
}

/// Whether the set at `set_key` holds `member`.
pub async fn set_member_exists(set_key: &str, member: &str) -> bool {
    let mut conn = make_connection().await;
    redis::cmd("SISMEMBER")
        .arg(set_key)
        .arg(member)
        .query_async(&mut conn)
        .await
        .expect("SISMEMBER should succeed")
}

/// Writes a raw hash field, as a pre-0.7.2 deployment would have left it.
pub async fn hset_raw(hash_key: &str, field: &str, value: i64) {
    let mut conn = make_connection().await;
    let _: () = redis::cmd("HSET")
        .arg(hash_key)
        .arg(field)
        .arg(value)
        .query_async(&mut conn)
        .await
        .expect("HSET should succeed");
}

/// Registers a raw instance id with a last-seen time, as the counters do.
pub async fn zadd_raw(zset_key: &str, member: &str, score_ms: u128) {
    let mut conn = make_connection().await;
    let _: () = redis::cmd("ZADD")
        .arg(zset_key)
        .arg(score_ms as u64)
        .arg(member)
        .query_async(&mut conn)
        .await
        .expect("ZADD should succeed");
}

/// Adds a raw member to the keys set, as the counters do.
pub async fn sadd_raw(set_key: &str, member: &str) {
    let mut conn = make_connection().await;
    let _: () = redis::cmd("SADD")
        .arg(set_key)
        .arg(member)
        .query_async(&mut conn)
        .await
        .expect("SADD should succeed");
}
