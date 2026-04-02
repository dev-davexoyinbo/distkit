use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use redis::aio::ConnectionManager;

use crate::RedisKey;
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
        RedisKey::from(unique_prefix),
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
        prefix: RedisKey::from(unique_prefix),
        connection_manager: conn,
        dead_instance_threshold_ms: threshold_ms,
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
        RedisKey::from(unique_prefix.clone()),
        conn1,
    ));
    let c2 = StrictInstanceAwareCounter::new(StrictInstanceAwareCounterOptions::new(
        RedisKey::from(unique_prefix),
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
        prefix: RedisKey::from(unique_prefix.clone()),
        connection_manager: conn,
        dead_instance_threshold_ms: threshold_ms,
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
            StrictInstanceAwareCounterOptions::new(RedisKey::from(unique_prefix.clone()), conn),
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
                prefix: RedisKey::from(unique_prefix.clone()),
                connection_manager: conn,
                dead_instance_threshold_ms: threshold_ms,
            },
        ));
    }
    counters
}

pub fn key(name: &str) -> RedisKey {
    RedisKey::from(name.to_string())
}
