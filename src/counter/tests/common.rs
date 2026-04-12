use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use redis::aio::ConnectionManager;

use crate::{
    RedisKey,
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

async fn make_connection() -> ConnectionManager {
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
    StrictCounter::new(CounterOptions::new(RedisKey::from(unique_prefix), conn))
}

pub async fn make_lax_counter(prefix: &str) -> Arc<LaxCounter> {
    let conn = make_connection().await;
    let unique_prefix = format!("{}_{}", run_id(), prefix);
    LaxCounter::new(CounterOptions::new(RedisKey::from(unique_prefix), conn))
}

pub fn key(name: &str) -> RedisKey {
    RedisKey::from(name.to_string())
}
