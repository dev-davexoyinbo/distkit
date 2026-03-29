// Shared helpers imported via `mod common;` in each bench binary.
// Bench binaries are external to the crate — only the public API is available.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use distkit::counter::{CounterOptions, LaxCounter, StrictCounter};
use distkit::RedisKey;
use redis::aio::ConnectionManager;

pub async fn make_connection() -> ConnectionManager {
    let url = std::env::var("REDIS_URL")
        .expect("REDIS_URL must be set — run via `make bench`");
    let client = redis::Client::open(url).expect("REDIS_URL is not a valid Redis URL");
    client
        .get_connection_manager()
        .await
        .expect("could not connect to Redis — is it running?")
}

pub async fn make_strict_counter(bench_name: &str) -> StrictCounter {
    let conn = make_connection().await;
    StrictCounter::new(CounterOptions::new(bench_prefix(bench_name), conn))
}

pub async fn make_lax_counter(bench_name: &str) -> Arc<LaxCounter> {
    let conn = make_connection().await;
    LaxCounter::new(CounterOptions::new(bench_prefix(bench_name), conn))
}

/// Builds a `RedisKey` from a plain name string.
pub fn key(name: &str) -> RedisKey {
    RedisKey::try_from(name.to_string())
        .expect("bench key must be non-empty, ≤255 chars, and colon-free")
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn bench_prefix(bench_name: &str) -> RedisKey {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    RedisKey::try_from(format!("bench_{}_{}", ts, bench_name))
        .expect("constructed bench prefix is always valid")
}
