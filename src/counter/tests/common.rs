use redis::aio::ConnectionManager;

use crate::{RedisKey, counter::StrictCounter};

pub async fn make_strict_counter(prefix: &str) -> StrictCounter {
    let url = std::env::var("REDIS_URL")
        .expect("REDIS_URL must be set — run via `make test`");

    let client = redis::Client::open(url).expect("valid Redis URL");

    let conn: ConnectionManager = client
        .get_connection_manager()
        .await
        .expect("Redis must be reachable");

    StrictCounter::new(RedisKey::from(prefix.to_string()), conn)
}

pub fn key(name: &str) -> RedisKey {
    RedisKey::from(name.to_string())
}
