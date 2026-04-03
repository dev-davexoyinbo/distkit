//! Setup helpers shared across all doc-test examples.
//!
//! This module is compiled **only** when `cargo test --doc` runs
//! (`#[cfg(doctest)]`). It is not part of the public API and will not appear
//! in the generated documentation.

use std::sync::Arc;

use crate::{
    RedisKey,
    counter::{CounterOptions, LaxCounter, StrictCounter},
    icounter::{
        LaxInstanceAwareCounter, LaxInstanceAwareCounterOptions, StrictInstanceAwareCounter,
        StrictInstanceAwareCounterOptions,
    },
};

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn unique_prefix() -> Result<RedisKey, crate::DistkitError> {
    RedisKey::try_from(format!("test_{}", uuid::Uuid::new_v4()))
}

/// Creates a [`StrictCounter`] with a unique Redis prefix.
pub async fn strict_counter() -> Result<Arc<StrictCounter>, Box<dyn std::error::Error>> {
    let conn = redis::Client::open(redis_url())?.get_connection_manager().await?;
    Ok(StrictCounter::new(CounterOptions::new(unique_prefix()?, conn)))
}

/// Creates a [`LaxCounter`] with a unique Redis prefix.
pub async fn lax_counter() -> Result<Arc<LaxCounter>, Box<dyn std::error::Error>> {
    let conn = redis::Client::open(redis_url())?.get_connection_manager().await?;
    Ok(LaxCounter::new(CounterOptions::new(unique_prefix()?, conn)))
}

/// Creates a single [`StrictInstanceAwareCounter`] with a unique Redis prefix.
pub async fn strict_icounter(
) -> Result<Arc<StrictInstanceAwareCounter>, Box<dyn std::error::Error>> {
    let conn = redis::Client::open(redis_url())?.get_connection_manager().await?;
    Ok(StrictInstanceAwareCounter::new(
        StrictInstanceAwareCounterOptions::new(unique_prefix()?, conn),
    ))
}

/// Creates two [`StrictInstanceAwareCounter`]s sharing the same Redis prefix,
/// simulating two independent server instances.
pub async fn two_strict_icounters() -> Result<
    (
        Arc<StrictInstanceAwareCounter>,
        Arc<StrictInstanceAwareCounter>,
    ),
    Box<dyn std::error::Error>,
> {
    let client = redis::Client::open(redis_url())?;
    let conn1 = client.get_connection_manager().await?;
    let conn2 = client.get_connection_manager().await?;
    let prefix = unique_prefix()?;
    let opts = |conn| StrictInstanceAwareCounterOptions::new(prefix.clone(), conn);
    Ok((
        StrictInstanceAwareCounter::new(opts(conn1)),
        StrictInstanceAwareCounter::new(opts(conn2)),
    ))
}

/// Creates a single [`LaxInstanceAwareCounter`] with a unique Redis prefix.
pub async fn lax_icounter() -> Result<Arc<LaxInstanceAwareCounter>, Box<dyn std::error::Error>> {
    let conn = redis::Client::open(redis_url())?.get_connection_manager().await?;
    Ok(LaxInstanceAwareCounter::new(
        LaxInstanceAwareCounterOptions::new(unique_prefix()?, conn),
    ))
}

/// Creates two [`LaxInstanceAwareCounter`]s sharing the same Redis prefix,
/// simulating two independent server instances.
pub async fn two_lax_icounters() -> Result<
    (
        Arc<LaxInstanceAwareCounter>,
        Arc<LaxInstanceAwareCounter>,
    ),
    Box<dyn std::error::Error>,
> {
    let client = redis::Client::open(redis_url())?;
    let conn1 = client.get_connection_manager().await?;
    let conn2 = client.get_connection_manager().await?;
    let prefix = unique_prefix()?;
    let opts = |conn| LaxInstanceAwareCounterOptions::new(prefix.clone(), conn);
    Ok((
        LaxInstanceAwareCounter::new(opts(conn1)),
        LaxInstanceAwareCounter::new(opts(conn2)),
    ))
}
