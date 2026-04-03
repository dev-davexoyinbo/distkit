//! Distributed counter implementations.
//!
//! This module provides [`StrictCounter`] (immediate consistency) and
//! [`LaxCounter`] (eventual consistency with in-memory buffering). Both
//! implement [`CounterTrait`] and are constructed directly from [`CounterOptions`].

mod lax_counter;
use std::time::Duration;

pub use lax_counter::*;

mod strict_counter;
use redis::aio::ConnectionManager;
pub use strict_counter::*;

mod counter_trait;
pub use counter_trait::*;

mod error;
pub use error::*;

use crate::RedisKey;

#[cfg(test)]
mod tests;

/// Configuration for counter construction.
///
/// Carries a Redis key prefix, a connection manager, and the `allowed_lag`
/// duration used by [`LaxCounter`] to control how long local state is
/// considered fresh before re-fetching from Redis.
#[derive(Debug, Clone)]
pub struct CounterOptions {
    /// Redis key prefix used to namespace all counter keys.
    pub prefix: RedisKey,
    /// Redis connection manager for executing commands.
    pub connection_manager: ConnectionManager,
    /// Maximum acceptable staleness for [`LaxCounter`] reads (default 20 ms).
    /// Controls how often the background task flushes buffered writes to Redis.
    pub allowed_lag: Duration,
}

impl CounterOptions {
    /// Creates counter options with a default `allowed_lag` of 20 ms.
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            prefix,
            connection_manager,
            allowed_lag: Duration::from_millis(20),
        }
    }
}
