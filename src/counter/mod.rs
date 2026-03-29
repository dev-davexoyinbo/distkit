//! Distributed counter implementations.
//!
//! This module provides [`StrictCounter`] (immediate consistency) and
//! [`LaxCounter`] (eventual consistency with in-memory buffering). Both
//! implement [`CounterTrait`]. Use [`Counter`] as a facade to access both
//! from a single configuration.

mod lax_counter;
use std::{sync::Arc, time::Duration};

pub use lax_counter::*;

mod strict_counter;
use redis::aio::ConnectionManager;
pub use strict_counter::*;

mod counter_trait;
pub use counter_trait::*;

mod common;

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
    pub(crate) prefix: RedisKey,
    pub(crate) connection_manager: ConnectionManager,
    pub(crate) allowed_lag: Duration,
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

/// Facade providing access to both [`StrictCounter`] and [`LaxCounter`]
/// instances that share the same Redis prefix.
#[derive(Debug, Clone)]
pub struct Counter {
    lax: Arc<LaxCounter>,
    strict: Arc<StrictCounter>,
}

impl Counter {
    /// Creates both counter types from the given options.
    pub fn new(options: CounterOptions) -> Self {
        Self {
            strict: Arc::new(StrictCounter::new(options.clone())),
            lax: LaxCounter::new(options.clone()),
        }
    }

    /// Returns a reference to the [`LaxCounter`] (eventual consistency).
    pub fn lax(&self) -> &LaxCounter {
        &self.lax
    }

    /// Returns a reference to the [`StrictCounter`] (immediate consistency).
    pub fn strict(&self) -> &StrictCounter {
        &self.strict
    }
}
