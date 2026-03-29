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

#[derive(Debug, Clone)]
pub struct CounterOptions {
    prefix: RedisKey,
    connection_manager: ConnectionManager,
    allowed_lag: Duration,
}

impl CounterOptions {
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            prefix,
            connection_manager,
            allowed_lag: Duration::from_millis(20),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Counter {
    lax: Arc<LaxCounter>,
    strict: Arc<StrictCounter>,
}

impl Counter {
    pub fn new(options: CounterOptions) -> Self {
        Self {
            strict: Arc::new(StrictCounter::new(options.clone())),
            lax: LaxCounter::new(options.clone()),
        }
    } // end new

    pub fn lax(&self) -> &LaxCounter {
        &self.lax
    } // end function lax

    pub fn strict(&self) -> &StrictCounter {
        &self.strict
    } // end function strict
} // end impl Counter
