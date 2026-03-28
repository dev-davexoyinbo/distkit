mod lax_counter;
use std::sync::Arc;

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

#[derive(Debug, Clone)]
pub struct Counter {
    lax: Arc<LaxCounter>,
    strict: Arc<StrictCounter>,
}

impl Counter {
    pub fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            strict: Arc::new(StrictCounter::new(
                prefix.clone(),
                connection_manager.clone(),
            )),
            lax: Arc::new(LaxCounter::new(prefix), connection_manager.clone()),
        }
    } // end new

    pub fn lax(&self) -> &LaxCounter {
        &self.lax
    } // end function lax

    pub fn strict(&self) -> &StrictCounter {
        &self.strict
    } // end function strict
} // end impl Counter
