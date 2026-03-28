use crate::{
    RedisKey,
    counter::{CounterError, CounterTrait},
};

#[derive(Debug, Clone)]
pub struct LaxCounter {
    prefix: RedisKey,
}

impl LaxCounter {
    pub fn new(prefix: RedisKey) -> Self {
        Self { prefix }
    }
}

#[async_trait::async_trait]
impl CounterTrait for LaxCounter {
    async fn inc(&self, key: String, count: i64) -> Result<(), CounterError> {
        todo!()
    } // end function inc

    async fn dec(&self, key: String, count: i64) -> Result<(), CounterError> {
        todo!()
    } // end function dec

    async fn get(&self, key: String) -> Result<i64, CounterError> {
        todo!()
    } // end function get
} // end impl CounterTrait for LaxCounter
