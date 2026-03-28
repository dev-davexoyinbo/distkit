use crate::{RedisKey, counter::CounterError};

#[async_trait::async_trait]
pub trait CounterTrait {
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<i64, CounterError>;
    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, CounterError>;
    async fn get(&self, key: &RedisKey) -> Result<i64, CounterError>;
} // end trait CounterTrait
