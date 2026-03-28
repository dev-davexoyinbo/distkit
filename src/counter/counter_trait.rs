use crate::counter::CounterError;

#[async_trait::async_trait]
pub trait CounterTrait {
    async fn inc(&self, key: String, count: i64) -> Result<(), CounterError>;
    async fn dec(&self, key: String, count: i64) -> Result<(), CounterError>;
    async fn get(&self, key: String) -> Result<i64, CounterError>;
} // end trait CounterTrait
