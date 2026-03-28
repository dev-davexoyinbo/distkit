use std::ops::{Deref, DerefMut};

use crate::DistkitError;

#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct RedisKey(String);

impl RedisKey {
    pub(crate) fn from(value: String) -> Self {
        Self(value)
    }
}

impl Deref for RedisKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RedisKey {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TryFrom<String> for RedisKey {
    type Error = DistkitError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(DistkitError::InvalidRedisKey(
                "Redis key must not be empty".to_string(),
            ))
        } else if value.len() > 255 {
            Err(DistkitError::InvalidRedisKey(
                "Redis key must not be longer than 255 characters".to_string(),
            ))
        } else if value.contains(":") {
            Err(DistkitError::InvalidRedisKey(
                "Redis key must not contain colons".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }
}

#[derive(Clone, Debug, strum_macros::Display)]
pub(crate) enum RedisKeyGeneratorTypeKey {
    #[strum(to_string = "lax_counter")]
    LaxCounter,
    #[strum(to_string = "strict_counter")]
    StrictCounter,
}

#[derive(Clone, Debug)]
pub(crate) struct RedisKeyGenerator {
    prefix: RedisKey,
    key_type: RedisKeyGeneratorTypeKey,
}

impl RedisKeyGenerator {
    pub(crate) fn new(prefix: RedisKey, key_type: RedisKeyGeneratorTypeKey) -> Self {
        Self { prefix, key_type }
    }

    pub(crate) fn member_key(&self, member: &RedisKey) -> String {
        format!("{}:{}:{}", *self.prefix, self.key_type, **member)
    }
}
