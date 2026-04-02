use std::{
    ops::{Deref, DerefMut},
    sync::Mutex,
    time::Duration,
};

mod activity_tracker;
pub(crate) use activity_tracker::*;

pub(crate) const EPOCH_CHANGE_INTERVAL: Duration = Duration::from_secs(15);

use crate::DistkitError;

/// A validated Redis key.
///
/// Keys must be non-empty, at most 255 characters, and must not contain
/// colons (`:`). Construct via [`TryFrom<String>`].
///
/// `RedisKey` dereferences to [`String`], so all string methods are
/// available through auto-deref.
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct RedisKey(String);

impl RedisKey {
    #[cfg(test)]
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
    #[strum(to_string = "instance_aware_counter")]
    InstanceAwareCounter,
    #[strum(to_string = "lax_instance_aware_counter")]
    LaxInstanceAwareCounter,
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

    pub(crate) fn container_key(&self) -> String {
        format!("{}:{}", *self.prefix, self.key_type)
    }
}

pub(crate) fn mutex_lock<'a, T>(
    m: &'a Mutex<T>,
    what: &'static str,
) -> Result<std::sync::MutexGuard<'a, T>, DistkitError> {
    m.lock().map_err(|_| DistkitError::MutexPoisoned(what))
}
