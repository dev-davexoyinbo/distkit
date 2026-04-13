use std::{
    ops::{Deref, DerefMut},
    sync::{LazyLock, Mutex},
    time::Duration,
};

use redis::aio::ConnectionManager;
use regex::Regex;

mod activity_tracker;
pub(crate) use activity_tracker::*;

pub(crate) const EPOCH_CHANGE_INTERVAL: Duration = Duration::from_secs(15);

use crate::DistkitError;

static REDIS_KEY_STRIP_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r":").expect("REDIS_KEY_STRIP_RE is valid"));

pub(crate) async fn execute_pipeline_with_script_retry<'s, T, I, F>(
    conn: &mut ConnectionManager,
    script: &'s redis::Script,
    items: &[I],
    build_invocation: F,
) -> Result<T, DistkitError>
where
    T: redis::FromRedisValue,
    F: Fn(&I) -> redis::ScriptInvocation<'s>,
{
    let mut pipe = redis::Pipeline::new();

    for item in items {
        pipe.invoke_script(&build_invocation(item));
    }

    match pipe.query_async::<T>(conn).await {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) => {
            let mut retry_pipe = redis::Pipeline::new();

            retry_pipe.load_script(script).ignore();

            for item in items {
                retry_pipe.invoke_script(&build_invocation(item));
            }

            retry_pipe
                .query_async::<T>(conn)
                .await
                .map_err(DistkitError::RedisError)
        }
        Err(err) => Err(DistkitError::RedisError(err)),
    }
}

/// A validated Redis key.
///
/// All Redis-backed distkit operations require keys wrapped in this type.
/// Validation happens at construction time, whether you use
/// [`DistkitRedisKey::new`], [`TryFrom<String>`], or
/// [`DistkitRedisKey::new_or_panic`].
///
/// # Validation Rules
///
/// - Must not be empty
/// - Must be 255 bytes or shorter
/// - Must not contain `:` because distkit uses colons internally as separators
///
/// `DistkitRedisKey` dereferences to [`String`], so standard string methods are
/// available through auto-deref.
///
/// # Examples
///
/// ```
/// use distkit::DistkitRedisKey;
///
/// let key = DistkitRedisKey::new("user_123".to_string()).unwrap();
/// let key = DistkitRedisKey::try_from("api_v2_endpoint".to_string()).unwrap();
/// let key = DistkitRedisKey::new_or_panic("team_alpha".to_string());
///
/// assert!(DistkitRedisKey::try_from("user:123".to_string()).is_err());
/// assert!(DistkitRedisKey::try_from("".to_string()).is_err());
/// assert!(DistkitRedisKey::try_from("a".repeat(256)).is_err());
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct DistkitRedisKey(String);

impl DistkitRedisKey {
    /// Returns the default Redis namespace prefix used by distkit.
    ///
    /// # Examples
    ///
    /// ```
    /// use distkit::DistkitRedisKey;
    ///
    /// assert_eq!(*DistkitRedisKey::default_prefix(), "distkit");
    /// ```
    pub fn default_prefix() -> Self {
        Self("distkit".to_string())
    }

    /// Fallible constructor. Equivalent to [`TryFrom<String>`].
    ///
    /// # Examples
    ///
    /// ```
    /// use distkit::DistkitRedisKey;
    ///
    /// let key = DistkitRedisKey::new("orders".to_string())?;
    /// assert_eq!(*key, "orders");
    /// # Ok::<(), distkit::DistkitError>(())
    /// ```
    pub fn new(value: String) -> Result<Self, DistkitError> {
        Self::try_from(value)
    }

    /// Panicking constructor for validated keys.
    ///
    /// # Examples
    ///
    /// ```
    /// use distkit::DistkitRedisKey;
    ///
    /// let key = DistkitRedisKey::new_or_panic("orders".to_string());
    /// assert_eq!(*key, "orders");
    /// ```
    pub fn new_or_panic(value: String) -> Self {
        Self::try_from(value).expect("invalid DistkitRedisKey")
    }

    /// Strips colons from `value`, then validates the sanitized result.
    ///
    /// # Examples
    ///
    /// ```
    /// use distkit::DistkitRedisKey;
    ///
    /// let key = DistkitRedisKey::try_sanitize("user:123".to_string())?;
    /// assert_eq!(*key, "user123");
    ///
    /// assert!(DistkitRedisKey::try_sanitize(":".to_string()).is_err());
    /// # Ok::<(), distkit::DistkitError>(())
    /// ```
    pub fn try_sanitize(value: String) -> Result<Self, DistkitError> {
        let sanitized = REDIS_KEY_STRIP_RE.replace_all(&value, "").into_owned();
        Self::try_from(sanitized)
    }

    /// Strips colons from `value` and returns the sanitized key.
    ///
    /// Panics if the sanitized result is still invalid.
    ///
    /// # Examples
    ///
    /// ```
    /// use distkit::DistkitRedisKey;
    ///
    /// let key = DistkitRedisKey::sanitize_or_panic("user:123".to_string());
    /// assert_eq!(*key, "user123");
    /// ```
    pub fn sanitize_or_panic(value: String) -> Self {
        Self::try_sanitize(value).expect("sanitized DistkitRedisKey value is still invalid")
    }

    #[cfg(test)]
    pub(crate) fn from(value: String) -> Self {
        Self(value)
    }
}

impl Deref for DistkitRedisKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DistkitRedisKey {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TryFrom<String> for DistkitRedisKey {
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

/// Backwards-compatible alias for [`DistkitRedisKey`].
#[doc(hidden)]
pub type RedisKey = DistkitRedisKey;

#[derive(Clone, Debug, strum_macros::Display)]
pub(crate) enum RedisKeyGeneratorTypeKey {
    #[strum(to_string = "lax_counter")]
    Lax,
    #[strum(to_string = "strict_counter")]
    Strict,
    #[strum(to_string = "instance_aware_counter")]
    InstanceAware,
    #[strum(to_string = "lax_instance_aware_counter")]
    LaxInstanceAware,
}

#[derive(Clone, Debug)]
pub(crate) struct RedisKeyGenerator {
    prefix: DistkitRedisKey,
    key_type: RedisKeyGeneratorTypeKey,
}

impl RedisKeyGenerator {
    pub(crate) fn new(prefix: DistkitRedisKey, key_type: RedisKeyGeneratorTypeKey) -> Self {
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
