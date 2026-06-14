//! Tests for [`LockOptions`](crate::lock::LockOptions) defaults and the
//! [`LockOptionsBuilder`](crate::lock::LockOptionsBuilder) overrides /
//! entry points. A live `ConnectionManager` is required for construction, so
//! these run under `make test`.

use std::time::Duration;

use redis::aio::ConnectionManager;

use crate::DistkitRedisKey;
use crate::lock::tests::common::make_options;
use crate::lock::{LockOptions, LockOptionsBuilder};

/// Borrows a process-unique key and a live connection from the shared harness.
async fn key_and_connection(name: &str) -> (DistkitRedisKey, ConnectionManager) {
    let options = make_options(name).await;
    (options.key.clone(), options.connection_manager.clone())
}

#[tokio::test]
async fn new_sets_documented_defaults() {
    let (key, connection) = key_and_connection("new_sets_documented_defaults").await;
    let options = LockOptions::new(key, connection);

    assert_eq!(*options.namespace, "distkit-locks");
    assert_eq!(options.ttl, Duration::from_secs(30));
    assert_eq!(options.max_wait, None);
    assert_eq!(options.retry_interval, Duration::from_millis(50));
    assert!(options.auto_refresh);
    assert!(
        options.owner_id.is_some_and(|owner| !owner.is_empty()),
        "owner_id should default to a non-empty UUID"
    );
}

#[tokio::test]
async fn new_generates_a_unique_owner_id_each_time() {
    let (key, connection) = key_and_connection("new_generates_a_unique_owner_id_each_time").await;
    let first = LockOptions::new(key.clone(), connection.clone());
    let second = LockOptions::new(key, connection);

    assert_ne!(
        first.owner_id, second.owner_id,
        "each LockOptions::new should mint a distinct owner_id"
    );
}

#[tokio::test]
async fn builder_defaults_match_new() {
    let (key, connection) = key_and_connection("builder_defaults_match_new").await;
    let from_new = LockOptions::new(key.clone(), connection.clone());
    let from_builder = LockOptionsBuilder::new(key, connection).build();

    // owner_id is randomly minted per construction, so compare every other field.
    assert_eq!(from_builder.key, from_new.key);
    assert_eq!(from_builder.namespace, from_new.namespace);
    assert_eq!(from_builder.ttl, from_new.ttl);
    assert_eq!(from_builder.max_wait, from_new.max_wait);
    assert_eq!(from_builder.retry_interval, from_new.retry_interval);
    assert_eq!(from_builder.auto_refresh, from_new.auto_refresh);
}

#[tokio::test]
async fn builder_overrides_every_field() {
    let (key, connection) = key_and_connection("builder_overrides_every_field").await;
    let custom_namespace = DistkitRedisKey::from("custom-namespace".to_string());

    let options = LockOptionsBuilder::new(key, connection)
        .namespace(custom_namespace.clone())
        .ttl(Duration::from_secs(5))
        .owner_id("fixed-owner")
        .max_wait(Duration::from_secs(2))
        .retry_interval(Duration::from_millis(10))
        .auto_refresh(false)
        .build();

    assert_eq!(options.namespace, custom_namespace);
    assert_eq!(options.ttl, Duration::from_secs(5));
    assert_eq!(options.owner_id.as_deref(), Some("fixed-owner"));
    assert_eq!(options.max_wait, Some(Duration::from_secs(2)));
    assert_eq!(options.retry_interval, Duration::from_millis(10));
    assert!(!options.auto_refresh);
}

#[tokio::test]
async fn lock_options_builder_entry_point_matches_builder_new() {
    let (key, connection) = key_and_connection("lock_options_builder_entry_point").await;

    // `LockOptions::builder` is just sugar for `LockOptionsBuilder::new` — it
    // yields the same defaults.
    let options = LockOptions::builder(key, connection)
        .ttl(Duration::from_secs(7))
        .build();

    assert_eq!(*options.namespace, "distkit-locks");
    assert_eq!(options.ttl, Duration::from_secs(7));
    assert_eq!(options.retry_interval, Duration::from_millis(50));
    assert!(options.auto_refresh);
}
