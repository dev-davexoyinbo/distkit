//! Direct-against-Redis tests for the Stage 1 mutex backend
//! (`crate::lock::mutex_backend`): owner-gated acquire / refresh / release.

use redis::aio::ConnectionManager;

use crate::lock::mutex_backend;
use crate::lock::tests::common::make_options;

const OWNER_A: &str = "owner_a";
const OWNER_B: &str = "owner_b";

/// Returns a cloned connection and a process-unique raw key for `name`.
async fn conn_and_key(name: &str) -> (ConnectionManager, String) {
    let opts = make_options(name).await;
    (opts.connection_manager.clone(), opts.key.to_string())
}

async fn exists(conn: &mut ConnectionManager, key: &str) -> bool {
    let n: i64 = redis::cmd("EXISTS")
        .arg(key)
        .query_async(conn)
        .await
        .expect("EXISTS");
    n == 1
}

async fn pttl(conn: &mut ConnectionManager, key: &str) -> i64 {
    redis::cmd("PTTL")
        .arg(key)
        .query_async(conn)
        .await
        .expect("PTTL")
}

#[tokio::test]
async fn acquire_is_exclusive() {
    let (mut conn, key) = conn_and_key("acquire_is_exclusive").await;

    assert!(
        mutex_backend::acquire(&mut conn, &key, OWNER_A, 30_000)
            .await
            .unwrap(),
        "first acquire should take the lock"
    );
    assert!(
        !mutex_backend::acquire(&mut conn, &key, OWNER_B, 30_000)
            .await
            .unwrap(),
        "second acquire on a held key should fail"
    );
}

#[tokio::test]
async fn refresh_is_owner_gated() {
    let (mut conn, key) = conn_and_key("refresh_is_owner_gated").await;

    // Short initial lease so a successful refresh visibly bumps the PTTL.
    assert!(
        mutex_backend::acquire(&mut conn, &key, OWNER_A, 2_000)
            .await
            .unwrap()
    );

    assert!(
        mutex_backend::refresh(&mut conn, &key, OWNER_A, 30_000)
            .await
            .unwrap(),
        "owner refresh should succeed"
    );
    assert!(
        pttl(&mut conn, &key).await > 2_000,
        "owner refresh should extend the lease"
    );

    assert!(
        !mutex_backend::refresh(&mut conn, &key, OWNER_B, 30_000)
            .await
            .unwrap(),
        "non-owner refresh should fail"
    );
}

#[tokio::test]
async fn release_is_owner_gated() {
    let (mut conn, key) = conn_and_key("release_is_owner_gated").await;

    assert!(
        mutex_backend::acquire(&mut conn, &key, OWNER_A, 30_000)
            .await
            .unwrap()
    );

    assert!(
        !mutex_backend::release(&mut conn, &key, OWNER_B).await.unwrap(),
        "non-owner release should fail"
    );
    assert!(
        exists(&mut conn, &key).await,
        "key should survive a non-owner release"
    );

    assert!(
        mutex_backend::release(&mut conn, &key, OWNER_A).await.unwrap(),
        "owner release should succeed"
    );
    assert!(
        !exists(&mut conn, &key).await,
        "key should be gone after owner release"
    );
}

#[tokio::test]
async fn acquire_succeeds_after_release() {
    let (mut conn, key) = conn_and_key("acquire_succeeds_after_release").await;

    assert!(
        mutex_backend::acquire(&mut conn, &key, OWNER_A, 30_000)
            .await
            .unwrap()
    );
    assert!(mutex_backend::release(&mut conn, &key, OWNER_A).await.unwrap());
    assert!(
        mutex_backend::acquire(&mut conn, &key, OWNER_B, 30_000)
            .await
            .unwrap(),
        "key should be acquirable again after release"
    );
}

#[tokio::test]
async fn lease_expiry_frees_key() {
    let (mut conn, key) = conn_and_key("lease_expiry_frees_key").await;

    assert!(mutex_backend::acquire(&mut conn, &key, OWNER_A, 100).await.unwrap());

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    assert!(
        mutex_backend::acquire(&mut conn, &key, OWNER_B, 30_000)
            .await
            .unwrap(),
        "key should be acquirable once the lease expires"
    );
}
