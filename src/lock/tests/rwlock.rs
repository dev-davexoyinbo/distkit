//! Live-Redis tests for [`RwLock`](crate::lock::RwLock) and its guards:
//! shared reads, writer/reader exclusion, writer preference, bounded waiting,
//! release/drop-frees semantics, auto-refresh, and lost-lease state.

use std::time::Duration;

use crate::DistkitError;
use crate::lock::tests::common::{
    make_fast_options, make_options, make_options_with_rw_keys, raw_connection,
};
use crate::lock::{LockError, RwLock, LockGuardState};

/// Multiple readers share the lock: two `try_read` guards are held at once.
#[tokio::test]
async fn concurrent_reads_share() {
    let rw_a = RwLock::new(make_options("concurrent_reads_share").await);
    let rw_b = RwLock::new(make_options("concurrent_reads_share").await);

    let _read_a = rw_a.try_read().await.expect("first reader should acquire");
    let _read_b = rw_b
        .try_read()
        .await
        .expect("second reader should share the lock");
}

/// A held write excludes readers: `try_read` fails while a writer holds.
#[tokio::test]
async fn writer_excludes_readers() {
    let writer = RwLock::new(make_options("writer_excludes_readers").await);
    let reader = RwLock::new(make_options("writer_excludes_readers").await);

    let _write_guard = writer.try_write().await.expect("writer should acquire");

    match reader.try_read().await {
        Err(DistkitError::LockError(LockError::AcquireFail)) => {}
        other => panic!("expected AcquireFail while write held, got {other:?}"),
    }
}

/// A writer waits while readers hold, then acquires once they release.
#[tokio::test]
async fn writer_waits_for_readers() {
    let reader = RwLock::new(make_options("writer_waits_for_readers").await);
    let writer = RwLock::new(make_fast_options("writer_waits_for_readers").await);

    let read_guard = reader.try_read().await.expect("reader should acquire");

    let waiter = tokio::spawn(async move { writer.try_write_with_timeout(Duration::from_secs(2)).await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    read_guard.release().await.expect("read release should succeed");

    waiter
        .await
        .expect("waiter task should not panic")
        .expect("writer should acquire after readers release");
}

/// A reader waits while a writer holds, then acquires once it releases.
#[tokio::test]
async fn reader_waits_for_writer() {
    let writer = RwLock::new(make_options("reader_waits_for_writer").await);
    let reader = RwLock::new(make_fast_options("reader_waits_for_writer").await);

    let write_guard = writer.try_write().await.expect("writer should acquire");

    let waiter = tokio::spawn(async move { reader.try_read_with_timeout(Duration::from_secs(2)).await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    write_guard
        .release()
        .await
        .expect("write release should succeed");

    waiter
        .await
        .expect("waiter task should not panic")
        .expect("reader should acquire after writer releases");
}

/// Writer preference: while a writer is waiting (enqueued), new readers are
/// turned away so they cannot jump ahead of it.
#[tokio::test]
async fn waiting_writer_blocks_new_readers() {
    let reader = RwLock::new(make_options("waiting_writer_blocks_new_readers").await);
    let writer = RwLock::new(make_fast_options("waiting_writer_blocks_new_readers").await);
    let late_reader = RwLock::new(make_options("waiting_writer_blocks_new_readers").await);

    let _read_guard = reader.try_read().await.expect("first reader should acquire");

    // A waiting (enqueuing) writer blocked by the held reader.
    let waiter = tokio::spawn(async move { writer.try_write_with_timeout(Duration::from_secs(2)).await });

    // Let the writer register itself as pending at least once.
    tokio::time::sleep(Duration::from_millis(150)).await;

    match late_reader.try_read().await {
        Err(DistkitError::LockError(LockError::AcquireFail)) => {}
        other => panic!("expected AcquireFail — waiting writer should block new readers, got {other:?}"),
    }

    waiter.abort();
}

/// Releasing a write frees the resource for the next writer.
#[tokio::test]
async fn write_release_frees() {
    let writer_a = RwLock::new(make_options("write_release_frees").await);
    let writer_b = RwLock::new(make_options("write_release_frees").await);

    let guard = writer_a.try_write().await.expect("writer should acquire");
    guard.release().await.expect("write release should succeed");

    writer_b
        .try_write()
        .await
        .expect("write should be acquirable after release");
}

/// Dropping a read guard frees the slot (best-effort fire-and-forget release), so
/// a writer can then acquire.
#[tokio::test]
async fn read_drop_frees() {
    let reader = RwLock::new(make_options("read_drop_frees").await);
    let writer = RwLock::new(make_options("read_drop_frees").await);

    {
        let _read_guard = reader.try_read().await.expect("reader should acquire");
    }

    // Drop release is spawned/async — give it a moment to land.
    tokio::time::sleep(Duration::from_millis(150)).await;

    writer
        .try_write()
        .await
        .expect("write should be acquirable after the reader drops");
}

/// Background auto-refresh keeps a held write lease alive past its `ttl`.
#[tokio::test]
async fn auto_refresh_keeps_lease_alive() {
    let mut options = make_options("auto_refresh_keeps_lease_alive").await;
    options.ttl = Duration::from_millis(150);
    let writer_a = RwLock::new(options);
    let writer_b = RwLock::new(make_options("auto_refresh_keeps_lease_alive").await);

    let _guard = writer_a.try_write().await.expect("writer should acquire");

    // Well past ttl (150 ms) — without refresh the lease would have expired.
    tokio::time::sleep(Duration::from_millis(400)).await;

    match writer_b.try_write().await {
        Err(DistkitError::LockError(LockError::AcquireFail)) => {}
        other => panic!("expected AcquireFail — refresh should keep lease alive, got {other:?}"),
    }
}

/// A healthy held read reports [`LockGuardState::Acquired`].
#[tokio::test]
async fn get_state_reports_acquired() {
    let reader = RwLock::new(make_options("get_state_reports_acquired").await);

    let guard = reader.try_read().await.expect("reader should acquire");

    assert_eq!(guard.get_state().await, LockGuardState::Acquired);
}

/// An uncontended read/write wins on the first poll: `get_on_attempt` is `0`.
#[tokio::test]
async fn get_on_attempt_zero_when_uncontended() {
    let reader = RwLock::new(make_options("rw_get_on_attempt_zero").await);
    let writer = RwLock::new(make_options("rw_get_on_attempt_zero_w").await);

    let read_guard = reader.try_read().await.expect("reader should acquire");
    assert_eq!(read_guard.get_on_attempt().await, 0);

    let write_guard = writer.try_write().await.expect("writer should acquire");
    assert_eq!(write_guard.get_on_attempt().await, 0);
}

/// A reader that blocks behind a held writer only wins after retries:
/// `get_on_attempt` is non-zero.
#[tokio::test]
async fn get_on_attempt_counts_retries_under_contention() {
    let writer = RwLock::new(make_options("rw_get_on_attempt_retries").await);
    let reader = RwLock::new(make_fast_options("rw_get_on_attempt_retries").await);

    let write_guard = writer.try_write().await.expect("writer should acquire");

    let waiter = tokio::spawn(async move { reader.try_read_with_timeout(Duration::from_secs(2)).await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    write_guard
        .release()
        .await
        .expect("write release should succeed");

    let guard = waiter
        .await
        .expect("waiter task should not panic")
        .expect("reader should acquire after writer releases");

    assert!(
        guard.get_on_attempt().await >= 1,
        "a contended acquire should take more than one attempt"
    );
}

/// If the write lease is lost (writer key deleted out from under us), the refresh
/// task marks it [`LockGuardState::Lost`]: `get_state` reports it and `release`
/// returns `Ok(Lost)` without issuing a delete we no longer own.
#[tokio::test]
async fn lost_lease_reports_lost() {
    let (mut options, keys) = make_options_with_rw_keys("lost_lease_reports_lost").await;
    options.ttl = Duration::from_millis(150);
    let writer = RwLock::new(options);

    let guard = writer.try_write().await.expect("writer should acquire");

    // Delete the writer key out from under the holder; the next refresh tick
    // (~50 ms) sees GET != owner -> Ok(false) -> lost.
    let mut conn = raw_connection().await;
    let _: () = redis::cmd("DEL")
        .arg(&keys.writer)
        .query_async(&mut conn)
        .await
        .expect("DEL should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        guard.get_state().await,
        LockGuardState::Lost,
        "lease should be marked lost after the writer key was deleted"
    );

    match guard.release().await {
        Ok(LockGuardState::Lost) => {}
        other => panic!("expected Ok(LockGuardState::Lost) after the lease was deleted, got {other:?}"),
    }
}

/// A one-shot `try_write` on a read-held lock fails without enqueuing, so a
/// subsequent `try_read` still succeeds (the failed write did not block readers).
#[tokio::test]
async fn try_write_does_not_block_readers() {
    let reader = RwLock::new(make_options("try_write_does_not_block_readers").await);
    let writer = RwLock::new(make_options("try_write_does_not_block_readers").await);
    let late_reader = RwLock::new(make_options("try_write_does_not_block_readers").await);

    let _read_guard = reader.try_read().await.expect("first reader should acquire");

    match writer.try_write().await {
        Err(DistkitError::LockError(LockError::AcquireFail)) => {}
        other => panic!("expected AcquireFail for one-shot try_write, got {other:?}"),
    }

    late_reader
        .try_read()
        .await
        .expect("reader should still acquire — one-shot try_write must not enqueue");
}

/// Retry-bounded reads and writes against a held writer exhaust their retries
/// and fail with `RetriesExhausted`.
#[tokio::test]
async fn try_with_retries_exhausts() {
    let writer = RwLock::new(make_options("rw_try_with_retries_exhausts").await);
    let reader = RwLock::new(make_fast_options("rw_try_with_retries_exhausts").await);
    let other_writer = RwLock::new(make_fast_options("rw_try_with_retries_exhausts").await);

    let _write_guard = writer.try_write().await.expect("writer should acquire");

    match reader.try_read_with_retries(3).await {
        Err(DistkitError::LockError(LockError::RetriesExhausted { retries: 3 })) => {}
        other => panic!("expected RetriesExhausted {{ retries: 3 }} from try_read_with_retries, got {other:?}"),
    }

    match other_writer.try_write_with_retries(3).await {
        Err(DistkitError::LockError(LockError::RetriesExhausted { retries: 3 })) => {}
        other => panic!("expected RetriesExhausted {{ retries: 3 }} from try_write_with_retries, got {other:?}"),
    }
}

/// A retry-bounded writer waits while readers hold, then acquires once they
/// release (within the retry budget).
#[tokio::test]
async fn try_write_with_retries_succeeds() {
    let reader = RwLock::new(make_options("rw_try_write_with_retries_succeeds").await);
    let writer = RwLock::new(make_fast_options("rw_try_write_with_retries_succeeds").await);

    let read_guard = reader.try_read().await.expect("reader should acquire");

    let waiter = tokio::spawn(async move { writer.try_write_with_retries(50).await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    read_guard.release().await.expect("read release should succeed");

    waiter
        .await
        .expect("waiter task should not panic")
        .expect("writer should acquire after readers release");
}

/// The deprecated `try_read_for` / `try_write_for` still work: bounded attempts
/// against a held writer fail with `Timeout`. Retained for coverage of the
/// legacy API.
#[allow(deprecated)]
#[tokio::test]
async fn try_for_methods_still_work() {
    let writer = RwLock::new(make_options("rw_try_for_still_works").await);
    let reader = RwLock::new(make_options("rw_try_for_still_works").await);
    let other_writer = RwLock::new(make_options("rw_try_for_still_works").await);

    let _write_guard = writer.try_write().await.expect("writer should acquire");

    match reader
        .try_read_for(Duration::from_millis(100), Duration::from_millis(20))
        .await
    {
        Err(DistkitError::LockError(LockError::Timeout { .. })) => {}
        other => panic!("expected Timeout from try_read_for, got {other:?}"),
    }

    match other_writer
        .try_write_for(Duration::from_millis(100), Duration::from_millis(20))
        .await
    {
        Err(DistkitError::LockError(LockError::Timeout { .. })) => {}
        other => panic!("expected Timeout from try_write_for, got {other:?}"),
    }
}
