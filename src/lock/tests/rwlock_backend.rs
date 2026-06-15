//! Direct-against-Redis tests for the rwlock backend
//! (`crate::lock::rwlock_backend`): shared reads, exclusive writes, the
//! writer-preference invariant (readers yield to a waiting writer), the
//! `mark_pending` flag (one-shot writes don't enqueue / block readers), FIFO
//! ordering among waiting writers, lazy purge of expired readers / dead waiting
//! writers, and owner-gated refresh / release.
//!
//! All ops on one lock pass a **consistent `ttl_ms`** — the reader/pending purge
//! thresholds are computed from the caller's `ttl_ms`, not stored per member, so
//! mixing ttls on the same key gives inconsistent expiry (see the module review).

use std::time::Duration;

use redis::aio::ConnectionManager;

use crate::lock::rwlock_backend::{self, AcquireOptions};
use crate::lock::tests::common::{RwKeys, make_options_with_rw_keys};

const READER_A: &str = "reader_a";
const READER_B: &str = "reader_b";
const WRITER_A: &str = "writer_a";
const WRITER_B: &str = "writer_b";

const LONG_TTL: i64 = 30_000;
const SHORT_TTL: i64 = 150;
/// Comfortably past `SHORT_TTL` so a short-lived member is purgeable.
const PAST_SHORT_TTL: Duration = Duration::from_millis(300);

async fn conn_and_keys(name: &str) -> (ConnectionManager, RwKeys) {
    let (opts, keys) = make_options_with_rw_keys(name).await;
    (opts.connection_manager.clone(), keys)
}

fn opts<'a>(k: &'a RwKeys, owner: &'a str, ttl_ms: i64) -> AcquireOptions<'a> {
    AcquireOptions {
        owner,
        ttl_ms,
        writer_key: k.writer.as_str(),
        readers_key: k.readers.as_str(),
        pending_writers_key: k.pending.as_str(),
        pending_writers_heartbeat_key: k.pending_heartbeat.as_str(),
    }
}

async fn acquire_read(conn: &mut ConnectionManager, k: &RwKeys, owner: &str, ttl: i64) -> bool {
    rwlock_backend::acquire_read(conn, opts(k, owner, ttl))
        .await
        .unwrap()
}

/// `acquire_write` that joins the writer queue when blocked (the waiting forms).
async fn write_waiting(conn: &mut ConnectionManager, k: &RwKeys, owner: &str, ttl: i64) -> bool {
    rwlock_backend::acquire_write(conn, opts(k, owner, ttl), true)
        .await
        .unwrap()
}

/// `acquire_write` that does not enqueue when blocked (one-shot `try_write`).
async fn write_oneshot(conn: &mut ConnectionManager, k: &RwKeys, owner: &str, ttl: i64) -> bool {
    rwlock_backend::acquire_write(conn, opts(k, owner, ttl), false)
        .await
        .unwrap()
}

async fn release_write(conn: &mut ConnectionManager, k: &RwKeys, owner: &str) -> bool {
    rwlock_backend::release_write(conn, &k.writer, &k.pending, &k.pending_heartbeat, owner)
        .await
        .unwrap()
}

async fn zcard(conn: &mut ConnectionManager, key: &str) -> i64 {
    redis::cmd("ZCARD")
        .arg(key)
        .query_async(conn)
        .await
        .expect("ZCARD")
}

async fn writer_exists(conn: &mut ConnectionManager, key: &str) -> bool {
    let n: i64 = redis::cmd("EXISTS")
        .arg(key)
        .query_async(conn)
        .await
        .expect("EXISTS");
    n == 1
}

/// Seed a zset member at an exact score, bypassing the backend — lets a test
/// place two pending writers at an *identical* arrival score (see
/// `same_ms_writer_fifo_is_lexical`).
async fn zadd(conn: &mut ConnectionManager, key: &str, score: f64, member: &str) {
    let _: i64 = redis::cmd("ZADD")
        .arg(key)
        .arg(score)
        .arg(member)
        .query_async(conn)
        .await
        .expect("ZADD");
}

#[tokio::test]
async fn read_is_shared() {
    let (mut conn, k) = conn_and_keys("read_is_shared").await;

    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    assert!(acquire_read(&mut conn, &k, READER_B, LONG_TTL).await);
    assert_eq!(
        zcard(&mut conn, &k.readers).await,
        2,
        "both readers hold the lock"
    );
}

#[tokio::test]
async fn writer_blocks_new_readers() {
    let (mut conn, k) = conn_and_keys("writer_blocks_new_readers").await;

    assert!(write_oneshot(&mut conn, &k, WRITER_A, LONG_TTL).await);
    assert!(
        !acquire_read(&mut conn, &k, READER_A, LONG_TTL).await,
        "a reader must not acquire while a writer holds the lock"
    );
}

#[tokio::test]
async fn write_waits_for_readers() {
    let (mut conn, k) = conn_and_keys("write_waits_for_readers").await;

    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    assert!(
        !write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "write fails while a reader holds the lock"
    );
    assert_eq!(
        zcard(&mut conn, &k.pending).await,
        1,
        "the blocked writer is queued"
    );

    assert!(
        rwlock_backend::release_read(&mut conn, &k.readers, READER_A)
            .await
            .unwrap()
    );
    assert!(
        write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "write succeeds once the reader is gone"
    );
    assert_eq!(
        zcard(&mut conn, &k.pending).await,
        0,
        "acquiring clears the queue entry"
    );
}

#[tokio::test]
async fn write_excludes_write() {
    let (mut conn, k) = conn_and_keys("write_excludes_write").await;

    assert!(write_oneshot(&mut conn, &k, WRITER_A, LONG_TTL).await);
    assert!(
        !write_oneshot(&mut conn, &k, WRITER_B, LONG_TTL).await,
        "a second writer fails while the first holds the lock"
    );
}

#[tokio::test]
async fn reentrant_write_succeeds() {
    let (mut conn, k) = conn_and_keys("reentrant_write_succeeds").await;

    assert!(write_oneshot(&mut conn, &k, WRITER_A, LONG_TTL).await);
    assert!(
        write_oneshot(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "the same owner re-acquiring its own write lock should succeed"
    );
}

/// The core invariant: once a writer is waiting, later readers may not jump ahead.
#[tokio::test]
async fn read_blocked_by_waiting_writer() {
    let (mut conn, k) = conn_and_keys("read_blocked_by_waiting_writer").await;

    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    assert!(
        !write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "writer now waiting"
    );

    assert!(
        !acquire_read(&mut conn, &k, READER_B, LONG_TTL).await,
        "a later reader must not jump ahead of the waiting writer"
    );

    // Writer abandons (release_write clears its pending entry); reader frees.
    assert!(
        !release_write(&mut conn, &k, WRITER_A).await,
        "abandon: not the holder"
    );
    assert!(
        rwlock_backend::release_read(&mut conn, &k.readers, READER_A)
            .await
            .unwrap()
    );
    assert!(
        acquire_read(&mut conn, &k, READER_B, LONG_TTL).await,
        "the reader proceeds once no writer is waiting"
    );
}

#[tokio::test]
async fn oneshot_write_does_not_enqueue_or_block_readers() {
    let (mut conn, k) = conn_and_keys("oneshot_write_does_not_enqueue_or_block_readers").await;

    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    assert!(
        !write_oneshot(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "one-shot write fails while a reader holds the lock"
    );
    assert_eq!(
        zcard(&mut conn, &k.pending).await,
        0,
        "mark_pending=false must not register a queue entry"
    );
    assert!(
        acquire_read(&mut conn, &k, READER_B, LONG_TTL).await,
        "a later reader proceeds since no writer is queued"
    );
}

#[tokio::test]
async fn waiting_write_enqueues_and_blocks_readers() {
    let (mut conn, k) = conn_and_keys("waiting_write_enqueues_and_blocks_readers").await;

    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    assert!(!write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await);
    assert_eq!(
        zcard(&mut conn, &k.pending).await,
        1,
        "mark_pending=true registers a queue entry"
    );
    assert!(
        !acquire_read(&mut conn, &k, READER_B, LONG_TTL).await,
        "a queued writer blocks later readers"
    );
}

#[tokio::test]
async fn writers_are_served_fifo() {
    let (mut conn, k) = conn_and_keys("writers_are_served_fifo").await;

    // A reader holds the lock so both writers queue.
    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    assert!(!write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await);
    // Separate arrival timestamps so FIFO order is unambiguous.
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(!write_waiting(&mut conn, &k, WRITER_B, LONG_TTL).await);

    assert!(
        rwlock_backend::release_read(&mut conn, &k.readers, READER_A)
            .await
            .unwrap()
    );

    assert!(
        !write_waiting(&mut conn, &k, WRITER_B, LONG_TTL).await,
        "the later writer must yield to the one that arrived first"
    );
    assert!(
        write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "the earliest-arrived writer wins the lock"
    );
    assert_eq!(
        zcard(&mut conn, &k.pending).await,
        1,
        "only WRITER_B remains queued"
    );
}

#[tokio::test]
async fn expired_readers_are_purged() {
    let (mut conn, k) = conn_and_keys("expired_readers_are_purged").await;

    assert!(acquire_read(&mut conn, &k, READER_A, SHORT_TTL).await);
    tokio::time::sleep(PAST_SHORT_TTL).await;

    assert!(
        write_oneshot(&mut conn, &k, WRITER_A, SHORT_TTL).await,
        "write succeeds once the stale reader is purged"
    );
}

#[tokio::test]
async fn dead_waiting_writer_is_purged() {
    let (mut conn, k) = conn_and_keys("dead_waiting_writer_is_purged").await;

    // Writer queues behind a reader, then stops heart-beating; reader frees.
    assert!(acquire_read(&mut conn, &k, READER_A, SHORT_TTL).await);
    assert!(!write_waiting(&mut conn, &k, WRITER_A, SHORT_TTL).await);
    assert!(
        rwlock_backend::release_read(&mut conn, &k.readers, READER_A)
            .await
            .unwrap()
    );

    tokio::time::sleep(PAST_SHORT_TTL).await;

    assert!(
        acquire_read(&mut conn, &k, READER_B, SHORT_TTL).await,
        "a reader proceeds once the dead waiting writer is purged"
    );
    assert_eq!(
        zcard(&mut conn, &k.pending).await,
        0,
        "the dead writer left the queue"
    );
}

#[tokio::test]
async fn refresh_read_renews_and_reports_loss() {
    let (mut conn, k) = conn_and_keys("refresh_read_renews_and_reports_loss").await;

    assert!(acquire_read(&mut conn, &k, READER_A, SHORT_TTL).await);
    assert!(
        rwlock_backend::refresh_read(&mut conn, &k.readers, READER_A, SHORT_TTL)
            .await
            .unwrap(),
        "refreshing a live reader slot succeeds"
    );

    // Stop refreshing READER_B and let its slot age out.
    assert!(acquire_read(&mut conn, &k, READER_B, SHORT_TTL).await);
    tokio::time::sleep(PAST_SHORT_TTL).await;
    assert!(
        !rwlock_backend::refresh_read(&mut conn, &k.readers, READER_B, SHORT_TTL)
            .await
            .unwrap(),
        "refreshing an expired reader slot reports the loss"
    );
}

#[tokio::test]
async fn refresh_write_is_owner_gated() {
    let (mut conn, k) = conn_and_keys("refresh_write_is_owner_gated").await;

    assert!(write_oneshot(&mut conn, &k, WRITER_A, LONG_TTL).await);
    assert!(
        rwlock_backend::refresh_write(&mut conn, &k.writer, WRITER_A, LONG_TTL)
            .await
            .unwrap(),
        "owner refresh succeeds"
    );
    assert!(
        !rwlock_backend::refresh_write(&mut conn, &k.writer, WRITER_B, LONG_TTL)
            .await
            .unwrap(),
        "non-owner refresh fails"
    );
}

#[tokio::test]
async fn release_write_is_owner_gated_and_frees() {
    let (mut conn, k) = conn_and_keys("release_write_is_owner_gated_and_frees").await;

    assert!(write_oneshot(&mut conn, &k, WRITER_A, LONG_TTL).await);

    assert!(
        !release_write(&mut conn, &k, WRITER_B).await,
        "non-owner release fails"
    );
    assert!(
        writer_exists(&mut conn, &k.writer).await,
        "key survives a non-owner release"
    );

    assert!(
        release_write(&mut conn, &k, WRITER_A).await,
        "owner release succeeds"
    );
    assert!(
        !writer_exists(&mut conn, &k.writer).await,
        "key gone after owner release"
    );
}

#[tokio::test]
async fn release_write_clears_pending_on_abandon() {
    let (mut conn, k) = conn_and_keys("release_write_clears_pending_on_abandon").await;

    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    assert!(!write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await);
    assert_eq!(zcard(&mut conn, &k.pending).await, 1);

    // A writer that gives up before acquiring uses release_write to leave the queue.
    assert!(
        !release_write(&mut conn, &k, WRITER_A).await,
        "never held the writer key"
    );
    assert_eq!(zcard(&mut conn, &k.pending).await, 0, "queue entry cleared");
    assert_eq!(
        zcard(&mut conn, &k.pending_heartbeat).await,
        0,
        "heartbeat cleared"
    );
}

// ---------------------------------------------------------------------------
// Edge-case / loophole characterization tests.
//
// Each test below pins *current* backend behavior for a known edge case or
// loophole — several are flagged as limitations in the module docstring and in
// DISTRIBUTED_LOCK_PLAN.md. They assert what the code does today, NOT what is
// ideal; a deliberate logic fix should flip the relevant assertion. No backend
// logic is changed by this suite.
// ---------------------------------------------------------------------------

/// CHARACTERIZATION (writer-handoff): a writer blocked solely by a *held* writer
/// enqueues into `:pw` — the acquire-write block condition includes `current_writer`
/// (`rwlock_backend.rs:93`), so writer-preference survives the handoff: a later
/// reader is blocked by the queued writer, and on release the queued writer (not a
/// reader) wins. This pins the *fixed* behavior; flip it if the queue-on-held-writer
/// path is ever removed.
#[tokio::test]
async fn writer_blocked_by_writer_enqueues_and_keeps_preference() {
    let (mut conn, k) =
        conn_and_keys("writer_blocked_by_writer_enqueues_and_keeps_preference").await;

    assert!(write_oneshot(&mut conn, &k, WRITER_A, LONG_TTL).await);
    assert!(
        !write_waiting(&mut conn, &k, WRITER_B, LONG_TTL).await,
        "second writer is blocked by the held writer"
    );
    assert_eq!(
        zcard(&mut conn, &k.pending).await,
        1,
        "a writer blocked by a held writer enqueues, guarding the handoff"
    );
    assert!(
        !acquire_read(&mut conn, &k, READER_A, LONG_TTL).await,
        "a later reader is blocked by the queued writer (writer-preference holds)"
    );

    assert!(release_write(&mut conn, &k, WRITER_A).await, "holder releases");
    assert!(
        write_waiting(&mut conn, &k, WRITER_B, LONG_TTL).await,
        "the queued writer wins the lock on handoff, not a reader"
    );
}

/// LOOPHOLE (mixed ttl on one key): purge thresholds use the *caller's* `ttl_ms`,
/// not a per-member stored ttl, so a reader that took a long lease is purged early
/// by any later op passing a short ttl.
#[tokio::test]
async fn mixed_ttl_purges_long_reader_early() {
    let (mut conn, k) = conn_and_keys("mixed_ttl_purges_long_reader_early").await;

    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    tokio::time::sleep(PAST_SHORT_TTL).await; // past SHORT_TTL, far short of LONG_TTL

    assert!(
        write_oneshot(&mut conn, &k, WRITER_A, SHORT_TTL).await,
        "LOOPHOLE: the long-lease reader is purged by a SHORT_TTL caller's threshold"
    );
}

/// LOOPHOLE (heartbeat lapse): a waiting writer that attempts once then idles past
/// `ttl_ms` has its `:pwh` heartbeat purged (and thus its `:pw` slot), so a writer
/// that arrived *later* is served first — FIFO position is lost.
#[tokio::test]
async fn idle_waiting_writer_loses_its_slot() {
    let (mut conn, k) = conn_and_keys("idle_waiting_writer_loses_its_slot").await;

    assert!(acquire_read(&mut conn, &k, READER_A, SHORT_TTL).await);
    assert!(!write_waiting(&mut conn, &k, WRITER_A, SHORT_TTL).await, "WRITER_A queues first");
    assert_eq!(zcard(&mut conn, &k.pending).await, 1);

    // WRITER_A stops re-attempting; reader + WRITER_A's heartbeat both age out.
    tokio::time::sleep(PAST_SHORT_TTL).await;

    assert!(
        write_waiting(&mut conn, &k, WRITER_B, SHORT_TTL).await,
        "LOOPHOLE: later WRITER_B acquires because idle WRITER_A was purged from the queue"
    );
}

/// EDGE (`ttl_ms <= 0`, no input validation): the backend forwards a non-positive
/// ttl straight to Redis, which treats the two acquire paths *asymmetrically* —
/// `acquire_read`'s `PEXPIRE key 0` deletes the readers key, so the read reports
/// success yet holds nothing; `acquire_write`'s `SET ... PX 0` is rejected as a
/// Redis error.
#[tokio::test]
async fn nonpositive_ttl_read_drops_silently_write_errors() {
    let (mut conn, k) = conn_and_keys("nonpositive_ttl_read_drops_silently_write_errors").await;

    assert!(
        acquire_read(&mut conn, &k, READER_A, 0).await,
        "acquire_read reports success even with ttl_ms = 0"
    );
    assert_eq!(
        zcard(&mut conn, &k.readers).await,
        0,
        "EDGE: PEXPIRE 0 deletes the readers key, so the reader silently holds nothing"
    );

    let write_res = rwlock_backend::acquire_write(&mut conn, opts(&k, WRITER_A, 0), false).await;
    assert!(
        write_res.is_err(),
        "EDGE: acquire_write with ttl_ms = 0 errors (Redis rejects SET ... PX 0)"
    );
}

/// EDGE (`release_read` gating): releasing a non-member, or releasing twice,
/// returns `false`; membership is the only gate.
#[tokio::test]
async fn release_read_nonmember_and_double_release_return_false() {
    let (mut conn, k) = conn_and_keys("release_read_nonmember_and_double_release_return_false").await;

    assert!(
        !rwlock_backend::release_read(&mut conn, &k.readers, READER_A).await.unwrap(),
        "releasing a reader that never acquired returns false"
    );

    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    assert!(rwlock_backend::release_read(&mut conn, &k.readers, READER_A).await.unwrap());
    assert!(
        !rwlock_backend::release_read(&mut conn, &k.readers, READER_A).await.unwrap(),
        "a second release of the same reader returns false"
    );
}

/// EDGE (writer key has no lazy purge): the `:w` key self-heals purely via Redis
/// `PX`. Once it expires, `refresh_write` reports the loss and another writer
/// can acquire.
#[tokio::test]
async fn writer_key_expires_via_px_only() {
    let (mut conn, k) = conn_and_keys("writer_key_expires_via_px_only").await;

    assert!(write_oneshot(&mut conn, &k, WRITER_A, SHORT_TTL).await);
    tokio::time::sleep(PAST_SHORT_TTL).await;

    assert!(!writer_exists(&mut conn, &k.writer).await, "PX expired the writer key");
    assert!(
        !rwlock_backend::refresh_write(&mut conn, &k.writer, WRITER_A, SHORT_TTL)
            .await
            .unwrap(),
        "refresh of an expired writer reports the loss"
    );
    assert!(
        write_oneshot(&mut conn, &k, WRITER_B, SHORT_TTL).await,
        "a different writer acquires once the key has expired"
    );
}

/// EDGE (degenerate owner): an empty-string owner round-trips through
/// acquire / reentrancy / refresh / release, and is a *real* holder (it blocks
/// other writers) — it must not silently alias "no writer".
#[tokio::test]
async fn empty_owner_round_trips() {
    let (mut conn, k) = conn_and_keys("empty_owner_round_trips").await;

    assert!(write_oneshot(&mut conn, &k, "", LONG_TTL).await, "empty owner acquires");
    assert!(
        write_oneshot(&mut conn, &k, "", LONG_TTL).await,
        "empty owner re-acquires its own write (reentrant)"
    );
    assert!(
        !write_oneshot(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "the empty owner is a real holder and blocks another writer"
    );
    assert!(
        rwlock_backend::refresh_write(&mut conn, &k.writer, "", LONG_TTL).await.unwrap(),
        "empty owner refreshes"
    );
    assert!(release_write(&mut conn, &k, "").await, "empty owner releases");
    assert!(!writer_exists(&mut conn, &k.writer).await, "key gone after release");
}

/// LOOPHOLE (same-millisecond tie-break): `:pw` scores are arrival ms, so two
/// writers enqueued in the same ms tie on score and Redis serves the
/// lexicographically smaller member first — not the true earliest arrival. Seed
/// the tie directly to make it deterministic.
#[tokio::test]
async fn same_ms_writer_fifo_is_lexical() {
    let (mut conn, k) = conn_and_keys("same_ms_writer_fifo_is_lexical").await;

    // Identical arrival score for both writers; fresh (far-future) heartbeats so
    // neither is treated as a dead waiter and purged.
    let tied_arrival = 1_000.0;
    let live_heartbeat = 1e15;
    zadd(&mut conn, &k.pending, tied_arrival, WRITER_A).await;
    zadd(&mut conn, &k.pending, tied_arrival, WRITER_B).await;
    zadd(&mut conn, &k.pending_heartbeat, live_heartbeat, WRITER_A).await;
    zadd(&mut conn, &k.pending_heartbeat, live_heartbeat, WRITER_B).await;

    // No readers, no held writer: the queue front decides.
    assert!(
        !write_waiting(&mut conn, &k, WRITER_B, LONG_TTL).await,
        "LOOPHOLE: WRITER_B yields to the lexically smaller member despite the tie"
    );
    assert!(
        write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "the lexically smaller member wins the same-ms tie, not strict arrival order"
    );
}

/// INVARIANT: a write stays withheld while *any* reader remains, and acquires
/// only after the last reader is released (extends `write_waits_for_readers` to
/// multiple concurrent readers).
#[tokio::test]
async fn write_held_until_all_readers_released() {
    let (mut conn, k) = conn_and_keys("write_held_until_all_readers_released").await;

    assert!(acquire_read(&mut conn, &k, READER_A, LONG_TTL).await);
    assert!(acquire_read(&mut conn, &k, READER_B, LONG_TTL).await);

    assert!(!write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await, "two readers block the write");

    assert!(rwlock_backend::release_read(&mut conn, &k.readers, READER_A).await.unwrap());
    assert!(
        !write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "still blocked while one reader remains"
    );

    assert!(rwlock_backend::release_read(&mut conn, &k.readers, READER_B).await.unwrap());
    assert!(
        write_waiting(&mut conn, &k, WRITER_A, LONG_TTL).await,
        "write acquires only once the last reader is gone"
    );
}
