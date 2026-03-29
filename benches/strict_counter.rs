// Criterion benchmarks for `StrictCounter`.
//
// Every method call goes through a Redis Lua script executed via a hash
// (HINCRBY / HGET / HSET / HDEL / DEL). Latency is dominated by the
// network round-trip to Redis.
//
// One counter instance is shared across all bench functions; each function
// uses a distinct member key so there is no cross-bench state interference.
// Destructive operations (del, clear) use `iter_batched` to re-seed the key
// before every measured call.

mod common;

use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use distkit::counter::CounterTrait;
use tokio::runtime::Runtime;

fn bench_strict_counter(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build Tokio runtime");
    let counter = rt.block_on(common::make_strict_counter("strict"));

    let mut group = c.benchmark_group("strict_counter");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    // inc
    let k = common::key("inc");
    group.bench_function("inc", |b| {
        b.to_async(&rt).iter(|| counter.inc(&k, 1));
    });

    // dec
    let k = common::key("dec");
    group.bench_function("dec", |b| {
        b.to_async(&rt).iter(|| counter.dec(&k, 1));
    });

    // get
    let k = common::key("get");
    group.bench_function("get", |b| {
        b.to_async(&rt).iter(|| counter.get(&k));
    });

    // set
    let k = common::key("set");
    group.bench_function("set", |b| {
        b.to_async(&rt).iter(|| counter.set(&k, 42));
    });

    // del — destructive; re-seed the key before each measured call
    let k = common::key("del");
    group.bench_function("del", |b| {
        b.to_async(&rt).iter_batched(
            || counter.set(&k, 1),
            |_| counter.del(&k),
            BatchSize::SmallInput,
        );
    });

    // clear — destructive; re-seed a key before each measured call
    let k = common::key("clear");
    group.bench_function("clear", |b| {
        b.to_async(&rt).iter_batched(
            || counter.set(&k, 1),
            |_| counter.clear(),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_strict_counter);
criterion_main!(benches);
