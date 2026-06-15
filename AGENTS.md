# Agent Notes

## Repo Shape
- Single Rust library crate (`distkit`, edition 2024), not a workspace.
- Public module gates live in `src/lib.rs`; crate-level docs come from `docs/lib.md` via `#![doc = include_str!("../docs/lib.md")]`.
- `Cargo.toml` is the source of truth for features: default = `counter` + `instance-aware-counter`; `full` = `counter` + `instance-aware-counter` + `trypema` + `lock`.
- `trypema` is only a re-export of the optional `trypema` crate from `src/trypema.rs`.
- The `lock` feature ships both `Mutex` (`src/lock/mutex.rs`) and `RwLock` (`src/lock/rwlock.rs`) as live behavior, plus auto-refresh and input validation; treat both as fully implemented. Only Stage 6 docs/benches remain from the lock plan.

## Commands
- Full test path: `make test` starts Docker Redis (`redis-test`) on `REDIS_PORT` default `16379`, sets `REDIS_URL=redis://127.0.0.1:16379/`, runs `cargo test --all-features -- --show-output`, then tears Redis down.
- Focused test path: `make redis-up`, then `REDIS_URL=redis://127.0.0.1:16379/ cargo test --all-features <filter> -- --show-output`, then `make redis-down`.
- Doc tests use live Redis too: with local compose Redis, run `REDIS_URL=redis://127.0.0.1:16379/ cargo test --all-features --doc`.
- Benchmarks: `make bench` wraps Redis setup/teardown around `cargo bench --all-features`.
- CI only runs `cargo test --all-features -- --show-output` against Redis on port `6379`; there is no repo rustfmt/clippy config or CI lint job.

## Redis/Test Gotchas
- Unit tests and benches panic if `REDIS_URL` is unset; prefer the `make` targets unless you need a focused filter.
- Test helpers create run-unique Redis prefixes/keys; keep new live-Redis tests using the existing `tests/common.rs` helpers instead of fixed keys.
- `DistkitRedisKey` rejects empty strings, values over 255 bytes, and colons; colons are reserved for internal namespacing like `{prefix}:{type}` and `{namespace}:{key}`.

## Coding Constraints
- The crate has `#![deny(missing_docs)]` and `#![forbid(unsafe_code)]`; every new public item, including feature-gated items, needs rustdoc and no `unsafe`.
- Public API docs usually need updates in `docs/lib.md` as well as module docs because `src/lib.rs` includes that file as the crate root docs.
- Lax counters and lock guards use background Tokio tasks; drops/releases can be asynchronous, so tests often need explicit awaits or short sleeps rather than assuming immediate Redis state.
- `DISTRIBUTED_LOCK_PLAN.md` is useful roadmap context, but current code wins where it conflicts; with Stages 0–6 done, the only remaining future work is the out-of-scope celeris-realtime migration.

## Testing Philosophy
- Tests assert **expected** behavior — what the code *should* do — not the behavior the current implementation happens to exhibit. Never write a test whose assertions are reverse-engineered from the implementation, docs, or other tests.
- A failing test is a signal of a real or potential bug in the **main code**, to be fixed there. It is never a cue to relax the assertion to match the code, and do not use `#[ignore]` to hide an expected-behavior failure.
- When the expected behavior is unclear, ask the behavior's owner (the human) — do not infer it from `DISTRIBUTED_LOCK_PLAN.md`, the Lua/Rust source, or pre-existing tests.
- It is fine (and expected) to commit tests that fail against today's code: they document the target behavior until the main logic is corrected.
