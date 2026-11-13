# Engineering rules (apply to the whole project, not just new code)

This is a Rust port of the Telegram Bot API server. The C++ implementation in
`tdlib/telegram-bot-api` is the behavioural reference: any difference from it
needs a written reason (comment or `CPP_VS_RUST_COMPARISON.md`).

## Principles
- Highest practical performance and efficiency: low CPU, low memory, minimal overhead, especially in hot paths (request routing, JSON relay, update delivery, MTProto up/download).
- Highly modular: one responsibility per module, small well-defined interfaces, no tightly coupled god-files (`client/engine.rs` is the main offender and must be split).
- Clean, idiomatic, production-grade Rust. No unnecessary abstractions, dependencies, allocations, `.clone()`, copies or data transformations. Prefer borrowing / zero-copy (`&str`, `Bytes`, `Cow`, streaming) where practical.
- No redundant computation in hot paths; async work and concurrency designed for efficiency and scalability (no blocking calls on the runtime, no lock held across `.await`, bounded queues and bounded concurrency).
- Explicit, reliable resource management: no memory or resource leaks, no uncontrolled background tasks (every spawned task has an owner, a cancellation path and a bounded lifetime), every map/cache has a bound or eviction.
- Remove dead code; no duplicate implementations; remove unused dependencies.
- Never trade correctness, reliability or security for speed.
- Every optimisation must be justified by a measurement (profile, benchmark, RSS/allocation count, binary size) recorded in the commit message. No speculative "optimisations".

## Process rules
- `cargo build`, `cargo test` and `cargo clippy --all-targets` must be clean at every commit.
- Refactors must be behaviour-preserving: add or keep tests that pin the behaviour before moving code.
- One logical change per commit; do not commit `Cargo.lock` noise.
- Do not claim something is fixed or tested unless it was actually run.
