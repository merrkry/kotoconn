# AGENTS.md

Kotoconn is a programmable proxy.

## Correctness

Time limits are allowed only when interacting with external systems, such as subprocesses, OS I/O, or user code. Fixed delays or timeout lengths must not express ordering dependencies. Use explicit synchronization or observable completion instead, so code and tests remain correct under parallel execution and varying scheduling speeds.

## Dependencies

Don't make re-inventing the wheel as primary consideration. Import well-maintained dependencies for:

- reducing boilerplate code
- complex features: cross-platform support, cross-language interop, network protocols
- performce- or security-critical logic

## Documentation

See `docs/adr` for design decisions.

Documentation must remain consistent with the implementation. In cases of ambiguous or conflicting semantics, you must explicitly confirm with the user whether to modify the documentation or adjust the implementation.

## Version control

Use scoped commits by crate name or aspect, e.g. `config: split outbound mod` `ci: allow workflow_dispatch`.

## Verification

Use static checks; start a dev server or preview only when explicitly asked.

- TypeScript: run `pnpm run format` and `pnpm run check` before finishing.
- Rust: run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace` separately. Keep Rust checks out of `package.json`.
- Generated binding declarations are ignored by Git. `pnpm run check` regenerates them before compilation; use `pnpm run bindings` to refresh editor types.
