# AGENTS.md

Kotoconn is a programmable proxy.

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
