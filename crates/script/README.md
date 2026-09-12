# Policy bindings

`Script::load(entry, sources)` evaluates TypeScript modules from a map of relative filenames. Imports include the supplied filename's extension. The native module is `@kotoconn/bindings`.

Oxc transpiles; `tsc` checks types. QuickJS retains registered handlers and runs synchronous results or promises that settle on its job queue. Native asynchronous I/O is not installed. Registration ends after module evaluation. Opaque references come only from registration, so carriers can only point to existing resources.

`ts-rs` derives configuration types in `config`. QuickJS adapters keep conversions in both directions so `structural-convert` can check field correspondence. Method declarations and typed callbacks use the same Rust signatures as execution. Native IP and target views also have compile-time method signature checks.

## TypeScript workspace

- `packages/bindings` contains generated declarations and types derived from them. Its package name matches the native module.
- `packages/api` is reserved for handwritten policy helpers and depends on bindings through `workspace:*`.

Run `pnpm run check` at the repository root. It regenerates declarations before compiling the workspace. Generated files and build output are ignored by Git; there is no checked-in copy to compare against.

The binding tests define independent types for nested options, nullable native references, tagged unions, collections, and callbacks. Rust tests exercise conversion and execution; `tsc` checks the generated test declarations and rejected assignments. They do not load application configuration or depend on generated files being present before the tests start.
