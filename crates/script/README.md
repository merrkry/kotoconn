# Policy bindings

`Script::load(entry, sources).await` evaluates modules from a map of relative filenames. `load_with_interrupt` also accepts a `ModuleSource` and an interruption callback. Daemon owns filesystem access; this crate normalizes module names and transpiles source with Oxc. Imports include their filename extension. The native module is `@kotoconn/bindings`.

Registration closes after entry-module evaluation, including top-level await. Registered handlers accept synchronous results or promises and share one JS context. Resource references come from registration, so carriers can only reference existing resources. `kotoconn.lookup(name)` runs the system resolver in an owned Tokio task and returns a promise of native IP values.

Poll `Script` only on its owning thread. Its shared-reference methods support concurrent awaits without holding exclusive borrows. Keep `drive()` polled to advance detached promises when no handler is waiting; `idle()` drains pending native futures and JS jobs. Dropping the runtime cancels its native tasks. Daemon owns cross-thread admission and shutdown deadlines.

## Type declarations and tests

`ts-rs` derives configuration declarations. QuickJS adapters use `structural-convert` in both directions to detect mismatched fields. The `api!` macro emits methods and declarations from the same signatures, and `Handler` ties callback execution to its declared input and output. Native IP and target views have compile-time signature checks and runtime tests for their JS properties.

`packages/bindings` contains generated declarations and related types; `packages/api` contains handwritten helpers and a typechecked example. `pnpm run check` generates or restores ignored declarations before compilation.

Binding tests use independent sample types for conversions, callbacks, and asynchronous native calls. Runtime tests cover detached work and cancellation without daemon code. Minimal registrations exercise the public Script entry points and registration lifetime. Compiler tests live in `typescript` and validate emitted JavaScript behavior, including asynchronous control flow and type-only imports.
