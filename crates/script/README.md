# Policy bindings

`Script::load(entry, sources).await` evaluates modules from a map of relative filenames. `load_with_interrupt` also accepts a `ModuleSource` and an interruption callback. Daemon owns filesystem access; this crate normalizes module names and transpiles source with Oxc. Imports include their filename extension. The native module is `@kotoconn/bindings`.

Registration closes after entry-module evaluation, including top-level await. Registered handlers accept synchronous results or promises and share one JS context. Resource references come from registration, so carriers can only reference existing resources. `kotoconn.lookup(name)` runs the system resolver in an owned Tokio task and returns a promise of native IP values.

Poll `Script` only on its owning thread. Its shared-reference methods support concurrent awaits without holding exclusive borrows. Keep `drive()` polled to advance detached promises when no handler is waiting; `idle()` drains pending native futures and JS jobs. Dropping the runtime cancels its native tasks. Daemon owns cross-thread admission and shutdown deadlines.

`kotoconn.cidr(prefix).contains(address)` matches native IP addresses against IPv4
or IPv6 CIDRs. Address families stay distinct, including IPv4-mapped IPv6 addresses.
IP values provide `is_private()`, `is_loopback()`, `is_link_local()`, `is_multicast()`
and `is_unspecified()`. Private means IPv4 RFC 1918 ranges or IPv6 unique-local addresses.
Invalid CIDRs raise an error.

`kotoconn.domain_suffix(name, suffix)` matches a domain itself and its subdomains.
`kotoconn.is_subdomain(name, parent)` matches only subdomains. Both accept plain DNS
names, compare labels without regard to case and ignore an optional trailing root
dot. A leading dot, an empty name or a malformed name raises an error. Exact,
keyword and regex matching use JavaScript's existing string and regex methods.

## Logging, timers, subprocesses and shutdown hooks

`kotoconn.log(level, message)` emits a daemon log event at `error`, `warn`, `info`, `debug` or `trace` with the `policy` target, inside the span of the policy call that logs it.

`kotoconn.sleep(timeout)` resolves after the timeout. Once shutdown starts, pending and later sleeps reject, so scheduled work such as a refresh loop cannot hold shutdown open. Catch the rejection to end the loop quietly.

`kotoconn.exec(argv, { timeout?, input? })` runs `argv[0]` with the remaining arguments, without a shell. The program inherits the daemon's environment, working directory and privileges; policy is trusted, so this is a general capability. `input` is written to stdin, which is otherwise empty. The promise resolves to `{ status, signal, stdout, stderr }` with output decoded as lossy UTF-8. `status` is the exit code, or `undefined` when a signal ended the program; a nonzero status is a result, not a rejection. An empty `argv`, a spawn failure or the timeout rejects. On timeout, or when the waiting call is cancelled, the child is killed; its own children are not.

`kotoconn.on_shutdown(handler)` registers a shutdown hook at any time, unlike resource registration. `Script::shutdown()` rejects sleeps, then runs the hooks one at a time, newest first, awaiting each result. A thrown error or rejection is logged and the next hook still runs. Hooks may call native operations such as `exec`, and a hook registered while hooks run also runs. Registration fails once all hooks have run.

## Type declarations and tests

`ts-rs` derives configuration declarations. QuickJS adapters use `structural-convert` in both directions to detect mismatched fields. The `api!` macro emits methods and declarations from the same signatures, and `Handler` ties callback execution to its declared input and output. Native IP and target views have compile-time signature checks and runtime tests for their JS properties.

`packages/bindings` contains generated declarations and related types; `packages/api` contains handwritten helpers and a typechecked example. `pnpm run check` regenerates ignored declarations before compilation.

Binding tests use independent sample types for conversions, callbacks, and asynchronous native calls. Runtime tests cover detached work and cancellation without daemon code. Minimal registrations exercise the public Script entry points and registration lifetime. Compiler tests live in `typescript` and validate emitted JavaScript behavior, including asynchronous control flow and type-only imports.
