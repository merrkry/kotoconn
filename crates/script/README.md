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

## Type declarations and tests

`ts-rs` derives configuration declarations. QuickJS adapters use `structural-convert` in both directions to detect mismatched fields. The `api!` macro emits methods and declarations from the same signatures, and `Handler` ties callback execution to its declared input and output. Native IP and target views have compile-time signature checks and runtime tests for their JS properties.

`packages/bindings` contains generated declarations and related types; `packages/api` contains handwritten helpers and a typechecked example. `pnpm run check` regenerates ignored declarations before compilation.

Binding tests use independent sample types for conversions, callbacks, and asynchronous native calls. Runtime tests cover detached work and cancellation without daemon code. Minimal registrations exercise the public Script entry points and registration lifetime. Compiler tests live in `typescript` and validate emitted JavaScript behavior, including asynchronous control flow and type-only imports.
