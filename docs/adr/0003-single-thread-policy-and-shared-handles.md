# Keep user policy on one thread

All user code runs in one QuickJS context on a dedicated thread. Awaiting a native operation allows other handlers to run on that same thread; native operations run on the daemon's Tokio multi-thread runtime. This preserves shared JavaScript state without making JS values transferable between threads.

The JS thread polls its handler futures directly. They do not need separately spawned tasks or a local runtime; all native task dispatch uses the entered multi-thread runtime.

Native callers use a cloneable `Policy` handle with `&self` methods and exchange owned Rust values through bounded channels. JS-facing native methods also use shared references; registration mutates traced state only for the duration of a method call. No exclusive borrow crosses an await. Configuration is sealed after entry-module evaluation, and protocol runtime state remains separate from configuration values.

Shutdown closes admission, drains accepted calls and pending native promises, then drops the JS context. A deadline on the native runtime cancels remaining work and interrupts running JS, so a synchronous loop cannot prevent shutdown.
