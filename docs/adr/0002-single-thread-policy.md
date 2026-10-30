# Run all policy in one JS context on one thread

All policy code runs in one QuickJS context on a dedicated thread, so handlers share ordinary JavaScript state and JS values never cross threads. Native operations run on the daemon's Tokio runtime; awaiting one lets other handlers run. Native callers reach policy through a cloneable handle and bounded queues.

Synchronous work in a handler therefore delays every policy call, including routing for new sessions. Expensive per-flow work belongs in native APIs. The shutdown deadline interrupts running JS, so a synchronous loop cannot prevent shutdown.
