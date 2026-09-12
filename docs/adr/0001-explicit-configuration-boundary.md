# Keep Rust configuration explicit

The config crate defines values and resource references exchanged with user policy. Routing handlers, resolve handlers, and UDP idle timeouts must be explicit; TypeScript constructors may supply defaults, but Rust must not resolve missing fields through global defaults. An absent carrier dialer means delivery to the I/O layer.

Address resolution returns IP values for user code to select; serving DNS returns a Hickory response or `Drop`. IP values retain their address family without prescribing a preference. Cache state, protocol capabilities, and I/O handling belong to native implementations.

A single routing handler receives a `Flow` with a TCP/UDP tag and client destination. UDP routing decisions are reused per client session and destination, while native code owns session identity, packet handling, and carrier sharing.
