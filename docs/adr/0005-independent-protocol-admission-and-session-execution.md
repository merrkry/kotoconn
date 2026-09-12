# Admit inbound connections independently of outbound establishment

An inbound handshake acknowledges a connection to Kotoconn. It does not wait for routing or outbound availability. A later rejection or connection failure closes the connection. This permits reading inbound payload before selecting an outbound, including sniffing, without a handshake dependency cycle.

TCP crosses adapter boundaries as `Pin<Box<dyn Stream>>`. Concrete streams may be non-Unpin. Client connection setup and forwarding run in owned asynchronous tasks, so progress does not depend on whether the caller first reads or writes. The current runtime uses bounded duplex buffers; this execution strategy is internal, not a requirement that protocols be universally lazy or eager. TCP EOF preserves half-close semantics.

Each session progresses independently of the supervisor. Shared mutable protocol state has an owner task; messages coordinate work without holding locks across waits. CancellationToken scopes interrupt setup, policy waits, capacity waits and I/O. Close requests and completion are separate: completion waits for registered work to release its resources. Dropping a connection closes its scope. A process-wide session registry provides stable close handles without owning stream state.

Daemon shutdown stops listeners and policy admission, drains accepted policy work and TCP sessions, and cancels remaining work at the configured deadline. UDP listener shutdown ends its associations. An accepted handshake that still needs policy after admission closes can fail; shutdown does not promise to finish routing every accepted socket.
