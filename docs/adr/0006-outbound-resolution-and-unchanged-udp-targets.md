# Resolve outbound server names without rewriting user targets

An outbound's configured resolve handler resolves only that outbound's own server name, before asking its carrier for a connection. Targets returned by user policy pass through protocol layers unchanged. The I/O carrier accepts IP targets only and rejects domains rather than performing implicit DNS resolution. The initial endpoint resolver uses the first returned address and reports an empty result as an error; address racing is not part of this implementation.

User policy decides whether to resolve a requested destination. TCP routing can return a replacement Target. UDP routing uses `route_udp(dialer)` and cannot return a Target; the policy boundary rejects a TCP decision for UDP even when written in untyped JavaScript. Consequently the runtime needs no UDP destination-rewrite or reverse-mapping mechanism. Wire-level reply addresses remain the protocol adapter's responsibility.

The existing `lookup` function uses system DNS as a placeholder. Routing resolver traffic is a separate DNS/resolver design and does not introduce a bypass path into the carrier graph.
