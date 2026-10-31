# Keep targets unchanged and resolve names explicitly

The destination is the endpoint the client requested. TCP routing may select a different target. UDP routing keeps the original destination, so the runtime needs no UDP address rewriting or reverse mapping; reply addresses on the wire are each adapter's responsibility.

No layer resolves a domain implicitly. A target passes unchanged down a carrier chain while carriers accept domain targets, and a carrier that requires IP targets rejects a domain. Policy decides whether to resolve a destination before routing.

An outbound's resolve handler resolves only that outbound's own server name, and only when its carrier requires IP targets. Otherwise the server name passes to the carrier, for example to be resolved by an upstream proxy.

A resolver maps a name to IP addresses and keeps their address families without preferring one. How a connection attempt uses several addresses belongs to the dialer, not to the resolver interface.
