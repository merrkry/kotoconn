# Compose dialers from carriers and self-contained protocol adapters

A dialer pairs an outbound with an optional carrier dialer. Without a carrier, it reaches the host network directly.

Each protocol is one adapter that wraps an established library in both directions. Cryptography, authentication and replay protection come from that library. Protocol-native state, multiplexing and session reuse stay inside the adapter. The core never converts protocols, adds multiplexing, or synthesizes features a protocol lacks, such as UDP over a TCP-only protocol.

A carrier declares its capabilities: TCP, UDP, and whether it accepts domain targets. Each adapter checks what it needs before starting I/O. For example, SOCKS5 UDP needs TCP and UDP from the same carrier. Unsupported requests fail immediately.

Carrier chains define cancellation. Closing a client ends the connections that depend on it, including those of upper adapters; sibling clients stay usable unless a shared resource fails. Adapters handle the protocol consequences, such as ending a SOCKS UDP association when its control stream closes. Protocol association state, such as replay windows, outlives individual sessions.
