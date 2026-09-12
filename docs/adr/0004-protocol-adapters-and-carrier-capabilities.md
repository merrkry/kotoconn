# Keep protocol adapters separate from runtime connections

Each protocol has one adapter crate wrapping its library in both directions. `kotoconn-protocol` defines shared I/O and lifecycle contracts without protocol-library dependencies. `kotoconn-inbounds` and `kotoconn-outbounds` construct and re-export adapters. Configuration remains in `kotoconn-config`, separate from executable protocol state.

A dialer has one carrier reference. At runtime it exposes independently closable TCP and UDP clients backed by a shared protocol instance. A carrier exposes its available transport capabilities; each adapter checks the capabilities it needs. SOCKS5 UDP needs TCP control and UDP data through that same carrier. A TCP-only carrier can support SOCKS5 TCP but cannot support SOCKS5 UDP. Unsupported requests fail before starting I/O. The core does not add protocol conversion, mux, or an alternate data path. Protocol-native shared state and any mux implementation belong inside the adapter.

Carrier ancestry supplies cancellation scopes. Closing a transport client ends that entry's connections, including connections used by upper adapters. The adapter handles protocol consequences such as ending a SOCKS UDP association when its control stream closes. Sibling transport clients remain usable unless a shared resource actually fails.
