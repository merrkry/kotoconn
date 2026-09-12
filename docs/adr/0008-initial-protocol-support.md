# Define the initial protocol adapter scope

The initial adapters implement HTTP/1 CONNECT, SOCKS5 CONNECT and UDP ASSOCIATE without authentication, and single-user Shadowsocks `2022-blake3-aes-128-gcm` over TCP and UDP. HTTP has no UDP capability. Other HTTP proxy modes, SOCKS BIND, authentication options, additional ciphers and protocol-specific mux extensions require explicit adapter support; the core does not synthesize them.

SOCKS UDP framing and association lifetime follow RFC 1928. This adapter does not implement SOCKS fragmentation and discards nonzero FRAG. It requires the TCP peer's source IP and fixes an unspecified client port from the first valid datagram. All destinations within that association are then split into runtime sessions.

Shadowsocks encryption, authentication, timestamp validation and TCP replay detection use shadowsocks-rust. UDP packet windows use its shadowsocks-service implementation. An authenticated UDP client session can update its source address. Protocol association and replay state survive short application-session expiry for at least the timestamp acceptance window; they are separate from Kotoconn sessions. Random session identifiers and checked packet counters prevent nonce reuse during an association.

TCP client setup uses the library's CryptoStream with explicit nonempty request padding. The high-level empty-write helper can randomly choose zero padding, which is invalid when the first chunk contains no application payload. The adapter also checks the response's echoed request salt before returning plaintext. Server-first operation must not depend on retrying this random failure.
