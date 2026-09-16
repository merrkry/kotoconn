# AnyTLS

The `kotoconn-anytls` adapter implements AnyTLS v2 inbound and outbound TCP, with
sing-box UDP-over-TCP v2 for UDP. Outbound UDP uses connected mode, matching
Kotoconn's destination-specific sessions. Inbound UDP also accepts datagram mode,
including IPv4, IPv6, domains and empty payloads. Only a TCP carrier is required.

`anytls` supplies frame encoding/decoding, command validation, settings negotiation
and padding schedule generation. The adapter drives that core with Kotoconn I/O
and cancellation scopes. TLS uses `rustls` with `ring` through `tokio-rustls`;
password hashing uses RustCrypto `sha2`, and comparison uses `subtle`. SOCKS address
encoding and parsing use `fast-socks5`. UoT framing maps its datagram address types
to that codec. No dependency is forked.

The `anytls` crate's bundled runtime is not used. Kotoconn must own task completion
and carrier selection, preserve padding updates per configured client, and honor
AnyTLS FIN's full-close semantics. Its bundled UoT datagram codec also uses SOCKS
family values rather than UoT's distinct family values. Tests cover the latter
with independent wire vectors.

A client reuses the newest idle TLS session before connecting again. Concurrent
requests open separate TLS sessions. Each session assigns increasing stream IDs.
The default idle timeout is 60 seconds; `idle_session_timeout` can override it.
Sessions never share state across configured clients. Dropping a logical stream
releases its session; closing the carrier cancels all dependent sessions.

TLS options are separate from protocol options. Client `tls.server_name` defaults
to the configured server domain or IP; IP names omit SNI. Certificates and names
are always verified. `tls.certificate` adds PEM trust anchors to Mozilla's public
roots. Server `tls.certificate` and `tls.private_key` contain a PEM chain and key.
The server's optional `padding_scheme` overrides the default AnyTLS padding.
Certificate rotation requires constructing a new inbound.

Inbound acknowledgement admits the stream to Kotoconn before routing, as specified
by ADR 0005. FIN closes both directions; it cannot represent TCP half-close.
TLS/authentication/target setup has a 15-second network deadline, and stalled TLS
writes have a 30-second deadline. The inbound allows at most 128 logical streams
per TLS session. A stream that fills its bounded receive queue is closed, leaving
other streams usable. The adapter rejects v1 clients, matching the dependency's
minimum supported protocol version. HTTP fallback and TLS fingerprint impersonation
are not implemented.

References: [AnyTLS protocol](https://github.com/anytls/anytls-go/blob/main/docs/protocol.md),
[sing-box UoT](https://sing-box.sagernet.org/configuration/shared/udp-over-tcp/),
[anytls crate](https://docs.rs/anytls/0.3.17/anytls/).
