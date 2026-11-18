# AnyTLS

Kotoconn implements AnyTLS v2 inbound and outbound TCP, with
sing-box UDP-over-TCP v2 for UDP. Outbound UDP uses connected mode, matching
Kotoconn's destination-specific sessions. Inbound UDP also accepts datagram mode,
including IPv4, IPv6, domains and empty payloads. Only a TCP carrier is required.

A client reuses the TLS session that most recently became idle before connecting
again. Concurrent requests open separate TLS sessions.
The default idle timeout is 60 seconds; `idle_session_timeout` can override it.
Sessions never share state across configured clients. Closing the carrier cancels
all dependent sessions.
During daemon shutdown, clients release idle sessions and stop pooling connections;
inbounds close idle sessions and let admitted streams finish.

TLS options are separate from protocol options. Client `tls.server_name` defaults
to the configured server domain or IP; IP names omit SNI. Certificates and names
are always verified. `tls.certificate` adds PEM trust anchors to Mozilla's public
roots. Server `tls.certificate` and `tls.private_key` contain a PEM chain and key.
The server's optional `padding_scheme` overrides the default AnyTLS padding.
Certificate rotation requires constructing a new inbound.

Inbound acknowledgement admits the stream to Kotoconn before routing, as specified
by [ADR 0006](adr/0006-independent-inbound-admission.md). FIN closes both directions; it cannot represent TCP half-close.
TLS/authentication/target setup has a 15-second network deadline, and stalled TLS
writes have a 30-second deadline. The inbound allows at most 128 logical streams
per TLS session, with a shared 4 MiB receive budget including message overhead.
A stream that exhausts receive capacity is closed. The adapter requires v2 peers
and bounds the wait for SYNACK by the handshake deadline. HTTP fallback and TLS
fingerprint impersonation are not implemented.

See [the TypeScript outbound example](../packages/api/examples/anytls.ts). An inbound
uses `k.anytls_inbound({ listen, password, tls: { certificate, private_key } })`.

## Verification

`moon run rust:test` includes adapter and daemon tests. The default e2e suite also
tests the adapter against the unmodified official Go session library pinned in
`e2e/anytls/reference/go.mod`, with sing's UoT codec. These tests cover both
directions, session reuse, padding updates across TLS connections, SYNACK failure,
server-first TCP, and connected/datagram UDP with empty and large payloads.

To run just the AnyTLS scenarios from the repository root:

```sh
mise exec -- moon run docker:test -- anytls
```

The tests run in an isolated container with no external network access.
Each direction's output is saved in its e2e artifact directory.

References: [AnyTLS protocol](https://github.com/anytls/anytls-go/blob/main/docs/protocol.md),
[sing-box UoT](https://sing-box.sagernet.org/configuration/shared/udp-over-tcp/).
