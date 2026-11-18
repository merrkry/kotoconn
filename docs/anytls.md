# AnyTLS

Kotoconn requires an AnyTLS v2 peer for TCP and UDP proxying in either direction.
UDP uses sing-box UDP-over-TCP v2, so TCP support on the outbound carrier is
enough for both TCP and UDP.
Outbound UDP uses connected mode because each Kotoconn UDP session has a fixed
destination. Inbound UDP also accepts datagram mode, including IPv4, IPv6, domain
names, and empty payloads.

## Configuration

See the [TypeScript outbound example](../packages/api/examples/anytls.ts). For an
inbound, use `k.anytls_inbound({ listen, password, tls: { certificate, private_key } })`.

Client `tls.server_name` defaults to the configured server domain or IP address.
The client omits SNI for IP addresses and always verifies the certificate and
server name. Client `tls.certificate` adds PEM trust anchors to Mozilla's public
roots. Server `tls.certificate` and `tls.private_key` contain a PEM chain and key.
To rotate the certificate, create a new inbound.

The server's optional `padding_scheme` overrides the default AnyTLS padding.
Server padding updates apply to later connections from the same configured client.

## Connections and limits

A client reuses idle TLS sessions and uses separate sessions for concurrent
requests. Set `idle_session_timeout` to a positive duration to override the
60-second default. Closing the carrier ends all AnyTLS sessions that depend on it.
During daemon shutdown, clients release idle sessions and stop pooling
connections. Inbounds close idle sessions and let admitted TCP streams finish.

An inbound acknowledgement confirms admission before Kotoconn routes the stream
or connects to the destination. See [ADR 0006](adr/0006-independent-inbound-admission.md).
AnyTLS FIN closes both directions of a stream, so TCP half-close is unavailable.

TLS, authentication, and stream setup have 15-second network deadlines. Stalled
TLS writes time out after 30 seconds. The inbound allows up to 128 logical streams
per TLS session. The streams share a 4 MiB receive budget that includes message
overhead. If an incoming message exceeds the remaining budget, the session closes
the receiving stream. HTTP fallback and TLS fingerprint impersonation are unavailable.

## Verification

To check interoperability with the official Go implementation in both directions,
run the AnyTLS scenarios from the repository root.

```sh
mise exec -- moon run docker:test -- anytls
```

See the [AnyTLS protocol](https://github.com/anytls/anytls-go/blob/main/docs/protocol.md)
and [sing-box UDP-over-TCP specification](https://sing-box.sagernet.org/configuration/shared/udp-over-tcp/).
