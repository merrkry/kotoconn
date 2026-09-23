# Hysteria 2

`hysteria2_inbound` and `hysteria2_outbound` implement authenticated TCP and UDP
proxying, including UDP fragmentation and optional Salamander obfuscation.
Quinn supplies QUIC and BBR, rustls with ring supplies TLS 1.3, h3 and h3-quinn
supply HTTP/3, and RustCrypto supplies BLAKE2b. None of these dependencies is
forked. The adapter implements Hysteria messages and adapts public I/O traits.

Both directions use Quinn's BBR controller. The client advertises
`Hysteria-CC-RX: 0`; the server returns `Hysteria-CC-RX: auto`. There is no Brutal
controller or bandwidth setting. Negotiation follows the
[Hysteria 2 specification](https://v2.hysteria.network/docs/developers/Protocol/).

## Configuration

An outbound requires a server and password. It uses the public Mozilla CA roots;
`ca_certificate` adds trusted certificates in PEM format. `server_name` defaults
to the server's configured domain or IP, before resolution. Certificate and
server-name verification remain enabled.

```ts
const implementation = k.hysteria2_outbound({
  server: k.domain("proxy.example", 443),
  password: "replace-with-your-password",
  server_name: undefined,
  ca_certificate: undefined,
  obfs_password: undefined,
});
```

An inbound requires a PEM certificate chain and matching PEM private key. The
leaf certificate comes first. Strings can be imported from a policy module.

```ts
const implementation = k.hysteria2_inbound({
  listen: { address: k.ip("0.0.0.0"), port: 443 },
  password: "replace-with-your-password",
  certificate: certificatePem,
  private_key: privateKeyPem,
  obfs_password: undefined,
});
```

Set the same nonempty `obfs_password` at both ends to enable Salamander. The
password used for HTTP/3 authentication is independent of the obfuscation key.
Register these implementations through the usual `k.dialer` and `k.inbound`
configuration. An outbound's carrier must provide UDP even for TCP proxying.
A TCP-only carrier is rejected at construction. Server resolution uses the
configured resolver; user destinations retain their domain/IP metadata.

## Connections and limits

Concurrent TCP streams and UDP associations share one authenticated QUIC
connection per client. Closing either transport entry leaves the other usable.
The last user releases the connection immediately, allowing scope completion
and daemon shutdown without waiting for an idle pool timer. A later request
establishes a new connection. Failed connections are replaced for new requests;
existing TCP streams are never replayed. Neither direction imposes a fixed count
limit on Hysteria UDP associations. The client assigns increasing 32-bit session
IDs and rejects new associations if that connection's ID space is exhausted,
preventing collisions with previous IDs. The server expires idle associations
using the inbound `udp_idle_timeout`, with a minimum retention of one second.

Quinn's endpoint and connection drivers participate in scope cancellation and
completion. Inbound TCP acknowledges admission before routing, consistently with
[the admission contract](adr/0005-independent-protocol-admission-and-session-execution.md).
Listener shutdown stops new admission and drains accepted TCP sessions. UDP
sessions remain separated by association and destination.

Authentication failures and ordinary HTTP/3 requests receive an empty 404
response. HTTP/3 control processing continues after authentication. Unknown or
malformed Hysteria datagrams are discarded. The adapter bounds field lengths,
concurrent streams and reassembly memory. Reassembly holds
at most 64 incomplete packets and 256 KiB of payload per connection. Each
received datagram removes incomplete packets older than ten seconds. UDP
payloads larger than 65,507 bytes are discarded.

The carrier abstraction does not expose a path MTU, so QUIC uses its 1,200-byte
baseline without MTU discovery. Handshake and stream setup have ten-second I/O
deadlines; QUIC uses a thirty-second idle timeout and ten-second keepalives.

This adapter does not provide port hopping, Gecko, ACME, ECH, client certificates,
a reverse-proxy masquerade, or configurable BBR profiles.

## Verification

Rust tests cover TCP half-close and server-first traffic, empty and large UDP
payloads, concurrent associations, nested carriers, certificate and password
rejection, authentication before proxy admission, continued HTTP/3 service,
and cancellation during setup and shutdown.

The `hysteria2` and `hysteria2-salamander` E2E suites test both directions against
the official Hysteria 2.12.3 image pinned by digest. They cover TCP, fragmented
UDP, concurrency, and CLI shutdown. Official Hysteria uses 4 KiB UDP buffers,
so interop uses 2 KiB payloads; Rust tests cover 8 KiB datagrams through the
proxy and 60,000-byte reassembly. The official update check is disabled.

```sh
mise exec -- moon run docker:test -- hysteria2 hysteria2-salamander
```
