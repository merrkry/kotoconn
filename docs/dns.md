# DNS forwarding

Kotoconn can listen for UDP and TCP DNS queries. Each query enters a JavaScript
DNS handler, including queries answered from a service's cache. A handler may
return a response, drop the request, or call an upstream service.
Resolve handlers can also resolve names through a service.

See [the DNS example](../packages/api/examples/dns.ts) for a SOCKS5-backed service,
a domain policy, an answer check, and a dialer whose server name the service
resolves. Run the example with a SOCKS5 server at `127.0.0.1:1080`.
The DNS listener binds UDP and TCP at `127.0.0.1:1053`.

```ts
import { kotoconn as k } from "@kotoconn/bindings";
import { dns_service } from "@kotoconn/api";

const service = await dns_service({
  upstream: {
    protocol: "tcp",
    server: { address: k.ip("1.1.1.1"), port: 53 },
  },
  dialer,
});

await k.dns_listener({
  listen: { address: k.ip("127.0.0.1"), port: 1053 },
  protocols: ["udp", "tcp"],
  dns_handler: service.handler,
});
```

The listener is an inbound: while it drains, it reads no new queries and still
answers the ones it has read.

`dialer` is an open Kotoconn dialer reference. Every upstream connection uses
that dialer, so the dialer cannot close before the service. The service is a
resource with `close()`, `drain({ timeout })` and `wait()`; a drain lets queries
in flight finish. The upstream protocol is independent of the listener protocols.
UDP upstreams retry truncated responses over TCP through the same dialer. When
the dialer cannot carry TCP to the upstream, the truncated response is returned
as is, so the client can retry over TCP itself.
Closed pooled TCP connections are re-established once within the same query
deadline. Timeouts and DNS error responses do not trigger that retry.

Services accept one IP upstream with an explicit port. A domain server in the
service's dialer chain is resolved by the resolve handler of the dialer that
names it. Such a handler must not depend on the same service, which would need
its own connection to answer.

## Resolving names

`await service.resolve(name)` queries A and AAAA records through the service, so
its cache and forwarding apply, and returns the addresses of both answers that
belong to the name or to an alias its CNAME chain reaches.
NXDOMAIN and answers without addresses give an empty list. When one query fails
with another response code or an error while the other returns addresses, the
addresses are returned and the failure is logged as a warning; otherwise the
failure rejects. Like `service.handler`, it needs no `this`, so it serves directly as a
resolve handler:

```ts
const remote = await k.dialer({
  carrier: k.host_network({}),
  outbound: {
    resolve_handler: service.resolve,
    implementation: k.http_outbound({ server: k.domain("proxy.example.com", 8080) }),
  },
});
```

## Policy and responses

`service.handler` is a function that can serve as a DNS handler or be called
without binding `this`. `await service.query(request)` returns a native response for wrapping with
`k.respond`. Both share the same native service state.

`request.query` exposes a normalized domain name, record type, and numeric DNS
class. Domain names are lowercase without the final dot, except the root `.`.
Known record types use names such as `A` and `AAAA`; unknown types use numbers.
Listener requests also expose `inbound`, `source`, and the client `protocol`.
These context properties are undefined for requests built by policy or passed
directly to the embedding API without listener information.

`k.dns_request(name, type)` builds a recursive IN-class query with one question.
The type is a record type name or number, as in `request.query`.

`response.code` is the response code: `NOERROR`, `NXDOMAIN`, `SERVFAIL` and the
other RFC 1035 and RFC 2136 mnemonics, or a number. `response.answers` lists the
answer records with a normalized `name`, `type`, numeric `class`, `ttl` and
`data`. The data of A and AAAA records is an IP value; CNAME, NS, PTR and ANAME
records carry a normalized domain name; other types carry their RDATA as a
`Uint8Array` with uncompressed names. Request and response properties are read-only;
`k.request_bytes` and `k.response_bytes` return the wire message as a
`Uint8Array`, and `k.dns_response` accepts one, for example to read or build the
authority and additional sections.

Forwarding preserves the query's DNS flags and EDNS information. The listener
restores the client request ID and question and respects the client's UDP size
limit. Ordinary single-question queries are supported. Zone transfers, updates,
and authenticated DNS messages are unsupported.

`k.drop()` sends no response. Handler errors and failed upstream operations are
logged and answered with SERVFAIL. Upstream DNS responses such as NXDOMAIN remain
responses. The service's total upstream deadline defaults to 5000 milliseconds;
set `timeout: k.timeout(milliseconds)` to change it.

## Cache

Calling `dns_service` opts into its assembled forwarding behavior. Each service
defaults to a native TTL cache of 4096 responses. Use `cache: false` to disable
it, or `cache: { capacity: 1024 }` to set a positive response capacity.

Positive answers expire according to record TTLs. Negative answers require an
SOA and expire according to its negative TTL. Cached replies age their record
TTLs and use the current caller's request ID. Query flags and EDNS settings are
part of the cache key. Requests or responses containing client-specific EDNS
options bypass caching. Truncated responses and zero-TTL responses are not
cached. Separate services never share caches.

The listener has no response cache or short cache before policy execution.
Custom cache and eviction wrappers are separate from this service API.
