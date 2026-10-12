# DNS forwarding

Kotoconn can listen for UDP and TCP DNS queries. Each query enters a JavaScript
DNS handler, including queries answered from a service's cache. A handler may
return a response, drop the request, or call a registered upstream service.

See [the DNS example](../packages/api/examples/dns.ts) for a SOCKS5-backed service
and a domain policy. Run the example with a SOCKS5 server at `127.0.0.1:1080`.
The DNS listener binds UDP and TCP at `127.0.0.1:1053`.

```ts
import { kotoconn as k } from "@kotoconn/bindings";
import { dns_service } from "@kotoconn/api";

const service = dns_service({
  upstream: {
    protocol: "tcp",
    server: { address: k.ip("1.1.1.1"), port: 53 },
  },
  dialer,
});

const handler = k.dns_handler(service.handler);
k.dns_listener({
  listen: { address: k.ip("127.0.0.1"), port: 1053 },
  protocols: ["udp", "tcp"],
  dns_handler: handler,
});
```

`dialer` is an existing Kotoconn dialer reference. Every upstream connection uses
that dialer. The upstream protocol is independent of the listener protocols.
UDP upstreams retry truncated responses over TCP through the same dialer, which
must support TCP for that fallback to succeed.
Closed pooled TCP connections are re-established once within the same query
deadline. Timeouts and DNS error responses do not trigger that retry.

Services accept one IP upstream with an explicit port. Resolve proxy server
domains through the dialer's existing resolve handler. Its bootstrap resolution
must not depend on querying the DNS service that needs that proxy connection.

## Policy and responses

`service.handler` is a function that can be registered or called without binding
`this`. `await service.query(request)` returns a native response for wrapping with
`k.respond`. Both share the same native service state.

`request.query` exposes a normalized domain name, record type, and numeric DNS
class. Domain names are lowercase without the final dot, except the root `.`.
Known record types use names such as `A` and `AAAA`; unknown types use numbers.
Listener requests also expose `inbound`, `source`, and the client `protocol`.
These context properties are undefined for requests passed directly to the
embedding API without listener information. Request properties are read-only;
`k.request_bytes` and `k.response_bytes` provide explicit message access.

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
