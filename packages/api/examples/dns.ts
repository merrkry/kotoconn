import { kotoconn as k } from "@kotoconn/bindings";
import { dns_service } from "@kotoconn/api";

const proxy = await k.outbound({
  via: k.host_network({}),
  resolve_server: (name) => k.lookup(name),
  protocol: k.socks5_outbound({
    server: k.ip_endpoint(k.ip("127.0.0.1"), 1080),
  }),
});

const upstream = await dns_service({
  upstream: { protocol: "tcp", server: { address: k.ip("1.1.1.1"), port: 53 } },
  outbound: proxy,
  cache: { capacity: 4096 },
});

// The service resolves this server name through `proxy`, so its connections never
// depend on themselves.
export const remote = await k.outbound({
  via: k.host_network({}),
  resolve_server: upstream.resolve,
  protocol: k.http_outbound({ server: k.domain("proxy.example.com", 8080) }),
});

await k.dns_listener({
  listen: { address: k.ip("127.0.0.1"), port: 1053 },
  protocols: ["udp", "tcp"],
  answer: async (request) => {
    if (k.domain_suffix(request.query.name, "blocked.example")) {
      return k.drop();
    }

    const response = await upstream.query(request);
    // Drop answers that point at private networks, such as DNS rebinding attempts.
    const rebinding = response.answers.some(
      (record) => (record.type === "A" || record.type === "AAAA") && record.data.is_private(),
    );
    return rebinding ? k.drop() : k.respond(response);
  },
});
