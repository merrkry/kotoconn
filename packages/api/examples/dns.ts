import { kotoconn as k } from "@kotoconn/bindings";
import { dns_service } from "@kotoconn/api";

const proxy = k.dialer({
  carrier: k.host_network({}),
  outbound: {
    resolve_handler: k.resolve_handler((name) => k.lookup(name)),
    implementation: k.socks5_outbound({
      server: k.ip_target(k.ip("127.0.0.1"), 1080),
    }),
  },
});

const upstream = dns_service({
  upstream: { protocol: "tcp", server: { address: k.ip("1.1.1.1"), port: 53 } },
  dialer: proxy,
  cache: { capacity: 4096 },
});

// The service resolves this server name through `proxy`, so its connections never
// depend on themselves.
export const remote = k.dialer({
  dialer: null,
  outbound: {
    resolve_handler: k.resolve_handler(upstream.resolve),
    implementation: k.http_outbound({ server: k.domain("proxy.example.com", 8080) }),
  },
});

const policy = k.dns_handler(async (request) => {
  if (k.domain_suffix(request.query.name, "blocked.example")) {
    return k.drop();
  }

  const response = await upstream.query(request);
  // Drop answers that point at private networks, such as DNS rebinding attempts.
  const rebinding = response.answers.some(
    (record) => (record.type === "A" || record.type === "AAAA") && record.data.is_private(),
  );
  return rebinding ? k.drop() : k.respond(response);
});

k.dns_listener({
  listen: { address: k.ip("127.0.0.1"), port: 1053 },
  protocols: ["udp", "tcp"],
  dns_handler: policy,
});
