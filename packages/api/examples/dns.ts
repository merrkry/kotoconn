import { kotoconn as k } from "@kotoconn/bindings";
import { dns_service } from "@kotoconn/api";

const proxy = k.dialer({
  dialer: null,
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

const policy = k.dns_handler((request) => {
  if (k.domain_suffix(request.query.name, "blocked.example")) {
    return k.drop();
  }
  return upstream.handler(request);
});

k.dns_listener({
  listen: { address: k.ip("127.0.0.1"), port: 1053 },
  protocols: ["udp", "tcp"],
  dns_handler: policy,
});
