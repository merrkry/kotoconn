import { kotoconn as k } from "@kotoconn/bindings";

const resolve = k.resolve_handler((name) => k.lookup(name));
const anytls = k.dialer({
  dialer: null,
  outbound: {
    resolve_handler: resolve,
    implementation: k.anytls_outbound({
      server: k.domain("proxy.example.com", 443),
      password: "replace-with-server-password",
      tls: { server_name: "proxy.example.com" },
      idle_session_timeout: k.timeout(60_000),
    }),
  },
});

const routing = k.routing_handler((flow) =>
  flow.protocol === "udp" ? k.route_udp(anytls) : k.route(anytls, flow.dest),
);

k.inbound({
  implementation: k.socks5_inbound({
    listen: { address: k.ip("127.0.0.1"), port: 1080 },
  }),
  routing_handler: routing,
  udp_idle_timeout: k.timeout(60_000),
});
