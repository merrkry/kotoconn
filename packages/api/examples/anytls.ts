import { kotoconn as k } from "@kotoconn/bindings";

const anytls = await k.outbound({
  via: k.host_network({}),
  resolve_server: (name) => k.lookup(name),
  protocol: k.anytls_outbound({
    server: k.domain("proxy.example.com", 443),
    password: "replace-with-server-password",
    tls: { server_name: "proxy.example.com" },
    idle_session_timeout: k.timeout(60_000),
  }),
});

await k.inbound({
  implementation: k.socks5_inbound({
    listen: { address: k.ip("127.0.0.1"), port: 1080 },
  }),
  route: (flow) =>
    flow.protocol === "udp" ? k.route_udp(anytls) : k.route(anytls, flow.destination),
  udp_idle_timeout: k.timeout(60_000),
});
