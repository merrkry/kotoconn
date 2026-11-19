import { kotoconn as k, type RoutePolicy } from "@kotoconn/bindings";

const outbound = await k.outbound({
  via: k.host_network({}),
  resolve_server: (name) => k.lookup(name),
  protocol: k.direct_outbound({}),
});

const routing: RoutePolicy = async (flow) => {
  if (flow.protocol === "udp") {
    // This direct carrier needs an IP. UDP policy cannot rewrite the destination.
    return flow.destination.ip ? k.route_udp(outbound) : k.reject();
  }
  const domain = flow.sniff?.domain ?? flow.destination.domain;
  if (domain) {
    const addresses = await k.lookup(domain);
    const address = addresses[0];
    return address ? k.route(outbound, k.ip_endpoint(address, flow.destination.port)) : k.reject();
  }
  return k.route(outbound, flow.destination);
};

for (const implementation of [
  k.http_inbound({ listen: { address: k.ip("127.0.0.1"), port: 8080 } }),
  k.socks5_inbound({ listen: { address: k.ip("127.0.0.1"), port: 1080 } }),
  k.shadowsocks2022_inbound({
    listen: { address: k.ip("127.0.0.1"), port: 8388 },
    password: "AAECAwQFBgcICQoLDA0ODw==",
  }),
]) {
  await k.inbound({
    route: routing,
    sniff: { timeout: k.timeout(300) },
    udp_idle_timeout: k.timeout(60_000),
    implementation,
  });
}
