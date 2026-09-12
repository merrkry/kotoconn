import { kotoconn as k } from "@kotoconn/bindings";

const resolve = k.resolve_handler((name) => k.lookup(name));
const dialer = k.dialer({
  dialer: null,
  outbound: { resolve_handler: resolve, implementation: k.direct_outbound({}) },
});

const routing = k.routing_handler(async (flow) => {
  if (flow.protocol === "udp") {
    // This direct carrier needs an IP. UDP policy cannot rewrite the destination.
    return flow.dest.ip ? k.route_udp(dialer) : k.reject();
  }
  if (flow.dest.domain) {
    const addresses = await k.lookup(flow.dest.domain);
    const address = addresses[0];
    return address ? k.route(dialer, k.ip_target(address, flow.dest.port)) : k.reject();
  }
  return k.route(dialer, flow.dest);
});

for (const implementation of [
  k.http_inbound({ listen: { address: k.ip("127.0.0.1"), port: 8080 } }),
  k.socks5_inbound({ listen: { address: k.ip("127.0.0.1"), port: 1080 } }),
  k.shadowsocks2022_inbound({
    listen: { address: k.ip("127.0.0.1"), port: 8388 },
    password: "AAECAwQFBgcICQoLDA0ODw==",
  }),
]) {
  k.inbound({
    routing_handler: routing,
    udp_idle_timeout: k.timeout(60_000),
    implementation,
  });
}
