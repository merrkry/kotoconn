import { kotoconn as k } from "@kotoconn/bindings";

const direct = await k.dialer({
  carrier: k.host_network({}),
  outbound: { resolve_handler: (name) => k.lookup(name), implementation: k.direct_outbound({}) },
});

await k.inbound({
  implementation: k.tun_inbound({
    name: "kototun0",
    mtu: 1500,
    addresses: [
      { address: k.ip("172.19.0.1"), prefix: 30 },
      { address: k.ip("fd00:19::1"), prefix: 126 },
    ],
  }),
  routing_handler: (flow) =>
    flow.protocol === "udp" ? k.route_udp(direct) : k.route(direct, flow.dest),
  udp_idle_timeout: k.timeout(60_000),
});
