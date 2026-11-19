import { kotoconn as k } from "@kotoconn/bindings";
import { auto_route, dns_service } from "@kotoconn/api";

// The mark lets the daemon's own sockets bypass the TUN routes.
const mark = 0x2022;
const direct = await k.outbound({
  via: k.host_network({ mark }),
  resolve_server: (name) => k.lookup(name),
  protocol: k.direct_outbound({}),
});
const dns = await dns_service({
  upstream: { protocol: "udp", server: { address: k.ip("1.1.1.1"), port: 53 } },
  outbound: direct,
});

const tun = await k.inbound({
  implementation: k.tun_inbound({
    name: "kototun0",
    mtu: 1500,
    addresses: [
      { address: k.ip("172.19.0.1"), prefix: 30 },
      { address: k.ip("fd00:19::1"), prefix: 126 },
    ],
  }),
  // DNS captured by the TUN reaches DNS policy instead of being forwarded as is.
  route: (flow) => {
    if (flow.destination.port === 53) return k.route_dns(dns.handler);
    return flow.protocol === "udp" ? k.route_udp(direct) : k.route(direct, flow.destination);
  },
  udp_idle_timeout: k.timeout(60_000),
});
await auto_route({ interface: "kototun0", inbound: tun, mark });
