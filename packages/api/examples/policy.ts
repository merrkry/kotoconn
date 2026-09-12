import { kotoconn as k } from "@kotoconn/bindings";

const resolve = k.resolve_handler((name) => k.lookup(name));
const outbound = k.direct_outbound({});
const dialer = k.dialer({
  dialer: null,
  outbound: { resolve_handler: resolve, implementation: outbound },
});

const routing = k.routing_handler((flow) => k.route(dialer, flow.dest));

k.inbound({
  routing_handler: routing,
  udp_idle_timeout: k.timeout(60_000),
  implementation: k.direct_inbound({
    listen: { address: k.ip("127.0.0.1"), port: 8080 },
    target: k.domain("example.com", 80),
  }),
});
