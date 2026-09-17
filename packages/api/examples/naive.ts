import { kotoconn as k } from "@kotoconn/bindings";

const resolver = k.resolve_handler((name) => k.lookup(name));
const naive = k.dialer({
  dialer: null,
  outbound: {
    resolve_handler: resolver,
    implementation: k.naive_outbound({
      server: k.domain("proxy.example.com", 443),
      username: "user",
      password: "replace-with-your-password",
    }),
  },
});
const routing = k.routing_handler((flow) =>
  flow.protocol === "tcp" ? k.route(naive, flow.dest) : k.reject(),
);

k.inbound({
  implementation: k.socks5_inbound({
    listen: { address: k.ip("127.0.0.1"), port: 1080 },
  }),
  routing_handler: routing,
  udp_idle_timeout: k.timeout(30_000),
});

// For a server policy, supply PEM strings and register this implementation with
// an inbound whose routing handler selects your desired outbound.
export function naiveServer(certificate: string, privateKey: string) {
  return k.naive_inbound({
    listen: { address: k.ip("0.0.0.0"), port: 443 },
    username: "user",
    password: "replace-with-your-password",
    certificate,
    private_key: privateKey,
  });
}
