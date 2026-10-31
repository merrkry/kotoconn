import { kotoconn as k } from "@kotoconn/bindings";

const naive = await k.dialer({
  carrier: k.host_network({}),
  outbound: {
    resolve_handler: (name) => k.lookup(name),
    implementation: k.naive_outbound({
      server: k.domain("proxy.example.com", 443),
      username: "user",
      password: "replace-with-your-password",
    }),
  },
});
await k.inbound({
  implementation: k.socks5_inbound({
    listen: { address: k.ip("127.0.0.1"), port: 1080 },
  }),
  routing_handler: (flow) => (flow.protocol === "tcp" ? k.route(naive, flow.dest) : k.reject()),
  udp_idle_timeout: k.timeout(30_000),
});

// For a server policy, supply PEM strings and create an inbound with this
// implementation and a routing handler that selects your desired outbound.
export function naiveServer(certificate: string, privateKey: string) {
  return k.naive_inbound({
    listen: { address: k.ip("0.0.0.0"), port: 443 },
    username: "user",
    password: "replace-with-your-password",
    certificate,
    private_key: privateKey,
  });
}
