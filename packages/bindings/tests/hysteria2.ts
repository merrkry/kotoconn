import { kotoconn as k } from "../src/index.js";

k.hysteria2_outbound({
  server: k.domain("proxy.example", 443),
  password: "password",
});

k.hysteria2_inbound({
  listen: { address: k.ip("127.0.0.1"), port: 443 },
  password: "password",
  certificate: "certificate PEM",
  private_key: "private key PEM",
});
