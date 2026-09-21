# NaiveProxy

`kotoconn-naive` provides an authenticated TLS HTTP/2 inbound and a Linux
Cronet outbound using HTTP/2 or HTTP/3. Both carry TCP streams. HTTP/3 requires
a UDP-capable carrier. The outbound uses one Chromium connection pool shared
by its tunnels and verifies the proxy certificate and hostname.

```ts
const naive = k.dialer({
  dialer: null,
  outbound: {
    resolve_handler: resolver,
    implementation: k.naive_outbound({
      server: k.domain("proxy.example.com", 443),
      username: "user",
      password: "secret",
      // Optional: server_name, certificate (PEM), quic (defaults to false).
    }),
  },
});
```

`server_name` overrides the TLS name without changing the network endpoint.
`certificate` supplies PEM trust anchors for a private CA. Omit it to use
Cronet's system trust store. There is no option to disable certificate checks.
User destinations are resolved by the proxy server, not by Cronet or the
outbound's resolver.

`k.naive_inbound` accepts `listen`, `username`, `password`, `certificate` and
`private_key`. The certificate chain and key are PEM strings. Authentication
failure returns HTTP 404 before routing. It does not serve a camouflage website;
deploy an appropriate frontend when that behavior is needed. HTTP/1 and inbound
HTTP/3 are not supported. Ordinary authenticated HTTP/2 CONNECT clients can
omit the padding header and exchange unframed bytes.

See [build setup](../../docs/build.md) for the native library and
[the ADR](../../docs/adr/0010-naiveproxy-cronet-carriers.md) for ownership and
carrier behavior. Protocol references are the
[Naive specification](https://github.com/klzgrad/naiveproxy#padding-protocol-an-informal-specification)
and [SagerNet's Cronet bindings](https://github.com/SagerNet/cronet-go).
