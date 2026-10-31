# Daemon

Run the example from the repository root:

```sh
mise exec -- uv run --locked python tools/rust.py run --locked -p kotoconn-cli -- run --config packages/api/examples/policy.ts
```

`Daemon::start(path, shutdown_timeout)` reads the policy and its imports, starts the resource manager and evaluates the entry module. CLI only supplies the path. `start_with_sources` accepts an independent source provider for embedding and tests. Startup returns once evaluation, including top-level await, and every resource creation started until then have settled, so the listeners it created are bound. An unhandled promise rejection before then fails startup. Handlers already serve calls during evaluation, for example to resolve a server name that a connection awaited by evaluation needs. Policy code changes take effect only after a restart. The example opens HTTP CONNECT, SOCKS5 and Shadowsocks 2022 listeners.

Policy creates and closes resources at any time; see [runtime resources](../../docs/adr/0003-runtime-resources.md). `resources()` reports the configurations of resources that have not been released yet, in creation order. `listen_addresses()` reports actual socket addresses of open inbounds and DNS listeners, including OS-assigned ports. `inbound_addresses()` also reports TUN interface names. See the [Linux TUN inbound](../tun/README.md) for configuration and routing.

Native components call handlers through the cloneable `daemon.policy()` handle with handler references taken from resource configurations. A call keeps its handler registered until it returns. Its methods use shared references. One dedicated thread polls the JS handler futures; native work runs on Tokio's multi-thread runtime. See [the threading decision](../../docs/adr/0002-single-thread-policy.md).

Ctrl+C and Unix SIGTERM start shutdown and its `--shutdown-timeout` deadline. Policy sleeps reject, then policy shutdown hooks run one at a time, newest first. Inbounds keep accepting and policy calls are still served while hooks run, so a hook can stop what it needs before undoing host configuration. After the last hook, resource creation and policy calls stop, and every open resource drains in dependency order. Inbounds and DNS listeners stop accepting and drain their sessions and queries. Once they are released and the accepted policy calls have finished, dialers and DNS services drain the connections and queries that remain, such as those of detached policy work. When the deadline expires, remaining work, including unfinished hooks, is cancelled, every resource is aborted and running JS is interrupted. Expiry makes the CLI exit with an error. A failed hook is logged and does not affect the result. An inbound that fails or panics starts the same shutdown, and `wait()` then reports its error. Accept errors do not fail an inbound: errors of one pending connection are skipped, and others, such as descriptor exhaustion, are retried after a pause. When startup fails, the hooks registered so far run and the resources created so far are released before `start` returns the error. `Daemon::stop()` requests shutdown synchronously; `wait()` observes completion. `shutdown(&self)` combines both and is idempotent; dropping the owner requests immediate cancellation.

Cancelling a lookup stops waiting for its result; an OS resolver call may continue in Tokio's blocking pool. After daemon cleanup, CLI shuts down the runtime without waiting for such calls.

Module names stay relative to the entry file's parent directory. Daemon reads only requested files and rejects paths, including symlinks, that leave that directory. It does not resolve npm packages; `@kotoconn/bindings` is provided natively. Startup transpiles TypeScript but does not run `tsc`; workspace checks typecheck the example separately.

TCP and UDP clients of a dialer expose independent close handles through `client_control`. `sessions()` returns stable session handles with `close()` and `wait()` methods. These are control capabilities; no hot-switching policy is imposed. Sessions run in independent Tokio tasks. See [the ADRs](../../docs/adr) for carrier capabilities, resolution, admission and UDP sessions.

Shadowsocks uses the single-user AES-128-GCM 2022 method in this version. The example key is public test data, not a deployment credential. HTTP supports CONNECT only; SOCKS5 supports CONNECT and UDP ASSOCIATE without authentication. UDP session routing uses `route_udp` and cannot override the destination. `lookup` uses system DNS.

A dialer's `carrier` is either another dialer or the host network, `k.host_network({ mark, interface })`. The host network requires IP targets and never resolves domains. On Linux, `mark` sets `SO_MARK` and `interface` sets `SO_BINDTODEVICE` on every TCP and UDP socket the dialer opens; on other platforms, creating a dialer with these options fails. Proxy outbounds carry domain targets in their own protocol, and direct inherits its carrier's behavior. A dialer's resolve handler therefore resolves its server name only when its carrier requires IP targets; otherwise the carrier, such as an upstream proxy, receives the name.

Routing handlers receive `flow.inbound`, `flow.source.address` and `flow.source.port`.
Compare an inbound with a created reference using `flow.inbound.equals(inbound)`.
Source is the transport peer for proxy listeners and the original packet source for
TUN. SOCKS5 UDP uses the UDP sender rather than its TCP control connection. UDP
source metadata stays at the association's initial peer if the transport migrates.

## Inbound sniffing

Set `sniff: { timeout: k.timeout(300) }` on `k.inbound(...)` to inspect payload before calling the routing handler. Omit `sniff` to disable it. The timeout must be positive and limits the whole inspection, rather than each read.

Sniffing recognizes HTTP Host and TLS ClientHello SNI over TCP, and QUIC Initial ClientHello SNI over UDP. QUIC v1, v2 and drafts 29–32 are supported. Fragmented TCP headers, TLS records and QUIC CRYPTO frames are assembled within a 64 KiB inspection limit. UDP also stops after 32 datagrams. Unsuccessful inspection leaves `flow.sniff === undefined` and preserves traffic for routing while the session remains active. UDP idle expiry or session cancellation can end the session during inspection and discard its buffers. Transport I/O errors close the session.

The handler receives `flow.sniff` with `protocol` set to `"http"`, `"tls"` or `"quic"`, and an optional `domain`. A recognized protocol without a hostname has `domain === undefined`. HTTP Host ports are removed, IP literals are excluded, and domains are lowercase. Encrypted ClientHello cannot expose its inner SNI.

`flow.dest` keeps the original destination. The script decides whether to use the sniffed domain for TCP routing:

```ts
const routing: RoutingHandler = (flow) => {
  if (flow.protocol === "udp") return k.route_udp(dialer);

  const domain = flow.sniff?.domain;
  const target = domain ? k.domain(domain, flow.dest.port) : flow.dest;
  return k.route(dialer, target);
};
```

UDP sniffing runs once per destination-specific session and can select an outbound without changing the destination. Inspection preserves TCP bytes and complete datagrams, including their order and address metadata. Server-first TCP protocols wait until the sniff timeout before routing. This follows sing-box's [protocol sniffing](https://sing-box.sagernet.org/configuration/route/sniff/) and [sniff action](https://sing-box.sagernet.org/configuration/route/rule_action/#sniff), with the protocol scope listed above.

## Logging

The `kotoconn` binary initializes a global tracing subscriber before starting the runtime. Logs go to stderr and default to `info`. Set `RUST_LOG` to control levels by module; an invalid filter fails startup. Use `--log-format json` for newline-delimited JSON, or keep the default `text` format. Text output uses color only when stderr is a terminal.

```sh
RUST_LOG=info,kotoconn_daemon=debug mise exec -- uv run --locked python tools/rust.py run --locked -p kotoconn-cli -- \
  run --config packages/api/examples/policy.ts --log-format json
```

CLI lifecycle events have stable `event` fields: `daemon_ready`, `daemon_stopping`, `daemon_stopped`, and `daemon_failed`. Consumers can parse `fields.event` in JSON output without matching the human-readable message. These events follow `RUST_LOG` filtering; lifecycle consumers must enable `info` for `kotoconn`.

Info events report startup, bound inbound addresses and shutdown. Warnings report connection and datagram failures; errors report policy call failures and fatal CLI failures. Debug events include session start and completion, routing decisions, TCP byte counts and UDP idle expiry. Session spans carry the transport, destination and registry session ID. Inbound and accepted TCP connection spans add the inbound ID and peer/local addresses. Policy calls retain their parent span across the worker queue, and spawned protocol and native tasks inherit the current span.

Logs do not dump configuration objects, credentials, source code or packet payloads. Addresses and domains appear in spans, and error text may include details supplied by user policy. Libraries emit tracing events without installing a subscriber; applications embedding `Daemon` configure their own subscriber.
