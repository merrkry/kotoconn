# Daemon

Run the example from the repository root:

```sh
pnpm exec nx run rust:run -- run --config packages/api/examples/policy.ts
```

`Daemon::start(path, shutdown_timeout)` reads the policy and its imports, evaluates it, and seals registration. CLI only supplies the path. `start_with_sources` accepts an independent source provider for embedding and tests. All configured listeners bind before startup returns. The example opens HTTP CONNECT, SOCKS5 and Shadowsocks 2022 listeners. `listen_addresses()` reports actual socket addresses, including OS-assigned ports. `inbound_addresses()` also reports TUN interface names. See the [Linux TUN inbound](../tun/README.md) for configuration and routing.

Native components call handlers through the cloneable `daemon.policy()` handle. Its methods use shared references, and configuration access returns an immutable startup snapshot. One dedicated thread polls the JS handler futures; native work runs on Tokio's multi-thread runtime. See [the threading decision](../../docs/adr/0003-single-thread-policy-and-shared-handles.md).

Ctrl+C and Unix SIGTERM stop admission. Accepted calls, pending native promises and TCP sessions drain until `--shutdown-timeout` expires, then remaining work is cancelled and running JS is interrupted. Expiry makes the CLI exit with an error. `Daemon::stop()` requests shutdown synchronously; `wait()` observes completion. `shutdown(&self)` combines both and is idempotent; dropping the owner requests immediate cancellation.

Cancelling a lookup stops waiting for its result; an OS resolver call may continue in Tokio's blocking pool. After daemon cleanup, CLI shuts down the runtime without waiting for such calls.

Module names stay relative to the entry file's parent directory. Daemon reads only requested files and rejects paths, including symlinks, that leave that directory. It does not resolve npm packages; `@kotoconn/bindings` is provided natively. Startup transpiles TypeScript but does not run `tsc`; workspace checks typecheck the example separately.

TCP and UDP clients expose independent close handles through `client_control`. `sessions()` returns stable session handles with `close()` and `wait()` methods. These are control capabilities; no hot-switching policy is imposed. Sessions run in independent Tokio tasks. See ADRs 0004–0008 for carrier capabilities, resolution, UDP lifetime, admission and protocol scope.

Shadowsocks uses the single-user AES-128-GCM 2022 method in this version. The example key is public test data, not a deployment credential. HTTP supports CONNECT only; SOCKS5 supports CONNECT and UDP ASSOCIATE without authentication. UDP session routing uses `route_udp` and cannot override the destination. `lookup` uses system DNS; the I/O carrier itself never resolves domain targets.

## Logging

The `kotoconn` binary initializes a global tracing subscriber before starting the runtime. Logs go to stderr and default to `info`. Set `RUST_LOG` to control levels by module; an invalid filter fails startup. Use `--log-format json` for newline-delimited JSON, or keep the default `text` format. Text output uses color only when stderr is a terminal.

```sh
RUST_LOG=info,kotoconn_daemon=debug pnpm exec nx run rust:run -- \
  run --config packages/api/examples/policy.ts --log-format json
```

CLI lifecycle events have stable `event` fields: `daemon_ready`, `daemon_stopping`, `daemon_stopped`, and `daemon_failed`. Consumers can parse `fields.event` in JSON output without matching the human-readable message. These events follow `RUST_LOG` filtering; lifecycle consumers must enable `info` for `kotoconn`.

Info events report startup, bound inbound addresses and shutdown. Warnings report connection and datagram failures; errors report policy call failures and fatal CLI failures. Debug events include session start and completion, routing decisions, TCP byte counts and UDP idle expiry. Session spans carry the transport, destination and registry session ID. Inbound and accepted TCP connection spans add the inbound ID and peer/local addresses. Policy calls retain their parent span across the worker queue, and spawned protocol and native tasks inherit the current span.

Logs do not dump configuration objects, credentials, source code or packet payloads. Addresses and domains appear in spans, and error text may include details supplied by user policy. Libraries emit tracing events without installing a subscriber; applications embedding `Daemon` configure their own subscriber.
