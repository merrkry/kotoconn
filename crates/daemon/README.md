# Daemon

Run the example from the repository root:

```sh
cargo run -p kotoconn-cli -- run --config packages/api/examples/policy.ts
```

`Daemon::start(path, shutdown_timeout)` reads the policy and its imports, evaluates it, and seals registration. CLI only supplies the path. `start_with_sources` accepts an independent source provider for embedding and tests. Protocol listeners and forwarding are not implemented yet; the example does not open port 8080.

Native components call handlers through the cloneable `daemon.policy()` handle. Its methods use shared references, and configuration access returns an immutable startup snapshot. One dedicated thread polls the JS handler futures; native work runs on Tokio's multi-thread runtime. See [the threading decision](../../docs/adr/0003-single-thread-policy-and-shared-handles.md).

Ctrl+C and Unix SIGTERM stop admission. Accepted calls and pending native promises drain until `--shutdown-timeout` expires, then remaining work is cancelled and running JS is interrupted. Expiry makes the CLI exit with an error. `Daemon::shutdown(&self)` is idempotent; dropping the owner requests immediate cancellation.

Cancelling a lookup stops waiting for its result; an OS resolver call may continue in Tokio's blocking pool. After daemon cleanup, CLI shuts down the runtime without waiting for such calls.

Module names stay relative to the entry file's parent directory. Daemon reads only requested files and rejects paths, including symlinks, that leave that directory. It does not resolve npm packages; `@kotoconn/bindings` is provided natively. Startup transpiles TypeScript but does not run `tsc`; workspace checks typecheck the example separately.
