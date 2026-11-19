# End-to-end tests

After [workspace setup](../docs/build.md), provide Docker with Compose.
Run the integration checks or select protocol suites from the repository
root:

```sh
mise exec -- moon run workspace:test-e2e
mise exec -- moon run docker:test -- socks5 nested
```

Use e2e for interoperability and kernel integration. Put parser and exact
scheduling regressions in Rust tests. See [TUN verification](tun_support/README.md)
for the Linux TUN launcher.

When invoking runners directly, finish Docker builds and protocol tests
before starting the host isolation check. Docker can create host bridge
interfaces during those tasks.

Keep the artifact directory printed by a failed run. If cleanup was interrupted,
use the recorded Compose project name to remove its resources. Give concurrent
builds distinct image tags when testing different revisions.
