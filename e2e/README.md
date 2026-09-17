# End-to-end tests

[Set up the workspace](../docs/build.md) and provide Docker with Compose. From
the repository root, run all integration checks or select protocol suites:

```sh
mise exec -- moon run workspace:test-e2e
mise exec -- moon run docker:test -- socks5 nested
mise exec -- moon run docker:test -- naive naive-nested naive-quic
```

For Linux TUN changes, see [TUN verification](tun_support/README.md). For
performance comparisons, see [benchmarks](../benchmarks/README.md).

When adding protocol coverage, edit [run.py](run.py) for scenarios and
[traffic.py](traffic.py) for traffic assertions. Put exact parser and scheduling
regressions in Rust tests; use e2e for interoperability and kernel integration.

The Naive suite tests HTTP/2 in both directions against the pinned sing-box
image. `naive-nested` and `naive-quic` place the server on the SOCKS gateway's
loopback interface, so bypassing the configured TCP or UDP carrier fails.
The QUIC server listens only on UDP. Each scenario creates a one-day certificate
inside its test container and verifies it through an explicit trust anchor.

Keep the artifact directory printed by a failed run. If cleanup was interrupted,
use its recorded Compose project name to remove only that run's resources.
Give concurrent builds distinct image tags when testing different revisions.
