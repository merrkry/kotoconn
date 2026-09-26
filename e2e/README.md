# End-to-end tests

[Set up the workspace](../docs/build.md) and provide Docker with Compose. From
the repository root, run all integration checks or select protocol suites:

```sh
mise exec -- moon run workspace:test-e2e
mise exec -- moon run docker:test -- socks5 nested
mise exec -- moon run docker:test -- hysteria2 hysteria2-salamander
mise exec -- moon run docker:test -- naive naive-nested naive-quic
mise exec -- moon run docker:test -- anytls
```

For Linux TUN changes, see [TUN verification](tun_support/README.md). For
performance comparisons, see [benchmarks](../benchmarks/README.md).

The protocol runner and Docker image builds share a Moon mutex with the host
network isolation check. Rootful Docker can create bridge interfaces during
these tasks, which would change the isolation check's host snapshot. TUN
containers still run concurrently within that check. When invoking the Python
runners directly, wait for protocol tests and image builds to finish first.

When adding protocol coverage, edit [run.py](run.py) for scenarios and
[traffic.py](traffic.py) for traffic assertions. Put exact parser and scheduling
regressions in Rust tests; use e2e for interoperability and kernel integration.

The AnyTLS suite runs the adapter against the pinned official Go session library
in both directions. The task builds the Go peer and Rust test harness, then runs
them in containers with only loopback networking. These scenarios run by default,
including in CI, and cover connection reuse, padding updates, TCP and UDP. See
[AnyTLS verification](../docs/anytls.md#verification) for details. Use
`--anytls-image` to select a separately tagged interoperability image.

The Naive suite tests HTTP/2 in both directions against the pinned sing-box
image. `naive-nested` and `naive-quic` place the server on the SOCKS gateway's
loopback interface, so bypassing the configured TCP or UDP carrier fails.
The QUIC server listens only on UDP. Each scenario creates a one-day certificate
inside its test container and verifies it through an explicit trust anchor.

Keep the artifact directory printed by a failed run. If cleanup was interrupted,
use its recorded Compose project name to remove only that run's resources.
Give concurrent builds distinct image tags when testing different revisions.

The Hysteria suites use the official 2.12.3 image with BBR in both directions.
Their test-only TLS material is documented in [hysteria2](hysteria2/README.md).
