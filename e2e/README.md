# End-to-end tests

[Set up the workspace](../docs/build.md) and provide Docker with Compose. From
the repository root, run all integration checks or select protocol suites:

```sh
mise exec -- moon run workspace:test-e2e
mise exec -- moon run docker:test -- socks5 nested
```

For Linux TUN changes, see [TUN verification](tun_support/README.md). For
performance comparisons, see [benchmarks](../benchmarks/README.md).

When adding protocol coverage, edit [run.py](run.py) for scenarios and
[traffic.py](traffic.py) for traffic assertions. Put exact parser and scheduling
regressions in Rust tests; use e2e for interoperability and kernel integration.

Keep the artifact directory printed by a failed run. If cleanup was interrupted,
use its recorded Compose project name to remove only that run's resources.
Give concurrent builds distinct image tags when testing different revisions.
