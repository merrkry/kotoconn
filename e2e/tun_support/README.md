# TUN verification

Use Rust tests for exact packet and scheduling contracts, Linux e2e for kernel
integration, and [benchmarks](../../benchmarks/tun/README.md) for measurements.
Known ordering bugs need explicit synchronization or virtual time in regression
tests. Repetitions and seeds cannot reproduce OS scheduling.

## Real-network E2E

After [workspace setup](../../docs/build.md), provide Docker and `/dev/net/tun`:

```sh
mise exec -- moon run docker:test-tun
mise exec -- moon run docker:test-tun -- --profile stress
mise exec -- moon run docker:test-isolation
```

Use quick coverage for routine changes and stress coverage for changes to
connection reuse, buffering or lifecycle. Select a failing workload with `--case`;
use `--help` for the available filters. To test optimized code, first run
`rust:build-linux-release`, then pass `--binary target/tun/release/kotoconn` and
`--traffic-binary target/tun/release/kotoconn-tun-traffic` to the TUN task.

## Changing coverage

- [tun.py](../tun.py) owns e2e expectations and lifecycle cases.
- [scenarios.py](scenarios.py) defines shared workloads; [packets.py](packets.py) supplies independent malformed-packet controls.
- [environment.py](environment.py) owns isolation and process management; [check_isolation.py](check_isolation.py) verifies concurrent runs and cleanup.

Distinguish the [supported protocol subset](../../crates/tun/README.md) from RFC
requirements. Resolve ambiguous behavior against the relevant RFC, then preserve
it in a deterministic Rust regression. Packet rejection alone does not prove
that the corresponding traffic is forbidden by the RFC.

## Isolation and artifacts

Always use the container launcher, including locally. Keep test routing and
sysctls inside its private network; do not mount host networking, D-Bus or the
container-engine socket. Source and binaries should remain read-only.

Retain failed-run artifacts before changing workloads. A seed reproduces payloads
and offered traffic, not timing. If SIGKILL prevents cleanup, the run's
`container.cid` identifies the container to remove. Never clean up another run's
resources by interface name or a shared container-name prefix.
