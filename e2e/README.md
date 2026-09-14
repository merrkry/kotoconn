# End-to-end tests

These tests run the real `kotoconn` CLI and sing-box in separate containers.
They cover protocol interoperability in both client and server roles, nested
dialers and TypeScript policies. Rust tests cover protocol edge cases and internal
lifecycle contracts. Kotoconn readiness and shutdown checks parse the CLI's JSON
`fields.event` values; sing-box readiness uses its startup log message.

Requirements: Python 3.10+, Docker Engine and Docker Compose v2 or later. No
Python packages are needed. Run from the repository root:

```sh
docker build -f e2e/Dockerfile -t kotoconn-e2e:local .
python3 e2e/run.py
python3 e2e/run.py socks5 nested
```

Use `python3 e2e/run.py --help` for available suites and execution options.

Runs are isolated and may execute concurrently. Build the image first, or give
concurrent builds distinct image tags.

The runner prints the artifact directory containing generated configurations and
logs. If cleanup is interrupted by SIGKILL or host shutdown, use the project name
in those logs to identify leftover Compose resources.

When changing coverage, see [run.py](run.py) for scenarios and
[traffic.py](traffic.py) for traffic assertions and sing-box limitations.

Linux TUN tests run the real CLI against the kernel TCP/IP stack in a Docker
container with a private network. They repeat single and concurrent TCP/UDP workloads at MTU 1500 and 9000,
including short connections, sparse activity, sustained transfers, mixed
malformed input, half-close and device cleanup.

First [build the TUN binaries and runtime image](tun_support/README.md#real-network-e2e).
Then run:

```sh
python3 e2e/tun.py
python3 e2e/tun.py --profile stress
python3 e2e/tun_support/check_isolation.py
```

This suite needs Docker and `/dev/net/tun`. Every invocation creates a new
container and a unique artifact directory, including when `--output` is supplied.
Containers have no external network or host D-Bus access. Concurrent workspaces
can use the same interface name and ports without sharing devices. See
[the TUN documentation](../crates/tun/README.md) for its supported protocol scope.

See [TUN test responsibilities](tun_support/README.md) for coverage, scenario
selection, failure artifacts and the boundary between Rust tests, E2E and
benchmarks. The stress profile adds MTU 1280/65535 and more connections and
repetitions. It is available through the E2E workflow dispatch input as well.

TUN E2E and benchmarks share container, process, packet-injection and measurement
helpers in `tun_support/`. The runners choose their own workloads and pass/fail
criteria.
