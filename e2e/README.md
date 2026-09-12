# End-to-end tests

These tests run the real `kotoconn` CLI and sing-box in separate containers.
They cover protocol interoperability in both client and server roles, nested
dialers and TypeScript policies. Rust tests cover protocol edge cases and internal
lifecycle contracts.

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
