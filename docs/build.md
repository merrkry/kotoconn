# Build and verification

Install [mise](https://mise.jdx.dev/getting-started.html), then run from the
repository root:

```sh
git submodule update --init --recursive
mise trust
mise install
mise exec -- moon run workspace:check
```

`mise.toml` pins moon, Rust, cargo-zigbuild, Zig, Node, pnpm and uv. uv installs
its managed Python version from `.python-version` and resolves the tooling
with `uv.lock`. An activated mise shell can omit `mise exec --` below.

## Tasks

| Command | Work |
| --- | --- |
| `moon run workspace:build` | Rust workspace and TypeScript API |
| `moon run workspace:check` | Rust, fork, TypeScript and Python checks |
| `moon run rust:format` | Apply Rust formatting |
| `moon run typescript:format python:format` | Apply TypeScript and Python formatting |
| `moon run rust:bindings` | Generate TypeScript declarations |
| `moon run docker:build` | Build Linux debug binaries and both runtime images |
| `moon run workspace:e2e` | Protocol, TUN and concurrent isolation tests |
| `moon run benchmark:run` | Release build and TUN benchmark |
| `moon run benchmark:smoke benchmark:udp-churn` | CI benchmark checks |
| `moon run workspace:ci` | All checks and integration suites |

`moon tasks` lists the individual targets. Arguments after `--` go to the
selected task, for example `moon run docker:tun -- --profile stress` or
`moon run benchmark:run -- --case udp-paced`. The root pnpm scripts delegate
to moon so editor and package workflows use the same dependency graph.

## Rust and container artifacts

`tools/rust.py` invokes cargo-zigbuild for build, clippy, test and code generation
with an explicit target. Linux GNU targets use a glibc 2.28 baseline. rustfmt
runs directly because it does not compile or link. For an individual Cargo
operation, use `uv run --locked python tools/rust.py check --workspace --locked`.

Native checks use rustc's host triple. Container builds support x86_64 and
aarch64, select the matching Linux GNU target, and install its Rust standard
library. `rust:linux-debug` and `rust:linux-release` copy the resulting CLI and
traffic tool into `target/tun/debug` and `target/tun/release`. Both images and
TUN runners consume these declared outputs. Running cross-compiled binaries
requires a compatible runtime; Linux network verification runs in containers.

`e2e/Dockerfile` has an `app` target for the CLI and a `test-runtime` target for
Python, iproute2 and util-linux. The echo service and TUN launcher share the
latter. The images contain no Rust compiler. Docker or its Podman-compatible
interface must be available for container tasks, with `/dev/net/tun` available
for Linux TUN tests. The existing network isolation rules remain documented in
[the TUN guide](../e2e/tun_support/README.md).

## Caches

Moon hashes explicit source groups, tool versions, lockfiles, configuration and
the mise-provided execution platform.
The glob walker includes smoltcp submodule contents, including local edits.
Generated declarations and TypeScript dist directories are outputs, excluded
from source globs. Rust tasks declare relevant compiler environment inputs.
Binding generation and staged binaries share a Cargo mutex within a moon run;
Cargo's own locks also protect concurrent invocations.

Moon archives only declared outputs. Cargo keeps intermediate compilation
artifacts in target directories. Install tasks always run their package
manager's locked reconciliation, so deleting node_modules or `.venv` is safe.
Docker image tasks always run the builder because a moon archive cannot restore
an engine's image store. E2E and benchmark tasks always execute and produce
fresh diagnostic directories. Benchmark tasks share a mutex to avoid overlapping
measurements within one moon invocation.

CI caches package downloads separately from compiler artifacts and moon output
archives. Compiler cache keys include the runner architecture, tool versions,
Cargo lockfile and check/e2e scope, with older compatible entries available for
incremental rebuilds. CI disables Cargo incremental compilation. It preserves
`.moon/cache/hashes` and `.moon/cache/outputs`, not machine-specific task state
or test results.

With `KOTOCONN_DOCKER_CACHE` set, the Docker adapter uses Buildx to load images
and export layers to that directory. Each target replaces its previous export
so old layers do not accumulate. Local runs use the engine's normal layer
cache. `.dockerignore` admits only the runtime Dockerfile, Python version and
staged CLI. A container build never sends Cargo or package caches as context.

To exercise a cold task cache without deleting compiler downloads, run
`MOON_CACHE=off moon run workspace:check`. Delete `target` for a full Rust rebuild;
moon can restore declared outputs unless its cache is also disabled.
