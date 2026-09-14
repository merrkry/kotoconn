# Development and verification

Nx coordinates the Cargo workspace, TypeScript packages, Python tooling, Docker
builds, E2E suites and benchmarks. Run commands from the repository root.

## Setup

Install [mise](https://mise.jdx.dev/installing-mise.html), then run:

```sh
mise trust
mise install
git submodule update --init --recursive
mise exec -- pnpm install --frozen-lockfile
mise exec -- pnpm exec nx run workspace:setup
mise exec -- pnpm exec nx run workspace:build
mise exec -- pnpm exec nx run workspace:verify
```

`mise.toml` pins Node, pnpm, uv, Rust/Cargo, cargo-zigbuild and Zig. mise installs
native tools; uv manages Python 3.14 and the Ruff/Pyrefly environment in `.venv`
using `.python-version`, `pyproject.toml` and `uv.lock`. `workspace:setup` runs
`uv sync --locked`. Subsequent examples assume a
[mise-activated shell](https://mise.jdx.dev/dev-tools/); otherwise prefix each
command with `mise exec --`.

Rust compilation, binding generation, Clippy and tests use cargo-zigbuild.
Build commands invoke `cargo zigbuild`; running binaries, Clippy and tests use
the supported `cargo-zigbuild run`, `clippy` and `test` subcommands. rustfmt uses
`cargo fmt` because it does not compile or link.

The default target is `x86_64-unknown-linux-gnu.2.36`, matching Debian Bookworm's
glibc baseline. Passing a target is required for cargo-zigbuild to use Zig.
`KOTOCONN_RUST_TARGET` overrides it for both Nx and Bake; add the corresponding
Rust target to `mise.toml` before running `mise install`. Local generators and
test binaries must be runnable on the host. Artifacts live under
`target/<triple>/`, with no glibc suffix in the directory name. Zig supplies the
target C compiler, libc and linker; build configuration contains no Nix store
paths or host-libc link flags.

The container runners use only the Python standard library. See
[the fork workflow](../external/README.md) before editing smoltcp.

For containers, install Docker Engine, Compose v2 and Buildx with Bake support.
TUN suites require Linux and `/dev/net/tun`. Buildx's default builder works
locally; CI uses the `docker-container` driver to export its cache. A Podman
`docker` alias can run the existing test launchers with prebuilt images and
binaries, but does not supply Buildx Bake.

## Tasks

| Command after `pnpm exec nx run` | Work |
| --- | --- |
| `workspace:build` | Build the Rust workspace, generate bindings and build the TypeScript API |
| `workspace:verify` | Check Rust formatting, Clippy, Rust tests, smoltcp fork, TypeScript and Python |
| `rust:build` / `rust:release` | Host debug workspace / optimized CLI and traffic binaries |
| `rust:run -- --help` | Build and invoke the CLI; arguments after `--` go to the CLI |
| `rust:format` / `rust:lint` / `rust:test` | Cargo formatting, Clippy, or workspace tests |
| `rust:fork-test` | Check formatting and run the supported smoltcp fork test matrix |
| `@kotoconn/bindings:generate` | Generate editor declarations from Rust |
| `@kotoconn/api:build` | Generate bindings, then compile the API |
| `workspace:check` | TypeScript formatting, lint, builds, type checking and binding tests |
| `python:check` / `python:format` | Ruff and Pyrefly checks / Ruff formatting |
| `docker:plan` | Print the resolved development Bake configuration |
| `docker:build` / `docker:release` | Compile with Zig and package container artifacts for E2E / optimized benchmarks |
| `e2e:test -- socks5 nested` | Build the images, then run selected protocol suites |
| `e2e:tun` / `e2e:tun:stress` | Build and run the quick / stress TUN profile |
| `e2e:isolation` | Build and check concurrent TUN isolation |
| `e2e:check` | All E2E suites, with one shared image build |
| `benchmarks:run -- --profile full` | Build release binaries and run the full benchmark profile |
| `benchmarks:udp-churn` | Release UDP warmup reuse regression |
| `benchmarks:check` | Short debug benchmark pipeline and UDP warmup checks |
| `workspace:ci` | All static/unit checks, E2E and benchmark pipeline checks |

`pnpm run format` and `pnpm run check` retain their TypeScript scope. Run Rust
checks through Nx or cargo-zigbuild, separately from package scripts. `pnpm run bindings`
refreshes ignored declarations for editors. `pnpm run build` builds the main
program and TypeScript API. To format all languages, run:

```sh
pnpm exec nx run-many -t format -p workspace,rust,python
```

Runner flags go after `--`; dependency tasks do not receive them. For example:

```sh
pnpm exec nx run e2e:tun -- --case mixed-malformed --mtu 9000 --family 6
pnpm exec nx run benchmarks:run -- --duration 10 --repetitions 3
```

Use `pnpm exec nx show projects` to list projects and
`pnpm exec nx graph --file=target/project-graph.json` to export the dependency
graph without starting a server. Rust is one Nx project because Cargo already
tracks crate dependencies and features within its workspace.

## Cache and ordering

Nx caches generated declarations and TypeScript compilation/checks. Their inputs
include Rust sources, manifests, the smoltcp revision, compiler identity,
the cargo-zigbuild/Zig versions, selected target, TypeScript configuration and
the mise configuration and lockfiles. Generated declarations and API
`dist` files are declared outputs, so deleting them is repaired by cache restore.
Cargo keeps its own incremental artifacts under `target`; Nx always invokes
cargo-zigbuild for builds, Clippy and Rust tests. Python checks also run each time.

Docker tasks depend on the Rust debug/release build, then invoke Bake to package
the results. Dockerfiles contain only the runtime and binary export stages.
BuildKit checks its layers and recreates missing images or exported files.
The named artifact context selects `target/<triple>/debug` or `release`;
`.dockerignore` limits the main context to the two Dockerfiles. Source compilation
and binding generation share Cargo artifacts, with no second compilation inside
Docker. CI caches Rust artifacts with rust-cache and runtime image layers with
BuildKit's GitHub Actions cache.

Real E2E runs and benchmarks are never cached. Benchmark targets run alone in
an Nx invocation to avoid competing with other scheduled tasks. This does not
reserve the machine against other shells or workspaces. Use an otherwise idle
machine for measurements. Dev and release artifacts have separate directories;
image tags remain local and shared, so concurrent builds in separate workspaces
need separate Docker builders/stores or custom Bake tags and runner image flags.

Nx dependency ordering and cache inputs follow the
[Nx project configuration](https://nx.dev/docs/reference/project-configuration).
The image and filesystem exports use
[Docker Bake targets](https://docs.docker.com/build/bake/reference/).

## CI

`checks.yml` invokes `workspace:verify` with the same lockfiles and tasks as local
development. Both workflows install native tools through the shared mise setup
action. `e2e.yml` builds portable binaries with `rust:build`, then invokes the
shared Bake file through Docker's Bake action, which supplies GitHub cache
credentials. It then runs the Nx E2E and benchmark
targets with `--excludeTaskDependencies`, because Bake has already produced their
images and binaries. Local commands include dependencies by default.

The workflows upload E2E and benchmark diagnostics even after failure. Manual
E2E dispatch can select the stress TUN profile. CI's short benchmark samples
check measurement and cleanup behavior; they are not performance baselines.
