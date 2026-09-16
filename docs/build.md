# Build and verification

Install [mise](https://mise.jdx.dev/getting-started.html), then run from the repository root:

```sh
git submodule update --init --recursive
mise trust
mise install
mise exec -- moon run workspace:check
```

mise owns native tools, uv owns Python, and moon owns orchestration. Activate
mise in your shell to omit `mise exec --` from subsequent commands.

## Finding tasks

Use `moon tasks` to discover tasks. Names follow `scope:action[-variant]`,
matching moon's project/task syntax: `rust:build-linux-release`,
`docker:test-tun`, `typescript:check-format`. Use `generate-*` for code generation,
`build-*` for artifacts, and `test-*` for execution tests. `check` aggregates
verification; `format` applies edits and `check-format` only checks them.

Start with `workspace:check` for development, `workspace:test-e2e` for integration
changes, and `benchmark:run` for performance work. Pass runner options after `--`:

```sh
mise exec -- moon run docker:test-tun -- --profile stress
```

## Where to change things

- Tool versions: [mise.toml](../mise.toml); Python version: [.python-version](../.python-version).
- Task graph: [.moon/workspace.yml](../.moon/workspace.yml) locates projects and their `moon.yml` files.
- CI setup and caches: [.github/actions/setup](../.github/actions/setup/action.yml).
- Network tests and measurement guidance: [e2e](../e2e/README.md) and [benchmarks](../benchmarks/README.md).
- Fork development and tests: [external sources](../external/README.md).

For individual Rust commands, use `uv run --locked python tools/rust.py <command>`
to retain Zig target selection. Network tests require Docker and `/dev/net/tun`;
use their launchers to keep test networking isolated.

When debugging stale artifacts, use `moon run --force <target>` to bypass moon's
cache. Remove `target` as well when you need a clean Cargo build. Keep installation,
container and measurement tasks uncached: their state lives outside moon's artifacts.
