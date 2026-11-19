# Build and verification

Install [mise](https://mise.jdx.dev/getting-started.html), then run from
the repository root:

```sh
git submodule update --init --recursive
mise trust
mise install
mise exec -- moon run workspace:check
```

Use `mise exec -- moon tasks` to find tasks. Pass runner arguments after
`--`. For individual Cargo commands, use
`mise exec -- uv run --locked python tools/rust.py <command>` to get the
workspace's target and native dependencies.

Linux builds fetch a prebuilt Cronet library for NaiveProxy. Install
`libcronet.so` on the runtime's library search path when distributing the
CLI. See [the supported targets](../tools/cronet.py).

See [end-to-end tests](../e2e/README.md) for isolated network tests and
[benchmarks](../benchmarks/README.md) for performance measurements.
