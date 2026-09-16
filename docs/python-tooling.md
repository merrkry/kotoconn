# Python tooling

[Install the workspace tools](build.md), then run:

```sh
mise exec -- moon run python:check
mise exec -- moon run python:format
```

mise installs uv. uv installs the managed interpreter pinned in `.python-version`
and the development tools locked in `uv.lock` into `.venv`. The tooling project
is not a distributable Python package and has no runtime dependencies.

Ruff checks formatting, imports and lint rules. Pyrefly checks types. Both cover
`e2e`, `benchmarks` and `tools`; third-party code under `external` is outside their
scope. `python:sync` reconciles the environment before checks, even with a warm
moon cache.

For direct runner invocations or import fixes, use the same environment:

```sh
mise exec -- uv run --locked python e2e/run.py --help
mise exec -- uv run --locked ruff check --fix
```
