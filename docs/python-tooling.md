# Python tooling

Install uv 0.12.5, then run these commands from the repository root:

```sh
uv sync --locked
uv run --locked ruff format --check
uv run --locked ruff check
uv run --locked pyrefly check
```

uv uses Python 3.14 from `.python-version` and installs the development tools
from `uv.lock` into `.venv`. It downloads Python if a compatible interpreter is
not available. The tooling project is not a distributable Python package and
has no runtime dependencies.

Ruff checks formatting, imports, and lint rules. Pyrefly checks types. Both check
Python files under `e2e`, `benchmarks`, and `tools`; third-party code under
`external` is outside their configured scope. The check job in
`.github/workflows/checks.yml` runs the same checks alongside Rust and TypeScript.

To apply formatting or import fixes locally:

```sh
uv run --locked ruff format
uv run --locked ruff check --fix
```

Existing Python runner commands remain available. Use `uv run --locked python`
in place of `python3` to run them with the managed interpreter.
