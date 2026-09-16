# Python tooling

Follow the [workspace setup](build.md). Use `moon run python:check` for verification
and `moon run python:format` to apply formatting.

Run one-off tools through `mise exec -- uv run --locked <command>`. Change Python
dependencies in [pyproject.toml](../pyproject.toml) and commit the updated `uv.lock`.
