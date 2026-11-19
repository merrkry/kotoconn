---
name: maintain-submodule-fork
description: Maintain a dependency fork tracked as a Git submodule. Use when changing the fork, integrating upstream changes, or updating the parent repository's pin.
---

# Maintain a submodule fork

Kotoconn keeps its forked submodules under `external/<repo>` and tracks each
fork's `kotoconn` branch.

`external/<repo>/PATCH.md` records how the fork differs from upstream.

Do not write regression tests for the fork's behavior in the parent
repository. The fork's test suite must verify that behavior. The parent
code should rely on those guarantees because we control both repositories.

Every change to the fork must pass its test suite.
