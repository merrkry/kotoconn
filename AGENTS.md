# AGENTS.md

Kotoconn is a programmable proxy.

The repository is experimental. Freely rewrite or replace abstractions that no longer fit the current design or are poorly implemented. Prioritize the current design and code quality without preserving backward compatibility.

## Correctness

In non-test code, every `unwrap`, `expect`, `unsafe` block or implementation, and similar operation that relies on programmer-maintained invariants must have a nearby `SAFETY` comment. Explain the required invariants and why they hold at that location.

Actively use `debug_assert!` and related assertion macros to check invariants that the type system and existing runtime checks do not cover.

Time limits are allowed only when interacting with external systems, such as subprocesses, OS I/O, or user code. Fixed delays or timeout lengths must not express ordering dependencies. Use explicit synchronization or observable completion instead, so code and tests remain correct under parallel execution and varying scheduling speeds.

## Dependencies

Do not make avoiding dependencies the primary consideration. Use well-maintained dependencies for:

- reducing boilerplate code
- complex features: cross-platform support, cross-language interop, network protocols
- performance- or security-critical logic

## Documentation

See `docs/adr` for design decisions.

Documentation must remain consistent with the implementation. In cases of ambiguous or conflicting semantics, you must explicitly confirm with the user whether to modify the documentation or adjust the implementation.

## Code Style

Leave one blank line between top-level items, including type, trait, implementation, and function definitions. Keep related imports and module declarations grouped.

Use blank lines to show the phases of longer functions. Separate setup, validation, core work, and cleanup into readable blocks, and expand dense one-line branches when the control flow is easier to scan that way. Avoid long uninterrupted runs of executable statements.

When reviewing or editing code, scan for consecutive non-empty lines and add a blank line wherever the next statement starts a distinct logical step.

## Version control

Use scoped commits by crate name or aspect, e.g. `config: split outbound mod` `ci: allow workflow_dispatch`.

Before editing `external/smoltcp`, follow the [stable fork branch workflow](external/README.md#smoltcp-development-branch). Keep its development checkout attached to the persistent branch.

## Verification

Run all toolchain commands through `mise exec --`. Moon is the task runner.

During development, run checks appropriate to the change. Before finishing, run the full standard checks with `mise exec -- moon run workspace:check`. Use `workspace:ci` when e2e and benchmark smoke tests are also needed. Read Moon configuration and language toolchain files for other tasks and options.

Read [build orchestration](docs/build.md) when changing tasks, tool versions, caches, containers or CI.
