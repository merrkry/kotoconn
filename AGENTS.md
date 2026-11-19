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

Write documentation sparingly. Every document must meet both conditions.

- Its content remains valid as the implementation evolves.
- It explains high-level intent and trade-offs that the code cannot convey.

Use [RFCs](docs/rfc/README.md) for fundamental design decisions and
[protocol notes](docs/protocols/README.md) for deliberate interoperability choices.

## Version control

Use scoped commits by crate name or aspect, e.g. `config: split outbound mod` `ci: allow workflow_dispatch`.

Forked dependencies live at `external/<repo>` as Git submodules. Use the
[maintain-submodule-fork skill](.agents/skills/maintain-submodule-fork/SKILL.md)
before changing a fork or its pin.

## Verification

Run all toolchain commands through `mise exec --`. Moon is the task runner.

During development, run checks appropriate to the change. Before finishing, run the full standard checks with `mise exec -- moon run workspace:check`. Use `workspace:ci` when e2e and benchmark smoke tests are also needed. Read Moon configuration and language toolchain files for other tasks and options.

Read [build orchestration](docs/build.md) when changing tasks, tool versions, caches, containers or CI.
