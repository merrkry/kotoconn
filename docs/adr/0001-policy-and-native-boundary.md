# Separate user policy from native traffic handling

Users write policy in TypeScript. Policy decides routing, name resolution, DNS answers, resource lifetimes and host configuration. Native Rust code carries traffic and owns protocol state, sessions, caches and I/O. The two sides exchange owned values and resource references, never protocol objects or JS values.

Native interfaces are explicit. Rust never fills missing options from global defaults, and native code takes no action that policy did not request.

The TypeScript API has two levels. The low-level API exposes each native capability directly. The high-level API in `@kotoconn/api` composes it with defaults for common setups. High-level helpers use only the public low-level API, so users can replace or bypass any of them.
