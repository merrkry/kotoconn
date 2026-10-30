# Leave host network configuration to policy

Native code never changes host network configuration on its own. It creates exactly what a resource requests, such as a TUN device and its addresses, and never adds routes, routing rules, firewall rules or DNS settings. Implicit automatic routing in other proxies is hard to reason about and to combine with complex host setups.

Policy configures the host instead. `@kotoconn/api` offers presets for common setups, such as automatic routing for a TUN inbound. Following [ADR 0001](0001-policy-and-native-boundary.md), presets use only public low-level primitives, so users can inspect, replace or bypass each step:

- `exec` runs a program from an argument vector without a shell and reports its exit status and output. A nonzero status is a result, not an error. Policy code is trusted and runs with the daemon's privileges, so `exec` is a general capability.
- Shutdown hooks run in reverse registration order when shutdown starts, under the shutdown deadline. The daemon drains the resources that remain open only after the hooks finish, so a hook decides what to drain before it removes host configuration.
- Dialers that reach the host network directly accept a socket mark and an interface binding. Loop avoidance needs these socket options, and commands cannot set them.
- A routing decision can pass a flow to a DNS handler, so DNS traffic captured by TUN reaches DNS policy through an explicit decision.

A deadline expiry, crash or forced kill can skip shutdown hooks, so presets are idempotent. They use fixed routing tables and rule priorities and replace their own previous entries, so the next start repairs leftover state. A preset chooses the order of route removal and TUN drain at shutdown. The default drains TUN first, so traffic cannot bypass the proxy while routes are removed; new connections fail during that period instead.
