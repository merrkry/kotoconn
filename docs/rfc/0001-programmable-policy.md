# RFC 0001. Programmable and explicit policy

Proxy policies often grow beyond static routes to include health checks,
failover, and host-specific rules. A custom configuration language gains
conditionals and special cases as it takes on those jobs. Implicit behavior
keeps common configurations short, but makes it harder to predict what the
proxy will do when it interacts with an existing network.

## Write policy in TypeScript

Kotoconn uses TypeScript so users can express those decisions with ordinary
functions and modules. Policy decides how to route traffic, resolve names,
and manage resources. Policy functions run in one JavaScript context and
can share state. Rust carries traffic and owns protocol state.

The low-level API exposes individual capabilities. The high-level API
composes those same public operations into helpers for common setups. Users
can use a helper as written, adapt it, or build their own.

## Make effects explicit

Routing host traffic into TUN and handling DNS are separate policy decisions.
Policy can install routes into TUN, leave the host's DNS settings in place,
and forward any captured DNS like other traffic. Answering captured DNS
through policy takes a separate routing decision.
