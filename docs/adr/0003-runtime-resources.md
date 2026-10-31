# Create and close resources at runtime

Policy creates inbounds, dialers, DNS services and DNS listeners at any time: during entry-module evaluation, or later from handlers and timers. Subscription updates, outbound groups and reconfiguration need no daemon restart or separate configuration phase.

The resource manager exists before the entry module runs, and creation is asynchronous. Resources refer to each other by reference, so dialers can share carriers. Creation validates references: every referenced resource must be open. A resource must exist before the resources created on it, such as a carrier before its dialers, so carrier cycles cannot form. A failed creation rejects its promise. Startup finishes once evaluation and the creations it started have settled, and an unhandled rejection before then fails startup, so a forgotten `await` cannot hide a failed listener.

Lifetimes are explicit and follow [ADR 0007](0007-abort-or-drain.md). Policy closes or drains a resource and can wait for its release; garbage collection never closes one. A resource cannot close while an open resource was created on it, such as a dialer that carries another dialer or serves a DNS service: closing it fails instead of cascading. A reference to a closed resource fails the work that uses it and never reaches a resource created later. Races at lifetime boundaries are handled best effort: work caught by a closing resource fails, and native resources are still released.

Daemon shutdown and failed startup drain every open resource under the shutdown deadline, after the shutdown hooks finish. Inbounds and DNS listeners drain first. The dialers and DNS services that serve them drain once those are released and no policy call remains, so accepted traffic and policy calls keep their outbound connections.

Handlers are functions, not resources, and live while something references them. Policy changes behavior at runtime through state that its handlers capture, so there is no API for replacing a handler.

Policy has timers for scheduled work and can probe a dialer for health checks. A probe belongs to its dialer and ends when the dialer closes.

Changing policy code requires a restart, because re-evaluating modules in the running context would retain old module state.
