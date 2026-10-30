# Create and close resources at runtime

Policy creates inbounds, dialers, DNS services and DNS listeners at any time: during entry-module evaluation, or later from handlers and timers. Subscription updates, outbound groups and reconfiguration need no daemon restart or separate configuration phase.

Resources refer to each other by reference, so dialers can share carriers. Creation validates references: every referenced resource and handler must exist and be open. A carrier must exist before the dialers that use it, so carrier cycles cannot form. A failed creation rejects its promise; an unhandled failure during entry-module evaluation fails startup.

Lifetimes are explicit and follow [ADR 0007](0007-abort-or-drain.md). Policy closes or drains a resource and can wait for its completion; garbage collection never closes one. A dialer cannot close while it carries another open dialer. Daemon shutdown drains every open resource under the shutdown deadline. A reference to a closed resource fails the work that uses it and never reaches a resource created later. Races at lifetime boundaries are handled best effort: work caught by a closing resource fails, and native resources are still released.

Handlers are functions, not resources, and live while something references them. Policy changes behavior at runtime through state that its handlers capture, so there is no API for replacing a handler.

Policy has timers for scheduled work and can probe a dialer for health checks. A probe belongs to its dialer and ends when the dialer closes.

Changing policy code requires a restart, because re-evaluating modules in the running context would retain old module state.
