# Choose abort or drain for every ending

Abort ends work immediately and discards pending traffic by dropping its owning future. Drain stops accepting new work at the affected scope and preserves accepted traffic through forwarding, flush, or handoff before ending.

For every EOF, timeout, cancellation, error, or protocol close, specify the affected scope and choose abort or drain according to the protocol specification or application contract. The scope can be an operation, stream direction, session, or daemon. Drain switches to abort if an error or deadline prevents completion.

A configured drain deadline is fixed by its owner at drain start and bounded by its parent's deadline. Activity and repeated requests do not extend it. Expiry aborts pending work; completion still waits for resource release.

Kotoconn chooses:

- TCP EOF drains the exhausted forwarding direction, then shuts down its output; the reverse direction continues. This follows [RFC 9293, section 3.6](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.6).
- Sniff completion, timeout, EOF, limits, or an inspection stop request drain inspection by handing buffered traffic back unchanged and in order. Unsuccessful inspection returns no metadata regardless of cause.
- UDP idle expiry, explicit session cancellation, association loss, and fatal transport errors abort the session, including sniff buffers. The session has ended, so no sniff consumer remains and no flush phase is added.

Close requests termination; completion confirms resource release. Session `Scope.close()` requests abort and propagates cancellation; `Scope.wait()` observes completion. Each UDP session has one idle owner shared across inbound and routing layers.

For daemon shutdown and protocol-specific closure, see [ADR 0005](0005-independent-protocol-admission-and-session-execution.md). For UDP session boundaries and protocol state retention, see [ADR 0007](0007-destination-specific-udp-sessions.md).
