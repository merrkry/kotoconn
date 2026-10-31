# Choose abort or drain for every ending

Abort ends work immediately and discards pending traffic. Drain stops accepting new work at the affected scope and lets accepted traffic finish through forwarding, flush or handoff.

For every EOF, timeout, cancellation, error or protocol close, the code names the affected scope and chooses abort or drain according to the protocol specification or application contract. The scope can be an operation, stream direction, session, resource or the daemon. Drain switches to abort if an error or deadline prevents completion.

A drain deadline is fixed by its owner when drain starts and bounded by its parent's deadline. Activity and repeated requests do not extend it. A close request starts termination; completion separately confirms that resources are released.

Kotoconn chooses:

- TCP EOF drains the exhausted forwarding direction, then shuts down its output; the reverse direction continues. This follows [RFC 9293, section 3.6](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.6).
- Sniff completion, timeout, EOF, limits, or an inspection stop request drain inspection by handing buffered traffic back unchanged and in order. Unsuccessful inspection returns no metadata regardless of cause.
- UDP idle expiry, explicit session cancellation, association loss, and fatal transport errors abort the session, including sniff buffers.
