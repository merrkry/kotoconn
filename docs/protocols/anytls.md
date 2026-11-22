# AnyTLS

Kotoconn follows the reference anytls-go implementation where it differs from
the protocol document. A padding record written after the payload is exhausted
carries a Waste frame whose payload is the full record size, so shaped writes
match those of official clients. [AnyTLS protocol][protocol]

The protocol has no per-stream flow control. When a session's receive budget
is full, Kotoconn stops reading that TLS connection, as the reference does,
instead of closing the stream. A stalled stream therefore delays its siblings
on the same session; official clients open one stream per session at a time.

Like the reference client, clients return a stream once SYN is queued, and a
SYNACK carrying an error closes it. A session returns to the pool only after
SYNACK, and one that leaves a stream unacknowledged for the handshake timeout
is retired; the reference watches only reused sessions, with a shorter
deadline.

Servers acknowledge SYN when they admit the stream, before routing, which the
protocol permits. They reject clients older than protocol version 2.

[protocol]: https://github.com/anytls/anytls-go/blob/main/docs/protocol.md
