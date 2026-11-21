# AnyTLS

Kotoconn follows the reference anytls-go implementation where it differs from
the protocol document. A padding record written after the payload is exhausted
carries a Waste frame whose payload is the full record size, so shaped writes
match those of official clients. [AnyTLS protocol][protocol]

The protocol has no per-stream flow control. When a session's receive budget
is full, Kotoconn stops reading that TLS connection, as the reference does,
instead of closing the stream. A stalled stream therefore delays its siblings
on the same session; official clients open one stream per session at a time.

Clients report a stream as connected only after its SYNACK. The reference
server sends SYNACK once its outbound connection succeeds, so its connection
failures reach the caller as connect errors. This costs one round trip per
stream; the reference client returns the stream before SYNACK.

Servers acknowledge SYN when they admit the stream, before routing, which the
protocol permits. They reject clients older than protocol version 2.

[protocol]: https://github.com/anytls/anytls-go/blob/main/docs/protocol.md
