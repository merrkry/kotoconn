# TUN buffering

TUN admits TCP connections and UDP associations without fixed count quotas. Queues constrain accumulated work using local consumption feedback. Transport independence follows [ADR 0005](adr/0005-independent-protocol-admission-and-session-execution.md); ownership and shutdown follow [ADR 0009](adr/0009-independent-tun-connection-drivers.md).

## TCP storage

Our smoltcp fork in `external/smoltcp` retains the TCP state machine, sequence handling, acknowledgments, retransmission and congestion control. Its storage interface accepts Kotoconn's sparse immutable blocks instead of fixed rings. New and idle connections allocate no RX/TX payload capacity in advance. Received segments retain views of the input frame, including when out of order; holes consume no payload storage. Reassembled datagrams have separate storage and copy their accepted TCP payload once. Overlap handling preserves the bytes and logical offsets expected by smoltcp's assembler.

The worker publishes contiguous RX blocks directly to Stream. Consumption returns receive credit and wakes the worker for window updates. Stream submits TX blocks directly to TCP storage; their credit returns only after ACK. Both paths transfer block ownership without a second byte queue. The fork dispatches a logical payload range. GSO output retains its block views, including ranges that cross block boundaries. Ordinary wire output copies them once into the final packet. Stream implements cooperative async reads and writes; block exchange uses concurrent queues and atomic counters, while only the worker accesses protocol state.

Each direction starts with a 128 KiB target and adapts to its own completed bytes using the common `Capacity` feedback. Receive completion means application consumption; transmit completion means ACK. Stalled consumers cannot grow the target merely by adding arrivals. Window scale 7 is selected for the fast local TUN leg, giving 128-byte granularity and a negotiated receive range just below 8 MiB. No outbound transport parameters enter this calculation. A peer without window scaling retains the ordinary 65535-byte wire limit.

Lowering a receive target preserves the largest promised receive right edge in byte precision. In-flight segments covered by the old window remain acceptable even when the new target is smaller. The scaled wire field rounds down, so its right edge may differ by at most 127 bytes; that rounding never grants new credit to a stalled reader. Payload storage is released as blocks are consumed or acknowledged, independently of the window target. FIN preserves unread bytes and the other direction; RST or cancellation reports a connection error.

## Pools and packet storage

Each worker shares a payload pool with its Streams. Blocks use power-of-two size classes, at least 256 bytes and at most 128 KiB per block. The free cache retains at most 32 blocks per class; exhaustion allocates another block and never rejects a connection because a cache is empty. Cache bounds limit retained free memory, not live connections. A block returns only when its final immutable view is dropped. Native reads compact short messages before publication so a small datagram does not retain a maximum-size receive allocation. Publishing a shared view still allocates its ownership descriptor. Shutdown trims free blocks. Allocation uses ordinary Rust allocation and is not a recoverable process-wide OOM contract.

A worker owns one `PacketArena` for TCP emission. It reserves output allowance before encoding, so rejected packets cannot leave gaps between published views. Small packets share 16 KiB blocks and are charged twice their length; large packets use the same power-of-two pool as payloads and are also charged twice their length. Descriptor metadata is charged by the queue. Sharing an arena is safe because all of this worker's TCP packets enter the same output queue.

UDP writers own their encoders and share atomic fragment identifiers. Unfragmented UDP is encoded directly into final packet storage. Fragmented UDP first computes the checksum over the full datagram. When the Linux writer supports TCP GSO, smoltcp emits an aggregate and its segment size directly. A vectored TUN write passes the virtio header, IP/TCP header and immutable payload views. Eligible UDP datagrams similarly use header and payload vectors, with UDP GSO for adjacent equal-size datagrams belonging to the same flow. Oversized UDP datagrams retain the fragmentation path. Ordinary packets use vectored checksum-free virtio headers without GRO scratch storage.

## Chunk I/O and direct execution

Protocol streams can return owned chunks and accept a prefix of an owned chunk. Acceptance returns the source's receive credit; TCP TX credit still waits for its own peer's ACK. HTTP retains handshake read-ahead as a prefix, SOCKS preserves the underlying chunk interface, and Shadowsocks keeps compatibility buffering at its codec boundary. A single relay future polls both directions without generic stream splitting or a shared stream lock.

Native outbound TCP sockets use TCP_NODELAY, and TUN TCP disables Nagle. Small request fragments, replies and trailers are sent without waiting for an earlier segment's acknowledgment. TCP receive credit, retransmission and delayed ACK handling remain independent of these send settings.

Eager setup and cancellation run in owner tasks that carry no payload. Cancellation revokes established I/O even when its handle is idle. A resource temporarily held by a synchronous poll remains included in scope completion until that poll releases it.

After the daemon routes a TUN flow, eligible native TCP and UDP transports move to that flow's worker. The worker polls socket readiness, TCP state and packet I/O directly. TCP retains half-close behavior and reports transferred byte counts to the session. UDP transfers its existing ingress queue before accepting new direct input; partial socket sends remove only the accepted prefix. Daemon routing, registration, carrier cancellation and idle expiry remain active. Encoded transports continue through the ordinary chunk or packet path.

Packet queues reserve bytes atomically and consume batches. Worker-local direct UDP backlog uses ordinary fields and a deque. Native Linux UDP uses `recvmmsg`/`sendmmsg`, UDP GRO metadata and vectored `UDP_SEGMENT` output where supported. An unsupported segmentation attempt falls back only for its unaccepted datagrams. Portable socket I/O keeps the same datagram boundaries. Successful forwarding records activity atomically per batch; idle timers check the latest activity when they expire.

## Backlog policy

| Storage | Behavior when its allowance is exhausted |
| --- | --- |
| Cross-worker ingress | Drop new packets |
| TCP RX | Advertise available receive credit; retain already accepted data |
| TCP TX | Apply Stream write backpressure until ACK returns credit |
| TUN transmit queue | TCP connections retry from the worker's blocked list; ordinary UDP writers wait; direct UDP retains one reply batch and retries from the worker's blocked list; immediate ACK/reset replies use nonblocking admission |
| UDP association ingress | Drop new datagrams without blocking shared reception |
| UDP GSO segments | Share one receive allocation; apply per-datagram admission before queueing views |
| IP reassembly | Reject growth beyond shared allowance before allocation; expiry releases incomplete datagrams |

`Capacity` measures completed bytes over a feedback interval. The target is the largest of the initial allowance, measured consumption rate times a target delay, and the previous target halved per elapsed interval. A smaller queue target constrains new admission without discarding accepted items. An empty packet queue can admit one complete item larger than its target, so a valid datagram remains sendable.

These policies constrain backlog, not total process memory. Flow metadata, application tasks, outbound transports and pool caches still consume resources. Removing fixed per-connection payload allocation improves scaling but does not make unlimited connections cost-free.
