# TUN buffering

TUN admits TCP connections and UDP associations without fixed count quotas. Queues constrain accumulated work using local consumption feedback. Transport independence follows [ADR 0005](adr/0005-independent-protocol-admission-and-session-execution.md); ownership and shutdown follow [ADR 0009](adr/0009-independent-tun-connection-drivers.md).

## TCP storage

Our smoltcp fork in `external/smoltcp` retains the TCP state machine, sequence handling, acknowledgments, retransmission and congestion control. Its storage interface accepts Kotoconn's sparse immutable blocks instead of fixed rings. New and idle connections allocate no RX/TX payload capacity in advance. Received segments allocate their payload, including when out of order; holes consume no payload storage. Overlap handling preserves the bytes and logical offsets expected by smoltcp's assembler.

The worker publishes contiguous RX blocks directly to Stream. Consumption returns receive credit and wakes the worker for window updates. Stream submits TX blocks directly to TCP storage; their credit returns only after ACK. Both paths transfer block ownership without a second payload copy. Stream implements cooperative async reads and writes; block exchange uses concurrent queues and atomic counters, while only the worker accesses protocol state.

Each direction starts with a 128 KiB target and adapts to its own completed bytes using the common `Capacity` feedback. Receive completion means application consumption; transmit completion means ACK. Stalled consumers cannot grow the target merely by adding arrivals. Window scale 7 is selected for the fast local TUN leg, giving 128-byte granularity and a negotiated receive range just below 8 MiB. No outbound transport parameters enter this calculation. A peer without window scaling retains the ordinary 65535-byte wire limit.

Lowering a receive target does not retract an advertised window. In-flight segments covered by the old window remain acceptable even when the new target is smaller. Payload storage is released as blocks are consumed or acknowledged, independently of the window target. FIN preserves unread bytes and the other direction; RST or cancellation reports a connection error.

## Pools and packet storage

Each worker shares a payload pool with its Streams. Blocks use power-of-two size classes, at least 256 bytes and at most 64 KiB per block. The free cache retains at most 32 blocks per class; exhaustion allocates another block and never rejects a connection because a cache is empty. Cache bounds limit retained free memory, not live connections. A block returns only when its final immutable view is dropped. Shutdown trims free blocks. Allocation uses ordinary Rust allocation and is not a recoverable process-wide OOM contract.

A worker owns one `PacketArena` for TCP emission. It reserves output allowance before encoding, so rejected packets cannot leave gaps between published views. Small packets share 16 KiB blocks and are charged twice their length; large packets use their own allocation and are charged their length. Descriptor metadata is charged by the queue. Sharing an arena is safe because all of this worker's TCP packets enter the same output queue.

UDP writers own their encoders and share atomic fragment identifiers. Unfragmented UDP is encoded directly into final packet storage. Fragmented UDP first computes the checksum over the full datagram. Linux output still copies into tun-rs GRO scratch storage for coalescing; the scratch belongs to the writer and holds a bounded batch.

## Backlog policy

| Storage | Behavior when its allowance is exhausted |
| --- | --- |
| Cross-worker ingress | Drop new packets |
| TCP RX | Advertise available receive credit; retain already accepted data |
| TCP TX | Apply Stream write backpressure until ACK returns credit |
| TUN transmit queue | TCP connections retry from the worker's blocked list; dedicated UDP writers wait; immediate ACK/reset replies use nonblocking admission |
| UDP association ingress | Drop new datagrams without blocking shared reception |
| UDP GSO segments | Reject aggregates beyond local allowance; drain admitted segments before receiving another aggregate |
| IP reassembly | Reject growth beyond shared allowance before allocation; expiry releases incomplete datagrams |

`Capacity` measures completed bytes over a feedback interval. The target is the largest of the initial allowance, measured consumption rate times a target delay, and the previous target halved per elapsed interval. A smaller queue target constrains new admission without discarding accepted items. An empty packet queue can admit one complete item larger than its target, so a valid datagram remains sendable.

These policies constrain backlog, not total process memory. Flow metadata, application tasks, outbound transports and pool caches still consume resources. Removing fixed per-connection payload allocation improves scaling but does not make unlimited connections cost-free.
