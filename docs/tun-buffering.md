# TUN buffering

TUN admits TCP connections and UDP associations without fixed count quotas. Queues bound accumulated work using local consumption feedback. Transport ownership and inbound/outbound independence follow [ADR 0005](adr/0005-independent-protocol-admission-and-session-execution.md); worker ownership and shutdown follow [ADR 0009](adr/0009-independent-tun-connection-drivers.md).

## Backlog policy

| Storage | Behavior when its allowance is exhausted |
| --- | --- |
| Cross-worker and per-flow ingress packet queues | Drop new packets so a stalled flow cannot block shared reception |
| TCP application byte queues | Apply backpressure and preserve accepted bytes |
| TUN transmit queue | Dedicated TCP/UDP producers wait; closed-port replies use nonblocking admission |
| UDP GSO segments | Reject an aggregate whose duplicated headers and payload exceed the local allowance; drain admitted segments before receiving another aggregate |
| IP reassembly | Reject storage growth beyond the allowance shared by all workers; expiry releases incomplete datagrams |

[`Capacity`](../crates/protocol/src/queue.rs) measures completed bytes over a feedback interval. The target is the largest of the initial allowance, the observed consumption rate multiplied by a target delay, and the previous target halved per elapsed interval. Arrivals alone cannot grow a stalled queue. A smaller target constrains new admission without discarding accepted data.

Packet queue charges include payload storage and descriptor metadata, including for empty datagrams. An empty queue can admit one complete item larger than its target so a valid datagram remains sendable. Reassembly charges retained storage across workers and samples only successful completions; malformed fragments and expiry cannot increase its allowance. These are backlog policies, not process memory quotas.

The constants live alongside the queue policy. TCP ingress starts with room for two receive-buffer-sized bursts. Reassembly has its own initial allowance. These choices absorb scheduling bursts while each queue adapts to its own consumer.

## TCP working storage

Unmodified smoltcp owns TCP acknowledgments, receive windows, out-of-order data and retransmission. Its contiguous RX/TX buffers stay fixed for each socket's lifetime. [`tcp.rs`](../crates/tun/src/tcp.rs) sets their sizes for the local TCP leg's throughput and feedback delay.

Application bytes use separately allocated chunks in [`BufferedStream`](../crates/protocol/src/stream_buffer.rs). Growth does not move previously buffered payload; consumed chunks are released. These queues absorb application scheduling bursts, but do not enlarge smoltcp's advertised window or retransmission storage. Backpressure reaches the TCP peer when the daemon stops consuming bytes.

## Packet storage

Each TCP driver and UDP writer owns a [`PacketArena`](../crates/tun/src/storage.rs). Small packets share blocks through immutable `Bytes` views; packets larger than half a block allocate their requested size. Each packet stays contiguous for smoltcp and OS I/O. Publishing another view does not copy existing packets, and queued views remain valid after the producer exits.

A retired small-packet block is more than half full. Queue accounting therefore charges small packets twice their length and large packets their length, plus descriptor metadata. FIFO consumption can additionally retain a partial head block and the producer's current tail. Separate arenas prevent independently stalled producers from retaining each other's blocks.

This packing bound requires admission before encoding on paths that can drop packets. The closed-port rejector reserves room for an IP header and the maximum TCP header before asking smoltcp for a reset. Otherwise, dropped responses could leave holes between admitted views and let a few small packets retain many blocks. Established TCP producers wait for output capacity; UDP writers encode datagrams already received from the transmit queue.

[`PacketSend`](../crates/tun/src/endpoint.rs) accepts immutable views. The Linux writer copies them into its reusable GRO scratch buffers, because coalescing needs mutable storage and extra room. Scratch storage is local to the writer and holds one bounded batch. Writer-local UDP encoders share only atomic fragment identifiers.

## Allocation and scheduling

Storage uses ordinary Rust allocation. Queue `reserve` and `try_reserve` operations acquire backlog allowance, not heap memory; allocation failure is outside the recoverable error contract.

Async queue operations and stream reads and writes participate in Tokio cooperative scheduling. This allows reverse traffic and cancellation to progress when another I/O direction stays ready. Synchronous packet processing uses bounded batches.
