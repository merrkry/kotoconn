# Terminate TUN traffic in user space with flow-owning workers

TUN terminates TCP inside Kotoconn with a fork of smoltcp, so TUN connections follow the same stream contracts, policy and shutdown rules as socket inbounds.

A TUN inbound runs one worker per device queue. Each TCP connection and UDP association belongs to one worker, chosen by a stable flow hash, and packets arriving at another worker are forwarded to the owner. Flow state needs no locks and stays on one thread. On Linux each worker has its own single-threaded runtime, so device I/O and the sockets of its flows share one I/O driver.

Workers drive TCP state machines directly and share payload blocks with application streams. smoltcp's interfaces, socket sets and ring buffers would duplicate storage, copying, packet parsing and scheduling. The fork adds only storage and host-context hooks; scheduling, allocation and pools stay in Kotoconn, which keeps the fork small.

IP fragments are reassembled by the worker that owns their flow, and all incomplete datagrams share one memory budget. Fragments do not bounce between workers, and fragmented traffic cannot exhaust memory.

Stopping a TUN inbound ends its UDP associations while TCP connections drain. A failed worker cancels only its own inbound's connections.
