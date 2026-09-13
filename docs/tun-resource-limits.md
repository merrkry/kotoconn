# TUN resource-limit assessment

The current 256 TCP / 128 UDP limits are too restrictive as a general transparent-proxy policy. They protect an implementation that allocates substantial state per flow. The parallel-worker change preserves them across the whole inbound; it does not multiply them by CPU count. Replacing these limits needs memory accounting and admission policy, not just a larger constant.

## What NICs and L3 VPNs limit

| Component | Resource being limited | Behavior under pressure |
| --- | --- | --- |
| Ordinary NIC and Linux receive path | Descriptors, packet queues and processing capacity; RSS distributes flows without allocating a TCP endpoint for each flow | Queue exhaustion loses packets. Optional RPS Flow Limit drops packets from dominant flows earlier when a CPU backlog is congested. It does not impose a fixed count of established TCP connections. |
| WireGuard | Peer/key state and staged/crypto packet queues | The transmit path drops the oldest staged packets when a peer queue exceeds its threshold. A missing route/peer has a separate error path. Inner TCP connections do not each become WireGuard TCP sockets. |
| OpenVPN | VPN clients and internal routes, among other resources | `max-clients` limits concurrent VPN clients; `max-routes-per-client` limits internal route entries. Its default of 256 routes is not a limit of 256 TCP connections inside a tunnel. |
| Linux conntrack / stateful NAT | Tracked flows, with configurable `nf_conntrack_max` | Allocation above the limit attempts early eviction. If no entry can be freed, the new packet is dropped and a rate-limited warning is emitted. |
| Linux TCP endpoints | Socket memory, incomplete handshakes, listen queues and process resources | Receive buffers can grow with demand. Aggregate TCP memory has pressure thresholds; SYN backlog protection is separate from established socket memory. |

Sources inspected on 2026-09-13: [Linux receive scaling](https://docs.kernel.org/networking/scaling.html), [WireGuard transmit implementation](https://git.zx2c4.com/wireguard-linux/tree/drivers/net/wireguard/device.c), [OpenVPN 2.6 manual](https://openvpn.net/community-docs/community-articles/openvpn-2-6-manual.html), [conntrack limits](https://docs.kernel.org/networking/nf_conntrack-sysctl.html), [conntrack allocation](https://github.com/torvalds/linux/blob/master/net/netfilter/nf_conntrack_core.c), and [Linux TCP memory and SYN backlog controls](https://docs.kernel.org/networking/ip-sysctl.html). These are examples of common implementations, not a claim that every NIC or VPN appliance has identical limits. Hardware flow-offload tables may have their own capacity limits; those do not define the capacity of ordinary software forwarding.

## Current Kotoconn behavior

`worker.rs` counts TCP entries from initial SYN through driver cleanup. With 256 entries, another new SYN is silently dropped: no admission queue and no immediate RST. The client can retransmit and may connect once capacity is released, or eventually time out. Packets for existing entries continue through their normal queues. UDP counts source/destination address-and-port pairs, so one application socket talking to many destinations can consume multiple associations. At 128 entries, a packet for a new pair is dropped without ICMP; idle expiry or handler completion frees the entry.

The two 8 MiB ingress budgets account for queued TCP frames and retained UDP payloads. They do not include smoltcp socket buffers, outbound kernel sockets, all task state or every transmit buffer. Cross-worker transit has its own 8 MiB budget. Per-worker shutdown diagnostics count admission and ingress byte-budget rejections in `capacity_drops`, and cross-worker queue/byte-budget rejections in `forwarding_drops`. Per-flow channel rejections are not included. These shutdown summaries are not yet a live resource-pressure interface.

Each admitted TCP flow currently allocates a 1 MiB smoltcp receive buffer, a 64 KiB send buffer, and Tokio duplex storage, in addition to tasks, queues and the outbound connection. Ten thousand flows would request about 9.8 GiB for receive-buffer storage alone. Resident memory depends on allocator behavior and which pages traffic touches, so that number is not an RSS measurement. A larger connection cap without changing allocation policy is not a sensible default.

## Recommended policy

Use an explicit, configurable resource budget as the normal admission constraint. Derive an initial budget from the effective host/container memory limit with headroom for the rest of the application and kernel sockets; do not let every inbound independently claim the entire available memory. Account for connection state and socket-buffer capacity before allocation. Keep an optional operator-specified connection cap for administrative policy, rather than treating 256/128 as network semantics.

Separate incomplete TCP handshakes from established connections. New handshakes should start with small bounded state and have their own count/rate protection. Established connections should begin with modest buffers and grow with measured demand while budget remains. This requires support for preserving buffered bytes and TCP window invariants during resizing; the current fixed smoltcp buffers cannot be called adaptive merely by changing their initial size. A transition to larger windows also needs correct window-scale negotiation from the initial handshake.

Under temporary memory pressure, preserve established TCP state, limit further buffer growth and allow TCP flow control to slow senders. If even minimal state cannot be admitted, drop the new SYN and count the reason. An immediate RST turns transient resource pressure into a definitive connection failure; it should be an explicit policy if offered. For UDP, expire idle associations, bound queued bytes and drop packets that cannot be accommodated. Evicting an active association changes outbound/session state and should not happen merely because its table is full. Do not generate ICMP port-unreachable for a local capacity shortage.

Keep reassembly storage and worker queues bounded independently of connection admission. Queue overflow should drop a packet without blocking unrelated flows. Expose current usage, high-water marks and drops by reason, with rate-limited logs rather than a log line per dropped packet.

Before selecting defaults, measure idle and active TCP memory, UDP association cost, handshake churn, and 1k/10k mixed-flow workloads under several container memory limits. Check p99 connection latency, loss/retransmission, memory use and whether existing sessions survive admission pressure. Throughput benchmarks with 1–64 streams establish worker scaling; they do not establish a safe high-connection default. The recommended resource policy is follow-up work, not implemented by the worker rewrite.
