# Linux TUN inbound

The [example policy](../../packages/api/examples/tun.ts) creates `kototun0` and routes TCP and UDP through a direct outbound. Run it with access to `/dev/net/tun` and `CAP_NET_ADMIN`:

```sh
pnpm exec nx run rust:build
sudo target/x86_64-unknown-linux-gnu/debug/kotoconn run --config packages/api/examples/tun.ts
```

`tun_inbound` takes an unused interface name, an MTU between 1280 and 65535, and an `addresses` array of native IP addresses and prefix lengths. An empty array leaves address assignment to the operator. One IPv4 address and multiple IPv6 addresses are supported. The interface is not persistent. Failed startup and completed shutdown release it. `Daemon::inbound_addresses()` reports its name; `listen_addresses()` reports only socket listeners.

Routing remains explicit. For example, on a Linux gateway with IP forwarding enabled, these rules send traffic arriving on `lan0` through TUN while the daemon's outbound connections use the main routing table:

```sh
ip route add default dev kototun0 table 100
ip -6 route add default dev kototun0 table 100
ip rule add priority 100 iif lan0 lookup 100
ip -6 rule add priority 100 iif lan0 lookup 100
```

For local applications, use socket/connection marks or a dedicated client source address and exclude the daemon. UID-only rules can miss kernel-generated TIME_WAIT acknowledgments because they no longer carry the application's UID. Sending the daemon's own connections back into TUN creates a routing loop. The daemon manages the interface and its addresses; the operator manages these policy rules and their removal.

The adapter proxies unicast TCP and UDP over IPv4 and IPv6. smoltcp handles TCP and provides the wire types and Assembler used for IP fragmentation and reassembly. IPv6 overlapping fragments poison their datagram until expiry; atomic fragments remain independent. IPv6 UDP requires a checksum; IPv4 UDP may omit it. Empty UDP payloads and destination-specific session routing are preserved.

ICMP echo forwarding, multicast, source routing, IPsec and unsupported IPv6 extension chains are filtered. Supported IPv6 options are those accepted by smoltcp's option parser with an action that permits processing to continue. Linux TUN offload is enabled when the kernel supports it. TCP GSO aggregates enter smoltcp as complete large segments with their verified checksum metadata. UDP GSO retains shared payload views with individual datagram boundaries. Output uses vectored TCP/UDP GSO when supported. After policy routing, eligible native sockets execute in the flow's worker; other outbounds use the same chunk and datagram contracts.

For queue ownership, TCP scheduling and shutdown, see [ADR 0009](../../docs/adr/0009-independent-tun-connection-drivers.md). For overload behavior, adaptive queues and packet storage, see [TUN buffering](../../docs/tun-buffering.md).

Run the protocol tests with:

```sh
cargo-zigbuild test --target x86_64-unknown-linux-gnu.2.36 -p kotoconn-tun
```

For Linux E2E tests, follow the [workspace setup](../../docs/development.md#setup), then run `pnpm exec nx run e2e:tun` to build the container artifacts and run the suite.
The runner exercises the real CLI against Linux TCP/UDP sockets, injects malformed
IP packets, and checks graceful and forced shutdown. It needs Docker and
`/dev/net/tun`. Each invocation owns a container with a private network and a
unique artifact directory. It has no host network or D-Bus access. See
[TUN test responsibilities](../../e2e/tun_support/README.md) for the quick/stress
profiles, deterministic tests, mixed malformed traffic and isolation checks.

## smoltcp fork

Cargo patches smoltcp to `external/smoltcp`, based on upstream 0.14.0. The fork
keeps TCP sequence handling, ACKs, retransmission and congestion control, while
adding the interfaces this crate needs:

- `TcpContext` and direct socket driving let the receive worker supply time,
  addresses and link capabilities without a per-connection Interface or device.
- Replaceable TCP byte storage accepts sparse immutable payload blocks. Receive
  targets are independent of allocated bytes, and receive context preserves views
  into owned input frames. Contiguous TX ranges may span multiple blocks.
- `dispatch_scattered` returns headers and logical payload ranges. TUN can retain
  their block views for vectored GSO output without gathering the data first.
- Dynamic receive windows preserve previously advertised space when a target
  falls. Scaling does not create extra credit, and a larger window waits for
  handshake completion instead of repeatedly transmitting SYN-ACK.
- `time_wait_reuse` checks a new SYN against a TIME-WAIT tuple's sequence
  boundaries and returns a safe local ISN. Tests cover old duplicates, wrapping
  sequence numbers and unread FIN payload. The worker retires old application
  I/O before installing a new generation for that tuple.

The ordinary Interface and ring-buffer APIs remain available. Runtime scheduling,
allocation policy, pools and worker ownership stay in Kotoconn. See
[TUN buffering](../../docs/tun-buffering.md) for their implementation.

The fork is excluded from the parent Cargo workspace. Run its tests separately:

```sh
pnpm exec nx run rust:fork-test
```

Git branch and submodule operations are documented in
[external sources](../../external/README.md).
