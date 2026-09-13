# Linux TUN inbound

The [example policy](../../packages/api/examples/tun.ts) creates `kototun0` and routes TCP and UDP through a direct outbound. Run it with access to `/dev/net/tun` and `CAP_NET_ADMIN`:

```sh
cargo build -p kotoconn-cli
sudo target/debug/kotoconn run --config packages/api/examples/tun.ts
```

`tun_inbound` takes an unused interface name, an MTU between 1280 and 65535, and an `addresses` array of native IP addresses and prefix lengths. An empty array leaves address assignment to the operator. One IPv4 address and multiple IPv6 addresses are supported. The interface is not persistent. Failed startup and completed shutdown release it. `Daemon::inbound_addresses()` reports its name; `listen_addresses()` reports only socket listeners.

Routing remains explicit. For example, on a Linux gateway with IP forwarding enabled, these rules send traffic arriving on `lan0` through TUN while the daemon's outbound connections use the main routing table:

```sh
ip route add default dev kototun0 table 100
ip -6 route add default dev kototun0 table 100
ip rule add priority 100 iif lan0 lookup 100
ip -6 rule add priority 100 iif lan0 lookup 100
```

For local applications, select client UIDs or firewall marks and exclude the daemon. Sending the daemon's own connections back into TUN creates a routing loop. The daemon manages the interface and its addresses; the operator manages these policy rules and their removal.

The adapter proxies unicast TCP and UDP over IPv4 and IPv6. TCP admission, retransmission, flow control, congestion control, FIN and RST use smoltcp. IPv4 reassembly and fragmentation also use smoltcp. One endpoint writer owns outgoing fragment identifiers across UDP associations. The IPv6 adapter uses smoltcp's wire types and Assembler, rejects overlapping fragments for their entire reassembly lifetime, and treats atomic fragments independently. IPv6 UDP requires a checksum; IPv4 UDP may omit it. Empty UDP payloads and destination-specific session routing are preserved.

ICMP echo forwarding, multicast, source routing, IPsec and unsupported IPv6 extension chains are filtered. Supported IPv6 options are those accepted by smoltcp's option parser with an action that permits processing to continue. Linux TUN offload is enabled when the kernel supports it. TCP GSO aggregates enter smoltcp as complete large segments after checksum normalization; UDP GSO is split into individual datagrams. Outgoing packets are coalesced with tun-rs GRO. The writer batches only packets already available and reuses up to 64 large buffers, growing the pool on demand. The number of TUN queues follows the available CPU count, up to four. All queues share one endpoint and writer, so connection limits, packet validation and fragment identifiers remain common. Transparent socket interception and automatic default-route management are not enabled.

Each inbound admits at most 256 TCP connections and 128 UDP associations. TCP and UDP input each have an 8 MiB budget; UDP payload ownership retains credit through routing queues. Each TCP connection has a 1 MiB receive buffer and 64 KiB send and duplex buffers. Its ingress queue holds two receive windows of MTU-sized packets, allocated on demand; the shared ingress budget still bounds queued bytes. The smoltcp device does not clamp the advertised receive window to its temporary egress queue capacity. Full ingress queues drop new packets. Incomplete TCP handshakes expire after 30 seconds; pending TCP transmissions have a 120-second user timeout. IP reassembly expires after 60 seconds. UDP uses the configured `udp_idle_timeout` independently of TCP.

Stopping admission closes UDP associations. Established TCP connections retain TUN I/O while draining. The daemon's shutdown deadline cancels remaining work and releases the device. [ADR 0009](../../docs/adr/0009-independent-tun-connection-drivers.md) describes the driver ownership.

Run the protocol tests and isolated Linux E2E tests with:

```sh
cargo test -p kotoconn-tun
cargo build -p kotoconn-cli
python3 e2e/tun.py
```

The E2E runner creates a network namespace, exercises the real CLI against Linux TCP/UDP sockets, injects malformed IP packets, and checks graceful and forced shutdown. It needs `unshare`, `iproute2`, and either unprivileged user namespaces or root. Logs and generated policy files are retained under `target/`.
