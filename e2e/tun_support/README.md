# TUN test responsibilities

TUN verification has three layers. They share workload identities and payload
rules where useful, but have different completion criteria.

| Layer | Responsibility | Normal use |
| --- | --- | --- |
| Rust tests | Exact parser, reassembly, offload, queue, storage and lifecycle contracts | Every related code change |
| Linux E2E | Real CLI, kernel sockets, TUN routing, repeated traffic and shutdown | Quick profile on pull requests; stress profile for larger changes |
| Benchmark | Verified throughput, latency distributions and resource measurements | Performance work; small pipeline checks in CI |

## Deterministic contracts

`crates/tun/src/tests.rs` covers the wire protocol, TCP state transitions and
storage. MTU boundary tests use 1280, 1500, 9000 and 65535, both IP families,
zero-length UDP, fragment boundaries and maximum supported datagrams.
`workload_tests.rs` interleaves rejected packets with valid reassembly through
one persistent decoder, and checks that inconsistent virtio metadata cannot
bypass transport validation.

The dynamic receive-window regression withholds the handshake ACK and requires
exactly one SYN-ACK before the retransmission deadline. Release churn exposed a
SYN-ACK flood when the larger scaled window repeatedly triggered an update
during the handshake. The regression fails immediately with the old smoltcp
logic; it does not depend on reproducing the later socket reset.

`parallel_tests.rs` supplies explicit receive queues and controlled writers.
It checks cross-worker fragment ownership, blocked-reader independence,
receive failures and forced cancellation. A virtual-clock test repeatedly
keeps one UDP session active while its sibling expires, then reuses the
expired flow. `udp_direct.rs` forces a full output queue and verifies that
empty replies are retained while reverse traffic continues. These tests
observe state or completion instead of waiting for scheduling luck.

The traffic tool has a socket regression for a sender that finishes before
receive-side completion is observed. Missing replies still terminate within
the external receive-drain limit. Real TUN workloads and benchmark experiments
must use the Docker launcher below, including local development.

Fixed repetitions improve coverage of state reuse; they do not prove that all
possible thread schedules were tested. A seed reproduces payloads and offered
traffic plans, not the OS scheduler. Known ordering bugs belong in tests with
controlled I/O or virtual time.

## Protocol expectations

RFC requirements and Kotoconn's supported protocol subset are different
contracts. Parser rejection of an unsupported feature is not evidence that
the feature is forbidden by an RFC. Existing protocol expectations include:

| Expectation | Source | Rust coverage |
| --- | --- | --- |
| IPv4 UDP may omit its checksum; a computed zero checksum is encoded as all ones | [RFC 768](https://www.rfc-editor.org/rfc/rfc768) | UDP checksum and offload tests in `tests.rs` |
| Ordinary IPv6 UDP requires a checksum | [RFC 8200, section 8.1](https://www.rfc-editor.org/rfc/rfc8200#section-8.1) | `udp_zero_checksum_is_only_valid_for_ipv4_and_source_port_may_be_omitted` |
| Overlapping IPv6 fragments cannot produce a reassembled datagram | [RFC 8200, section 4.5](https://www.rfc-editor.org/rfc/rfc8200#section-4.5) | `ipv6_overlaps_poison_the_datagram_but_atomic_fragments_are_independent` |
| Atomic IPv6 fragments are processed independently of matching incomplete assemblies | [RFC 6946, section 4](https://www.rfc-editor.org/rfc/rfc6946#section-4) | The same overlap/atomic-fragment test |

Use packet traces and another implementation during development when resolving
an ambiguous expectation. Resolve disagreements against the applicable RFC and
our documented support scope, then keep the resulting deterministic regression.
The TUN E2E suite does not launch or run a conformance suite against another
proxy. Linux sockets provide its real transport peer. The optional sing-box
1.15 process belongs only to performance comparisons.

## Real-network E2E

Use the [workspace setup](../../docs/development.md#setup), then run:

```sh
pnpm exec nx run e2e:tun
pnpm exec nx run e2e:tun:stress
pnpm exec nx run e2e:tun -- --case mixed-malformed --mtu 9000 --family 6 --repeat 8 --seed 23
pnpm exec nx run e2e:tun -- --case generic-relay
pnpm exec nx run e2e:isolation
```

Nx compiles the Debian-compatible binaries with cargo-zigbuild, then packages
them and the runtime image with Bake. Cargo and BuildKit reuse their artifacts. Bake exports binaries directly into
`target/tun/debug/`, separate from host Cargo artifacts. To build without running
tests, use `pnpm exec nx run docker:build`. Use `--binary` and `--traffic-binary`
to select other binaries compatible with Debian Bookworm.

For optimized-code verification, follow the [release build instructions](../../benchmarks/tun/README.md)
and pass `--binary target/tun/release/kotoconn
--traffic-binary target/tun/release/kotoconn-tun-traffic` to the stress command.

The quick profile repeats 18 workload recipes twice at MTU 1500 and 9000 over
IPv4 and IPv6. It covers one and four connections, persistent TCP transfers,
TCP/UDP churn, sparse activity, upload/download, four concurrent fragmented
UDP flows, datagram boundaries, paced UDP and mixed malformed traffic. Mixed
runs last one second; most other cases complete fixed work counts. All TCP
cases verify server-first traffic, payloads, EOF and a trailer sent after
client half-close.

The stress profile uses all four MTUs, four repetitions, 16 connections in
concurrent cases, more transactions and three-second mixed workloads. A daemon
is reused across repetitions at each MTU so connection and pool reuse remain
observable. The next MTU starts a new daemon. Resource snapshots accompany
each result; RSS is observed rather than asserted to return to an exact value.

Additional cases cover immediate TCP reset, simultaneous IPv4/IPv6 graceful
drain, forced shutdown and cleanup after failed startup. The generic-relay
case sends both transports through a local SOCKS5 inbound in the same daemon,
exercising the ordinary relay path instead of native TUN socket execution.
It requires no separate reference program.

## Mixed and malformed traffic

The independent Python wire encoder constructs valid controls, bad IP/UDP
checksums, truncated headers, bad UDP lengths, invalid TCP flags/offsets and
IPv6 overlapping fragments. It uses AF_PACKET inside the namespace so Linux
does not repair those bytes before delivering them to TUN. The Rust socket
server must receive valid controls and must never receive an invalid payload.
Injection counts are retained by category.

Mixed traffic continues while injection runs. Each bulk, sparse, churn and
paced-UDP flow must complete useful work. Existing queue policy allows ingress
drops under saturation, so paced UDP records every loss rather than asserting
that an overloaded network is lossless. After injection and load end, the same
daemon must pass strict UDP boundary exchanges. Ordinary non-overload cases
require every reply; there is no retry-until-success behavior.

Malformed-input rejection and the precise handling of blocked empty UDP
replies are asserted in the deterministic Rust layer. Black-box E2E cannot
prove that every rejected packet reached every internal worker. Valid controls,
continued traffic and recovery checks establish that the actual kernel/TUN
path is being exercised.

## Isolation and artifacts

Each invocation starts a Docker container with `--network none`, a read-only
root, NET_ADMIN, NET_RAW and `/dev/net/tun`. No host network, D-Bus socket,
`/run` directory or container-engine socket is mounted. Network setup refuses
to run outside this launcher. The existing `docker` command may be provided by
Podman's Docker-compatible interface. Neither path invokes `unshare`.

Source and binaries are mounted read-only, with only this run's unique
artifact directory writable. That directory inherits its host group, and the
container joins that group. Group write permissions keep nested results usable
by both the host runner and rootful Docker without filesystem override capabilities.
The launcher does not inspect or mount host runtime libraries. Supplied binaries
execute only inside the container, including reference version checks.
The launcher records the host source revision and image ID before starting,
so the container does not need access to a worktree's external Git directory.
Docker applies the test sysctls only to the container network. Source
routing includes kernel-generated TCP control packets after application exit.
Traffic invocations use server ports in 12000..19999. Benchmark UDP warmup and
measurement share one port within a sample so live warmup associations do not
consume a second set of outbound ports. Other invocations use separate server
ports. TCP clients defer source-port allocation until connect, so earlier cases' TIME_WAIT sockets do
not exhaust a new case's tuple space. The namespace uses ephemeral ports
20000..65535 and enables timestamp-protected TCP TIME_WAIT reuse for outbound
kernel sockets on its local documentation addresses. Raw injection source ports
22222..22224 are excluded from ephemeral allocation. These settings never
apply to the parent namespace. One invocation supports 8000 traffic processes,
including warmups and recovery checks.

The `/dev/net/tun` character device is shared as a factory. Each container owns
its own nonpersistent `ktest0`; no physical device, host route, host port or
named host network namespace is reserved by these runners. The runtime image
is explicitly built and can be shared by runs.
Each invocation creates a unique child of its output directory, even when two
workspaces pass the same `--output`. Each run records a container ID for cleanup
on completion or interruption. Cleanup never searches for another run's resources.

`check_isolation.py` runs two containers concurrently with different working
directories and the same artifact parent. It verifies distinct namespace IDs
and compares host addresses, rules, routes, TCP settings and available resolved
DNS status before and after. DHCP and router-advertisement countdown fields are excluded from that comparison.
Use `--daemon-cpus` to select one available CPU when exercising a single worker;
the default affinity includes up to two already available CPUs. Affinity does
not reserve those CPUs against another workspace.

Artifacts contain the case ID, seed, exact workload arguments, per-flow
counts and histograms, process resource samples, generated policy, daemon
logs and namespace network snapshots. A failed case marks the run failed and
keeps its first error. Successful later runs do not overwrite its directory.
A launcher killed by SIGKILL cannot execute cleanup code; its `container.cid`
identifies the container for explicit cleanup.

See [benchmark measurement definitions](../../benchmarks/tun/README.md) before
using small CI samples or overloaded UDP completion rates as performance data.
