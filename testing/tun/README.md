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
the receive side observes completion. Missing UDP replies still terminate
within the external receive-drain limit. It uses ephemeral sockets and can
run with the workspace tests inside a namespace:

```sh
unshare --user --map-root-user --net sh -c 'ip link set lo up; cargo test --workspace'
```

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

```sh
cargo build -p kotoconn-cli -p kotoconn-tun-traffic
python3 e2e/tun.py
python3 e2e/tun.py --profile stress
python3 e2e/tun.py --case mixed-malformed --mtu 9000 --family 6 --repeat 8 --seed 23
python3 e2e/tun.py --case generic-relay
python3 testing/tun/check_isolation.py
```

For optimized-code verification, build with `cargo build --release -p kotoconn-cli
-p kotoconn-tun-traffic` and pass `--binary target/release/kotoconn
--traffic-binary target/release/kotoconn-tun-traffic` to the stress command.

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

Both Python entry points re-execute under `unshare --net`, adding a user
namespace when the caller is not root. Network setup refuses to run unless the
current network namespace differs from the recorded parent. They change only
namespace-local addresses, routes, policy rules and IPv4 settings. Source
routing includes kernel-generated TCP control packets after application exit.

The `/dev/net/tun` character device is shared as a factory. Each namespace owns
its own nonpersistent `ktest0`; no physical device, host route, host port,
global image tag or named host network namespace is reserved by these runners.
Each invocation creates a unique child of its output directory, even when two
workspaces pass the same `--output`. Child processes have a dedicated process
group which is terminated on completion or interruption. No cleanup command
searches for or deletes another run's resources.

`check_isolation.py` runs two suites concurrently with different working
directories and the same artifact parent. It verifies distinct namespace IDs
and compares the parent's addresses, rules and routes before and after. DHCP
and router-advertisement countdown fields are excluded from that comparison.
Use `--daemon-cpus` to select one available CPU when exercising a single worker;
the default affinity includes up to two already available CPUs. Affinity does
not reserve those CPUs against another workspace.

Artifacts contain the case ID, seed, exact workload arguments, per-flow
counts and histograms, process resource samples, generated policy, daemon
logs and namespace network snapshots. A failed case marks the run failed and
keeps its first error. Successful later runs do not overwrite its directory.
A process killed by SIGKILL cannot execute cleanup code; any surviving child
processes can be identified by the run's command line and process group.

See [benchmark measurement definitions](../../benchmarks/tun/README.md) before
using small CI samples or overloaded UDP completion rates as performance data.
