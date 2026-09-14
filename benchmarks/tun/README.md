# TUN workloads

Build release binaries in Docker, copy them out without starting the image,
and build the TUN runtime. Run from the repository root:

```sh
docker build -f e2e/Dockerfile --build-arg CARGO_PROFILE=release -t kotoconn-bench:local .
mkdir -p target/tun/release
(
    tun_build=$(docker create kotoconn-bench:local)
    trap 'docker rm -f "$tun_build"' EXIT
    docker cp "$tun_build:/usr/local/bin/kotoconn" target/tun/release/kotoconn
    docker cp "$tun_build:/usr/local/bin/kotoconn-tun-traffic" target/tun/release/kotoconn-tun-traffic
)
docker build -f e2e/tun.Dockerfile -t kotoconn-tun:local .
```

Rebuild and copy the binaries after code changes. They are separate from host
Cargo artifacts. The benchmark defaults to `target/tun/release/`; use `--binary`
and `--traffic-binary` to select other binaries compatible with Debian Bookworm.

```sh
python3 benchmarks/run.py
python3 benchmarks/run.py --profile full
python3 benchmarks/check_udp_churn.py
```

The runner needs Linux, Python 3.10+, Docker, and `/dev/net/tun`.
It starts one container per invocation, with no external network, published
ports, host D-Bus or host network access. The container receives only NET_ADMIN,
NET_RAW and the TUN device. It does not use `unshare` or require host root.
The runtime image is built explicitly; `--container-image` selects its tag.

Each run gets a unique child of `target/benchmarks/`, or of `--output`, mounted
writable as `/artifacts`. Source and selected binaries are read-only. All binaries
execute inside the container, including the reference version check. The container's
ID, image ID, command, source revision and settings are recorded in
`container.json`. Cancellation removes only that run's container.
Source-address routing
keeps TCP TIME_WAIT acknowledgments on the same path as client traffic.
TCP warmup and measurement use separate reserved server ports. Each pure UDP
sample reuses its server port across those two phases. UDP has no FIN, so using
different destinations would retain two association populations and can exhaust
outbound ephemeral ports. Each sample still starts a fresh daemon. Namespace-local
ephemeral-port and TCP TIME_WAIT settings limit interference from earlier TCP traffic; see [isolation details](../../e2e/tun_support/README.md#isolation-and-artifacts).

## Cases

`check_udp_churn.py` runs the actual UDP benchmark phases over IPv4 and IPv6
with only 64 namespace-local ephemeral ports. Each phase completes 4096 strict
request/reply exchanges. It catches the port exhaustion caused by retaining
warmup associations under a different destination, without changing idle
timeouts or allowing packet loss.

The quick profile covers MTU 1500 and 9000 over IPv4: one and four persistent
TCP connections, connection churn, 64 sparse connections, paced UDP, and mixed
TCP/UDP with malformed input. The full profile adds IPv6, separate upload and
download with one and 16 connections, 256 sparse TCP connections, small UDP,
one-flow UDP, UDP churn, sparse UDP, fragmentation boundaries, and a clean
mixed case. Use `--case NAME`, `--mtu N`, and `--family 4|6` to select cases.
All three options can be repeated. MTU 1280 and 65535 are available explicitly.
Repeated MTU/family values are deduplicated in their original order. The runner
rejects matrices needing more than 8000 server ports before starting a container,
including all repetitions, reference samples and warmups in that count.

A persistent TCP connection exchanges independently generated data in each
direction. A transaction completes only after payload validation and a server
acknowledgment. Bulk transactions contain 1 MiB plus one byte per enabled
direction. Churn creates a new connection for each 64-byte transaction; sparse
connections retain their sockets and schedule 64-byte requests every 10 ms.
Each flow completes at least one operation, even in a short diagnostic run.
TCP transactions are request/reply workloads, not an unframed iperf wire-rate
measurement. Their acknowledgment and validation costs are intentional.
Comparison workloads exchange their final application trailer before sending
FIN, using `--close-mode exchange` for every implementation. This accommodates
references that do not preserve a reply after client FIN. The E2E profile keeps
the stricter `half-close` mode and verifies the trailer after FIN.

Paced UDP uses independent send and receive work. `--udp-rate` is the offered
number of datagrams per second **per UDP flow**. The default is 10000; sweep
this value to locate saturation. Replies are verified by flow and sequence.
Loss, duplicate replies, reordering and a sample of missing sequence numbers
are reported. Every flow must still complete an operation. Boundary and
ordinary request/reply cases require every datagram to return. Zero-length
UDP is exercised without adding a test header. UDP churn counts new client
sockets, not necessarily new proxy associations: Linux may reuse a source port
and its existing association, including one created during warmup.

The Linux echo server receives and sends up to 32 datagrams per syscall and
requests a 1 MiB socket receive buffer. Batching reduces server work at high
packet rates. The explicit buffer prevents unusually large host defaults from
turning server overload into hundreds of milliseconds of queued replies.
This does not remove overload: loss remains visible and must be read alongside
latency. No host socket settings are changed.

Use `--udp-echo-batch 1` for the ordinary receive/send path, or
`--udp-server-receive-buffer 0` to inherit the host buffer. Both parameters apply
to every workload, including mixed traffic and warmup. The traffic result records
the request and the effective SO_RCVBUF value, including kernel adjustment or
clamping. Compare candidates with the same settings; results made with different
echo configurations are separate experiments.

Mixed cases use one bulk TCP, one sparse TCP, one churn TCP and one paced UDP
flow per group of four. Malformed cases inject a named, deterministic corpus
every sampling interval, alongside valid raw-packet controls that must reach
the server. Corrupt data reaching the server fails the sample. Paced UDP loss
under load is allowed and reported; it is not retried or hidden.

## Measurement

Each sample starts a fresh daemon and runs a separate warmup workload. The
traffic tool binds its servers before reporting readiness, then waits for a
start command. The runner snapshots resource and network counters, starts the
workload, and snapshots them again after verified completion. The generator
stays alive until the final resource snapshot. Throughput and CPU use this
same complete-work window, including connection setup, final operations and
receive drain. There is no omitted interval in the byte denominator.

UDP receive drain can take up to two seconds after the sender finishes when
replies are missing. This time is included in the reported completion rate;
`duration` is the offered-load duration, not a promise that every sample's
wall time is identical. Read the loss counts and latency distributions with
the completion rate when comparing overloaded cases.

Results include:

- Confirmed bidirectional payload bytes/s, operations/s and connections/s.
  TCP upload counts bytes acknowledged by the test server, download counts
  bytes verified by the client, and duplex counts both. UDP counts both legs
  only for verified replies. Protocol headers and malformed bytes are excluded.
- TCP connection time and time to the server-first banner; operation RTT
  distributions grouped by workload kind. Mixed bulk-transfer latency never
  gets merged with sparse-request latency.
- Scheduled latency for paced UDP and sparse TCP, measured from the intended
  send time through reply completion. This includes load-generator delay.
  Ordinary operation RTT starts at the actual request attempt.
- Three-significant-digit HDR histogram buckets, sample counts, p50/p95/p99 and
  maximum latency per flow. Aggregation merges bucket counts, not percentiles.
- Daemon and generator user/system CPU, observed RSS/PSS peaks, process lifetime
  RSS high-water marks, thread and FD counts, and 100 ms resource samples.
  PSS is null when the OS denies access. Lifetime peaks can include warmup.
- Namespace TUN counters, socket state, IPv4/IPv6 protocol counters, process logs,
  binary hashes, source revision, smoltcp revision and CPU affinity.

`results.json` contains all sample summaries and throughput comparisons.
`samples.csv` has one row per completed sample. Individual directories retain
warmup/measurement results, histograms, generated policy and daemon logs.
Failures remain failures and retain their artifacts. Repetitions are independent
samples; a successful repetition does not replace an earlier failure.

These are loopback application measurements. Payload generation and verification
can limit throughput, especially with debug builds. Inspect generator CPU
measurements before attributing a limit to the proxy. Process CPU does not
include all kernel softirq work. RSS need not return to its initial
value because allocators and payload pools can retain free memory.

## Comparisons

```sh
python3 benchmarks/run.py --sing-box /path/to/sing-box-1.15
python3 benchmarks/run.py --case udp-paced --udp-rate 20000 --repetitions 5
```

The only reference is sing-box 1.15 with its go TUN stack. The runner checks
its version before starting a workload and records the full version output
and binary hash. The verified release is
[1.15.0-alpha.3](https://github.com/SagerNet/sing-box/releases/tag/v1.15.0-alpha.3).
Reference binaries are supplied by the caller, never downloaded or rebuilt
implicitly. System DNS configuration is disabled in the reference. Collect a
reference baseline deliberately and retain its artifacts; ordinary optimization
runs should omit `--sing-box` instead of collecting that baseline again. Omitting `--sing-box` runs only Kotoconn.
The reference logs warnings and errors. Readiness observes its TUN device,
so per-connection INFO logging does not enter churn measurements.

Both implementations receive the same addresses, MTU, payload seed and workload.
The reference runs only clean traffic. `mixed-malformed` measures Kotoconn alone;
`mixed-clean` in the full profile compares the same flow mix without injection.
This keeps differences in malformed-input policy out of performance comparisons.
Their order alternates between repetitions. Raw samples and medians remain
available; no fixed performance threshold fails shared CI. Once measurement
and payload validation complete, the reference process is killed and its TUN
release is checked. Reference graceful-shutdown behavior is not an E2E contract
of this repository and does not enter the measurement window.

`--daemon-cpus` sets only daemon process affinity. By default it uses the first
two CPUs already available to the caller, or one when only one is available.
It does not reserve CPUs or change host CPU settings. Choose separate affinity
sets when comparing concurrent workspaces, or run performance comparisons
sequentially. The generator uses four Tokio worker threads by default;
`--traffic-workers N` changes this independently of the daemon. It inherits the
runner's CPU affinity, recorded in metadata. For example, on a machine with six
available CPUs, run `taskset -c 2-5 python3 benchmarks/run.py --daemon-cpus 0,1`
to give the generator four CPUs separate from the daemon. Extra threads without
CPU time do not establish generator capacity. Small CI samples validate the
measurement pipeline; they are not performance baselines.
