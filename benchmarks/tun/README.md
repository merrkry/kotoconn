# TUN workloads

Build the candidate and the shared traffic tool, then run from any directory:

```sh
cargo build --release -p kotoconn-cli -p kotoconn-tun-traffic
python3 benchmarks/run.py
python3 benchmarks/run.py --profile full
```

The runner needs Linux, Python 3.10+, `unshare`, `iproute2`, `taskset`,
`/dev/net/tun`, and either unprivileged user namespaces or root. It does not
build binaries, install tools, download references, or modify host networking.
Every invocation creates a new network namespace and a unique child of
`target/benchmarks/`, or of the supplied `--output` directory. Devices, ports,
policy rules and child processes belong to that run. Source-address routing
keeps TCP TIME_WAIT acknowledgments on the same path as client traffic.

## Cases

The quick profile covers MTU 1500 and 9000 over IPv4: one and four persistent
TCP connections, connection churn, 64 sparse connections, paced UDP, and mixed
TCP/UDP with malformed input. The full profile adds IPv6, separate upload and
download with one and 16 connections, 256 sparse TCP connections, small UDP,
one-flow UDP, UDP churn, sparse UDP, fragmentation boundaries, and a clean
mixed case. Use `--case NAME`, `--mtu N`, and `--family 4|6` to select cases.
All three options can be repeated. MTU 1280 and 65535 are available explicitly.

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
UDP is exercised without adding a test header.

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
can limit throughput, especially with debug builds. Use the native calibration
and generator CPU measurements before attributing a limit to the proxy. Process
CPU does not include all kernel softirq work. RSS need not return to its initial
value because allocators and payload pools can retain free memory.

## Comparisons

```sh
python3 benchmarks/run.py --baseline /path/to/previous/kotoconn
python3 benchmarks/run.py --sing-box /path/to/sing-box --native-baseline
python3 benchmarks/run.py --case udp-paced --udp-rate 20000 --repetitions 5
```

The sing-box binary must include its `go` TUN stack. The original reference
revision is [68b74f9](https://github.com/SagerNet/sing-box/commit/68b74f9516a2b2e126065e71f31344e2802ce507).
The adapter uses the same
addresses, MTU and workload; references are never downloaded or rebuilt
implicitly. Native calibration bypasses TUN and is omitted for malformed-input
cases. Candidate/reference order alternates between repetitions. Raw samples
and medians remain available; no fixed performance threshold fails shared CI.

`--daemon-cpus` sets only daemon process affinity. By default it uses the first
two CPUs already available to the caller, or one when only one is available.
It does not reserve CPUs or change host CPU settings. Choose separate affinity
sets when comparing concurrent workspaces, or run performance comparisons
sequentially. The generator uses two Tokio worker threads. Small CI samples
validate the measurement pipeline; they are not performance baselines.
