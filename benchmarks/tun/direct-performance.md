# Direct TUN forwarding performance

Measurements on 2026-09-26 cover the release build at `717c213c49b98978a59896cf21eaa0895883816c`.
Concurrent TCP throughput exceeds the sing-box reference in these samples, with
less resident memory. The overall target is not yet met. Single-flow TCP at MTU
9000, several UDP CPU and latency results, and UDP churn memory still trail the
reference. This shared host has substantial scheduling noise; three short samples
per case do not establish a stable capacity limit.

## Changes measured

Linux TUN queues now own their socket I/O runtimes. Direct TCP gathers upload
blocks into vectored writes and shares receive scratch per executor thread.
Bounded pool caches and keyed flow hashes reduce allocation and lookup overhead.

Direct UDP opens native packet I/O without an outbound relay queue or handoff
task. Receive batches remain together through the TUN transmit queue. Smaller
supervision futures reduce memory retained per association. Before dropping new
input, a full direct backlog tries to send its pending prefix. Reassembly pressure
wakes workers to reclaim incomplete datagrams, while preserving poisoned IPv6
identities and rejecting stale fragment bindings.

Policy still runs before native handoff. The packet interface remains independent
of Linux device creation, and the handoff boundary leaves room for sniffing before
outbound selection. See [the execution design](../../docs/adr/0009-independent-tun-connection-drivers.md)
and [buffer ownership and limits](../../docs/tun-buffering.md).

## Setup and interpretation

- Linux 6.18.52, x86-64, AMD EPYC 9634, Rust 1.98.1 release profile.
- Private container network. Proxy affinity CPUs 0 and 1; generator affinity CPUs
  2 through 5, four generator workers. Affinity does not reserve host CPUs.
- Official [sing-box v1.15.0-alpha.8](https://github.com/SagerNet/sing-box/releases/tag/v1.15.0-alpha.8),
  go1.26.8, `stack: go`, multi-queue enabled, DNS disabled, direct outbound.
- IPv4 performance samples at MTU 1500 and 9000. Three repetitions, three seconds
  of offered load, no warmup, a fresh daemon per sample, alternating execution order.
- Paced UDP offers 20,000 datagrams/s per flow. Echo batch 32, receive-buffer request
  1 MiB, effective buffer 2 MiB. Churn uses eight concurrent clients and 1200-byte messages.

Every paired table cell is **Kotoconn / sing-box**. Throughput, process CPU per
confirmed byte, and observed peak RSS are sample medians. Throughput counts
verified application bytes in both directions, excluding headers. CPU includes
process user and system time but excludes kernel work charged elsewhere. RSS is
sampled during the workload, not an allocation total.

Loss divides summed lost datagrams by summed sent datagrams. p99 merges the raw
histogram counts with `measurement.distribution`; it never averages percentiles.
TCP p99 measures a bulk operation, including its roughly 1 MiB payload, rather
than bare packet RTT. UDP p99 covers returned replies and cannot describe lost
requests. Paced UDP includes up to two seconds of receive drain in its throughput
window. Even a few lost replies can therefore lower reported Gbit/s sharply.

## TCP

| MTU | Workload | Gbit/s | CPU ns/B | RSS MiB | Operation p99 us |
| --- | --- | ---: | ---: | ---: | ---: |
| 1500 | Duplex, 1 flow | 6.46 / 6.05 | 0.577 / 0.653 | 12.6 / 67.8 | 21471 / 28127 |
| 1500 | Upload, 16 flows | 25.98 / 22.31 | 0.613 / 0.592 | 15.0 / 65.8 | 9335 / 15607 |
| 1500 | Download, 16 flows | 30.42 / 25.49 | 0.487 / 0.597 | 15.9 / 95.7 | 8775 / 14935 |
| 9000 | Duplex, 1 flow | 8.42 / 10.38 | 0.516 / 0.508 | 12.7 / 66.8 | 7731 / 4591 |
| 9000 | Upload, 16 flows | 24.05 / 22.13 | 0.587 / 0.595 | 14.7 / 65.2 | 13967 / 17039 |
| 9000 | Download, 16 flows | 25.34 / 24.10 | 0.498 / 0.600 | 15.3 / 93.2 | 14159 / 14327 |

At MTU 1500, 16-flow upload and download medians exceed the reference by 16% and
19%. Download CPU per byte is 18% lower. Across these TCP workloads, Kotoconn uses
12.6 to 15.9 MiB RSS versus 65.2 to 95.7 MiB. Single-flow MTU 9000 throughput is
19% lower, and its p99 is higher.

## Paced UDP

| MTU | Datagram and flows | Gbit/s | CPU ns/B | RSS MiB | Loss % | Reply p99 us |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1500 | 64 B, 1 flow | 0.020 / 0.020 | 76.85 / 78.14 | 13.6 / 55.9 | 0.000 / 0.000 | 457 / 451 |
| 1500 | 1200 B, 1 flow | 0.382 / 0.382 | 4.51 / 4.38 | 13.8 / 56.8 | 0.000 / 0.000 | 539 / 614 |
| 1500 | 1200 B, 4 flows | 1.517 / 1.522 | 4.50 / 4.06 | 16.3 / 58.0 | 0.004 / 0.000 | 2383 / 2513 |
| 1500 | 8192 B, 1 flow | 1.545 / 1.495 | 2.82 / 3.22 | 17.1 / 67.2 | 1.329 / 5.076 | 5919 / 6695 |
| 1500 | 8192 B, 4 flows | 1.300 / 1.399 | 5.81 / 3.51 | 22.1 / 68.9 | 74.538 / 77.694 | 11055 / 9127 |
| 9000 | 64 B, 1 flow | 0.020 / 0.020 | 75.55 / 80.75 | 13.2 / 56.7 | 0.000 / 0.000 | 6163 / 4191 |
| 9000 | 1200 B, 1 flow | 0.382 / 0.382 | 4.59 / 4.45 | 13.3 / 56.7 | 0.011 / 0.000 | 11743 / 11679 |
| 9000 | 1200 B, 4 flows | 0.912 / 1.518 | 4.07 / 4.00 | 16.2 / 58.0 | 1.292 / 0.031 | 12727 / 7127 |
| 9000 | 8192 B, 1 flow | 1.568 / 2.607 | 1.02 / 0.92 | 16.2 / 57.2 | 0.411 / 0.000 | 3493 / 1226 |
| 9000 | 8192 B, 4 flows | 6.059 / 6.179 | 1.02 / 0.83 | 25.5 / 59.2 | 9.752 / 1.391 | 15863 / 5739 |

The MTU 1500, 8192-byte, four-flow reference row has only two valid repetitions.
Its third repetition failed payload validation with
`paced UDP payload differs: flow=1 sequence=17922`.
The failed sample remains in the [sample CSV](direct-performance-samples.csv) and
its [traffic log](../../target/performance/release/tun-q2ueaitw/mtu1500-v4-udp-large-2-sing-box-go/measure/traffic.log).
The cause was not established. No numeric metrics from that failed sample enter
the table. All 54 candidate samples passed payload validation; lossy UDP cases
explicitly allow and count missing replies.

The four-flow fragmented workload overloads both implementations. Its 75% to 78%
loss cannot support a claim of useful loss-free capacity. At MTU 9000, the same
payload avoids fragmentation, but Kotoconn still loses more replies and has higher
CPU per byte and p99. Small-datagram CPU is close to or below the reference while
latency varies substantially between the two measurement sessions.

## UDP churn

| MTU | Client connections/s | CPU ns/B | RSS MiB | Open FDs after | Reply p99 us |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1500 | 16998 / 17010 | 38.97 / 38.52 | 350.2 / 199.9 | 30801 / 16397 | 1388 / 1866 |
| 9000 | 18324 / 16374 | 35.45 / 39.61 | 360.9 / 198.6 | 31913 / 16397 | 1387 / 1821 |

Both implementations return every churn reply. Kotoconn matches completion rate
at MTU 1500 and exceeds it by 12% at MTU 9000, but total RSS remains about 350 to
361 MiB versus 199 to 200 MiB. Open descriptor counts show different retained
association populations. Client socket churn can reuse a proxy association, so
these completion counts are not counts of newly created outbound sockets.
The memory gap remains visible without normalizing it away.

## Verification and reproduction

`mise exec -- moon run workspace:ci` passed all 33 tasks, including standard checks,
protocol E2E, container isolation and cancellation, TUN quick tests, benchmark
smoke tests, and UDP warmup/port-reuse checks. The separate Linux stress run passed
all 148 workloads across IPv4 and IPv6 and MTUs 1280, 1500, 9000 and 65535. It also
passed reset, drain, forced-stop, failed-start and generic SOCKS TCP/UDP checks.
IPv6 performance is not measured in these tables.

Added or extended tests exercise retained read chunks, exact partial-write
accounting and half-close, native UDP datagram boundaries and nested cancellation,
worker failure and abort cleanup, pending output batches, large ingress bursts,
and recovery from fragment budget exhaustion without erasing overlap poison.

The committed [sample CSV](direct-performance-samples.csv) preserves all 108
attempts, including the failed reference sample. Complete local artifacts retain
histograms, process samples, configuration and logs:

- [MTU 1500 concurrent TCP and fragmented UDP](../../target/performance/release/tun-q2ueaitw/results.json)
- [MTU 1500 single TCP, ordinary UDP and churn](../../target/performance/release-mtu1500/tun-vwlc0sy5/results.json)
- [MTU 9000 matrix](../../target/performance/release-mtu9000/tun-v2rnhofj/results.json)
- [148-case stress run](../../target/performance/release-stress/tun-si35c6rn/results.json)
- [Full CI log](../../target/verification-isolation-order.log)

SHA256 digests:

```text
Kotoconn       89c62dfba92a4029f63897857fb07cb188e793eea0ffea637bbdbf75e447fc4e
sing-box       64f6d8613f9c7d42ef9a8e90dd9fca7290f176c5b482714915c04353911559c0
Traffic binary 30d8b534633203501e6cee0398a588dae05be63ffd24c9e87008de5706281dee
libcronet.so   c3949c6ad64e1d8fcd1e3b1fae4e302b2e553d769665a4bd7576483564c3f026
```

Only documentation was dirty when these measurements began. The frozen release
binary and its adjacent native library remain under `target/release-candidate`.
The same frozen traffic generator was used for both implementations throughout.
To repeat these workloads with the preserved binaries:

```sh
mise exec -- taskset -c 2-5 uv run --locked python benchmarks/run.py \
  --binary target/release-candidate/kotoconn \
  --traffic-binary target/traffic-fixed/kotoconn-tun-traffic \
  --sing-box target/reference/sing-box-1.15.0-alpha.8-linux-amd64/sing-box \
  --profile full --family 4 --mtu 1500 --mtu 9000 --daemon-cpus 0,1 \
  --duration 3 --warmup 0 --repetitions 3 --udp-rate 20000 \
  --case tcp-bulk-1 --case tcp-upload-16 --case tcp-download-16 \
  --case udp-paced --case udp-paced-1 --case udp-small \
  --case udp-large --case udp-large-1 --case udp-churn \
  --output target/performance/repeat-direct
```

The captured MTU 1500 measurements were split into two invocations after the
reference failure. A fresh checkout can use `mise exec -- moon run benchmark:run --`
with the same workload options and a caller-supplied reference binary; that task
builds and stages current release binaries. New measurements record their own
hashes and must not be silently combined with this sample set.
