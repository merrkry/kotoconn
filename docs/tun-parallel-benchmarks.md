# Parallel TUN worker benchmarks

Measured on 2026-09-13, Intel Core Ultra X7 358H, Linux 7.2.3, x86_64. Both binaries use the same container image, 2 GiB memory limit, MTU 1500 and direct loopback outbound. Each setup has three samples, one second omitted and three measured seconds. Each sample starts a fresh proxy and iperf3 server. Compilation runs separately from measurement; CPU affinity is not fixed.

The serial control is `c49aecd` with only CPU-count queue selection and the detection-error fallback applied. The parallel version includes all prior optimizations, per-queue workers and stable flow/fragment ownership. Both versions open 16 queues under a 16-CPU quota and four queues under a four-CPU quota, verified through `/proc/<pid>/fdinfo`. This isolates worker architecture from queue count.

## 16-CPU quota

| Streams | Direction | Serial Gbit/s | Parallel Gbit/s | Change | sing-box Go Gbit/s |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | upload | 8.077 | 7.858 | -2.7% | 37.618 |
| 1 | download | 4.705 | 4.732 | +0.6% | 31.436 |
| 4 | upload | 8.504 | 19.708 | +131.8% | 34.922 |
| 4 | download | 10.355 | 17.039 | +64.6% | 27.859 |
| 16 | upload | 9.433 | 26.157 | +177.3% | 32.638 |
| 16 | download | 6.258 | 35.400 | +465.7% | 27.413 |
| 64 | upload | 8.731 | 25.235 | +189.0% | 30.219 |
| 64 | download | 1.640 | 31.094 | +1795.6% | 26.651 |

The sing-box Go reference uses revision `68b74f9516a2b2e126065e71f31344e2802ce507`, the same 16-CPU quota, source-address routing and three-sample methodology. Its TUN adapter opens one descriptor with its default configuration.

## Four-CPU quota

| Streams | Direction | Serial Gbit/s | Parallel Gbit/s | Change |
| ---: | --- | ---: | ---: | ---: |
| 1 | upload | 8.452 | 8.177 | -3.3% |
| 1 | download | 4.820 | 4.873 | +1.1% |
| 4 | upload | 8.691 | 20.075 | +131.0% |
| 4 | download | 11.086 | 18.746 | +69.1% |

The improvement comes with greater parallel CPU use. For example, 16-stream download uses about 8.50 daemon CPU cores versus 3.43 for the serial control, under the same 16-CPU quota. Single-stream upload declines about 3%; this rewrite primarily improves concurrent traffic. CPU averages cover warmup, measurement and control overhead. These are short bulk-TCP tests, not UDP throughput, high-RTT or high-connection-count capacity measurements.

For 64-stream download, all 16 workers receive and transmit traffic. Only 0.039–0.045% of normalized receive packets are forwarded between workers. The busiest worker receives 10.8–12.7% of receive bytes across the three samples. Forwarding and admission drop counters remain zero; these counters do not cover every possible ingress or kernel drop. Upload uses 14–16 workers across the three samples. Stable flow ownership gives distribution, not an exact equal allocation of bytes.

## Routing correction

The initial UID-routed trial left one flow in LAST_ACK during 64-stream download, with empty receive/send buffers and both application directions closed. Linux TIME_WAIT replies obtain a routing UID without the original full socket, so the UID-only rule misses their ACKs. Replaying the same diagnostic binary with a client-source-address rule completed and drained normally. The final tables use this corrected routing on both binaries. Kernel ACK construction is in [`tcp_v4_send_ack`](https://github.com/torvalds/linux/blob/master/net/ipv4/tcp_ipv4.c). The tracked benchmark runner now uses the source-address rule too; application TCP shutdown behavior was not changed to hide missing control packets.

## Validation and artifacts

`cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace` pass. The TUN crate has 22 tests, including cross-worker fragments, shared reassembly capacity/expiry, transmit fragment IDs across queues, worker failure cleanup and forced cancellation during a blocked write. The real Linux TUN E2E suite passes dual-stack TCP/UDP, fragmentation, server-first traffic, half-close, malformed input and interface cleanup.

The HTML report and raw data are saved as `~/Downloads/kotoconn-tun-optimization.html` and `kotoconn-tun-optimization-data/parallel/`. Dataset directories `41-serial-final-16`, `42-parallel-final-16`, `43-serial-final-4` `44-parallel-final-4` and `45-sing-box-final-16` contain individual measurements, logs, source patches and binary hashes. The accompanying runner scripts preserve the exact high-concurrency setup; the repository's default benchmark still tests one and four streams. Experimental two-task queue workers and UID-routing diagnostics remain in the report as development history, separate from the final comparison.
