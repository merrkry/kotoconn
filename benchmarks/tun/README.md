# TUN measurements

See the [direct forwarding report](direct-performance.md) for the current
optimization results, reference comparison, remaining gaps and per-sample data.

Use Linux with Docker and `/dev/net/tun`, following [workspace setup](../../docs/build.md):

```sh
mise exec -- moon run benchmark:run
mise exec -- moon run benchmark:run -- --profile full
mise exec -- moon run benchmark:test-udp-churn
```

Use `--help` for workload filters and measurement options. To compare against a
caller-supplied sing-box 1.15 binary with the go TUN stack, pass
`--sing-box /path/to/sing-box`. The reference enables multi-queue TUN, using
the same CPU allowance as Kotoconn. Both implementations use the same generator
binary and alternate execution order. Omit this option for routine candidate
measurements.

## Comparing results

Use release binaries and identical workload, echo-server and CPU-affinity
settings. Run comparisons sequentially or reserve separate CPU sets yourself;
affinity settings do not reserve CPUs. Inspect generator CPU use before attributing
a throughput limit to the proxy. These are verified application exchanges, so
payload generation and verification can limit throughput.

Read completion rate together with loss and latency. Offered-load duration differs
from completion time under overload because receive drain remains in the measured
window. Paced UDP allows reported loss; strict request/reply cases require every
reply. UDP churn counts client sockets, which may reuse existing proxy associations.

The full profile includes `udp-large-1` and `udp-large`, with 8192-byte datagrams
on one and four flows. They exercise IP fragmentation at MTU 1500 and large
unfragmented packets at MTU 9000. `--udp-rate` sets the offered rate per flow.

Important metric interpretations:

- Payload throughput counts verified traffic in both directions, excluding headers
  and malformed bytes. Its window includes connection setup and receive drain.
- Scheduled latency includes generator delay; operation RTT starts at the actual
  request attempt. Keep latency distributions for different workloads separate.
- Merge histogram counts when aggregating samples, rather than averaging percentiles.
- Process CPU excludes some kernel work. Lifetime memory peaks can include warmup;
  retained allocator memory is not by itself evidence of a leak.

Shared CI smoke samples verify the pipeline and are not performance baselines.
Retain failed samples and their artifacts; later successful repetitions do not
replace them.

## Investigating or extending a workload

Start with `results.json` and `samples.csv` in the printed artifact directory, then
inspect the per-sample logs, histograms and resource records. Preserve the binary
hashes, source revision and settings with any reported result.

[run.py](../run.py) owns benchmark execution and result aggregation.
[measurement.py](../../e2e/tun_support/measurement.py) defines metrics, and
[scenarios.py](../../e2e/tun_support/scenarios.py) defines shared workloads.
Keep network experiments inside the [TUN launcher](../../e2e/tun_support/README.md#isolation-and-artifacts).
Changes to UDP warmup or port allocation should also pass `benchmark:test-udp-churn`.
