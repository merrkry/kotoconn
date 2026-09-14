# Benchmarks

The [TUN workload suite](tun/README.md) shares verified traffic generation and
network isolation with the TUN E2E tests.

```sh
cargo build --release -p kotoconn-cli -p kotoconn-tun-traffic
python3 benchmarks/run.py
```

Each run writes JSON samples, latency histograms, resource measurements and a
CSV summary to its own directory under `target/benchmarks/`. Use `--baseline`
for an existing Kotoconn binary, `--sing-box` for an optional reference, and
`--native-baseline` to check the generator's limits without a proxy.
