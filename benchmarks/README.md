# Benchmarks

The [TUN workload suite](tun/README.md) shares verified traffic generation and
network isolation with the TUN E2E tests.

```sh
cargo build --release -p kotoconn-cli -p kotoconn-tun-traffic
python3 benchmarks/run.py
```

Each run writes JSON samples, latency histograms, resource measurements and a
CSV summary to its own directory under `target/benchmarks/`. Use `--sing-box`
to compare against a sing-box 1.15 binary with the go TUN stack. This is the
only reference implementation; omitting it runs Kotoconn alone.
