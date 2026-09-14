# Benchmarks

The [TUN workload suite](tun/README.md) shares verified traffic generation and
network isolation with the TUN E2E tests. Follow the [setup instructions](tun/README.md),
then let Nx build the release binaries and run the workloads:

```sh
pnpm exec nx run benchmarks:run
```

Each run writes JSON samples, latency histograms, resource measurements and a
CSV summary to its own directory under `target/benchmarks/`. Use `--sing-box`
to compare against a sing-box 1.15 binary with the go TUN stack. This is the
only reference implementation; omitting it runs Kotoconn alone.
