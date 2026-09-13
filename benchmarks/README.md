# Benchmarks

Run every benchmark suite from the repository root:

```sh
python3 benchmarks/run.py
```

Results are JSON files under `target/benchmarks/`. Each file identifies its
suite and schema version and includes the source revisions, environment, raw
samples, daemon CPU time, and relevant operating-system counters.

The suite currently contains only
[`tun-loopback`](tun-loopback/README.md). It needs Docker or a compatible
container engine and access to `/dev/net/tun`.
