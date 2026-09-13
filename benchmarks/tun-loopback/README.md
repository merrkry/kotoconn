# TUN loopback throughput benchmark

This benchmark compares the current Kotoconn working tree with the sing-box
`go` TUN stack. It runs iperf3 inside one isolated container network namespace.
A UID policy rule sends only the client traffic through TUN. The proxy's direct
outbound reaches a server bound to another loopback address.

Run it from the repository root:

```sh
python3 benchmarks/run.py
```

The suite writes `target/benchmarks/tun-loopback/results.json`. It keeps the
generated policies and process logs next to the result for diagnosis.

Pass `--output PATH` to write the JSON to another location.

The runner needs Docker or a compatible container engine, `/dev/net/tun`,
`CAP_NET_ADMIN`, `cargo-zigbuild`, the Rust musl target, Go 1.25.5 or later, and
Git. It builds the current Kotoconn working tree and records its commit plus the
SHA-256 digest of changes under `crates/`. The default sing-box revision is
pinned in `run.py`; set `SING_BOX_REF` to test another commit.

Each scenario has five samples. iperf3 omits the first second and measures the
next three seconds. The JSON contains received throughput, daemon CPU time, and
TUN interface counters for each sample. Scenarios cover one or four TCP streams
in upload and reverse-download directions at MTU 1500.
