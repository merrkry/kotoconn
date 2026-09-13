# External sources

`smoltcp` is a submodule of [merrkry/smoltcp](https://github.com/merrkry/smoltcp), based on upstream 0.14.0. Cargo patches every smoltcp dependency to this checkout. Initialize it after cloning:

```sh
git submodule update --init --recursive
```

The fork exposes direct TCP driving, a small host-context interface, and replaceable byte storage. The ordinary Interface and ring-buffer API remain available for upstream tests and other users. Keep runtime scheduling and allocation policy in Kotoconn.

Test the fork separately because it is excluded from the parent workspace:

```sh
cargo test --manifest-path external/smoltcp/Cargo.toml --lib --no-default-features --features std,medium-ip,proto-ipv4,proto-ipv6,socket-tcp,socket-tcp-cubic,assembler-max-segment-count-32,segmentation-offload
```

Commit and push fork changes before updating the parent submodule pointer. Retain upstream history and keep fork changes in focused commits.
