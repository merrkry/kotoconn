# TUN verification

Use Linux with Docker and `/dev/net/tun`, following
[workspace setup](../../docs/build.md):

```sh
mise exec -- moon run docker:test-tun
mise exec -- moon run docker:test-tun -- --profile stress
mise exec -- moon run docker:test-isolation
```

Use the stress profile for changes to connection reuse, buffering, or
lifecycle. Refer to the relevant RFC to establish what the protocol permits
when a packet is rejected.

## Isolation and artifacts

Use the container launcher even for local runs. Keep test routing and sysctls
inside its private network. Keep host networking, D-Bus, and the container
engine socket outside the container.

Retain failed-run artifacts before changing workloads. A seed reproduces
payloads and offered traffic. Reproducing a scheduling bug requires explicit
synchronization or virtual time. If cleanup is interrupted, identify the
container from the run's `container.cid`.
