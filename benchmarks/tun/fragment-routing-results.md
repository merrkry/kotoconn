# Fragment routing verification

The offset-zero fragment now binds an IP datagram to its transport worker.
Earlier fragments retain their receive chunks until that binding is available.
The TUN runtime and queue count policy are unchanged.

Validation passed workspace Clippy with warnings denied and all 102 workspace
tests. New coverage includes concurrent TCP/UDP fragment arrivals in both IP
families, chunk ownership, retained allocation accounting, idle expiry, conflicting
IPv6 headers and fragment ID reuse with stale queued deliveries.

The Docker runner passes all 148 TUN E2E cases, the concurrent-container and
cancellation checks, and the UDP warmup reuse check with 64 ephemeral ports.
Host DNS, addresses, routes and the sampled sysctls match the post-cleanup snapshot.

The table is one candidate sample per case in Docker, with a two-second offered
load and 0.5-second warmup. Daemon affinity is CPUs 0 and 1; the generator has
CPUs 2 through 5, four workers, echo batch 32 and an SO_RCVBUF request of 1 MiB.
The effective echo receive buffer is 2 MiB. Paced UDP offers 100k datagrams/s
per flow over four flows. Its completion rate includes receive drain when replies
are lost. Payload generation and validation remain part of the workload.

| MTU | IP | UDP boundaries Gbit/s | Boundaries CPU ns/byte | TCP download-16 Gbit/s | Paced UDP loss |
| --- | --- | ---: | ---: | ---: | ---: |
| 1500 | IPv4 | 2.908 | 1.952 | 46.518 | 6.72% |
| 1500 | IPv6 | 2.914 | 1.962 | 46.756 | 9.30% |
| 9000 | IPv4 | 4.193 | 1.364 | 45.896 | 4.89% |
| 9000 | IPv6 | 4.079 | 1.352 | 45.730 | 7.92% |

All boundary and TCP cases completed without loss. These candidate-only samples
do not establish the size of an improvement or a new gap to sing-box. The old
host-namespace comparison was stopped after discovering its system DNS requests;
it is not combined with the container measurements. No reference was restarted
during container verification.

Artifacts:

- [Candidate samples](../../target/verification/docker-benchmark/tun-hhpesfu8/results.json)
- [Container command and image](../../target/verification/docker-benchmark/tun-hhpesfu8/container.json)
- [Docker E2E](../../target/verification/docker-e2e/tun-7pyh13ox/results.json)
- [Isolation and cancellation](../../target/e2e/parallel-d78fokhc/parent-after.json)
- [UDP warmup reuse](../../target/verification/docker-churn/tun-5luiiiue/results.json)
