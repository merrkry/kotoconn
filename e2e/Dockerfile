# Nx builds the portable binaries with cargo-zigbuild before Bake packages them.
# "artifacts" is a named context selected by docker-bake.hcl.
FROM scratch AS binaries
COPY --from=artifacts /kotoconn /kotoconn
COPY --from=artifacts /kotoconn-tun-traffic /kotoconn-tun-traffic

FROM docker.io/library/debian:bookworm-slim@sha256:5ae3c39ebd15e229dcedd5cee596b2497182493d41ff162e824ba13fc1b2b867 AS runtime
COPY --from=binaries /kotoconn /usr/local/bin/kotoconn
COPY --from=binaries /kotoconn-tun-traffic /usr/local/bin/kotoconn-tun-traffic
ENTRYPOINT ["kotoconn"]
CMD ["--log-format", "json", "run", "--config", "/config/main.ts", "--shutdown-timeout", "3"]
