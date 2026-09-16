FROM docker.io/library/debian:bookworm-slim@sha256:5ae3c39ebd15e229dcedd5cee596b2497182493d41ff162e824ba13fc1b2b867 AS runtime

# Rust compilation is a moon dependency. This image only packages its outputs.
FROM runtime AS app
COPY target/tun/debug/kotoconn /usr/local/bin/kotoconn
ENTRYPOINT ["kotoconn"]
CMD ["--log-format", "json", "run", "--config", "/config/main.ts", "--shutdown-timeout", "3"]

FROM ghcr.io/astral-sh/uv:0.12.5 AS uv

FROM runtime AS test-runtime
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_PYTHON_PREFERENCE=only-managed
COPY .python-version /tmp/.python-version
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates iproute2 util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && uv python install "$(cat /tmp/.python-version)" \
    && ln -s "$(uv python find "$(cat /tmp/.python-version)")" /usr/local/bin/python3 \
    && ln -s /usr/local/bin/python3 /usr/local/bin/python
WORKDIR /workspace
ENTRYPOINT ["python3"]
