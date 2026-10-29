FROM docker.io/library/debian:bookworm-slim@sha256:a4672c0cb26fbdde88e38fa2dfb6c681942306680e41e4378b28770b6e79ee91 AS runtime

# Rust compilation is a moon dependency. This image only packages its outputs.
FROM runtime AS app
COPY target/tun/debug/kotoconn /usr/local/bin/kotoconn
COPY target/tun/debug/libcronet.so /usr/local/lib/libcronet.so
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* && ldconfig
ENTRYPOINT ["kotoconn"]
CMD ["--log-format", "json", "run", "--config", "/config/main.ts", "--shutdown-timeout", "3"]

FROM runtime AS anytls-interop
COPY target/anytls-interop/anytls-tests /usr/local/bin/anytls-tests
COPY target/anytls-interop/anytls-go-peer /usr/local/bin/anytls-go-peer
ENTRYPOINT ["anytls-tests"]

FROM ghcr.io/astral-sh/uv:0.13.6 AS uv

FROM runtime AS test-runtime
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_PYTHON_PREFERENCE=only-managed
COPY .python-version /tmp/.python-version
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates iproute2 openssl util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && uv python install "$(cat /tmp/.python-version)" \
    && ln -s "$(uv python find "$(cat /tmp/.python-version)")" /usr/local/bin/python3 \
    && ln -s /usr/local/bin/python3 /usr/local/bin/python
WORKDIR /workspace
ENTRYPOINT ["python3"]
