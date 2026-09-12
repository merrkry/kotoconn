FROM docker.io/library/rust:1.98.0-bookworm@sha256:4e4a7e7939c17991ab35f2b8c2e67593980f771d28f6b1254b1850f860fd0c7f AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked -p kotoconn-cli && cp target/debug/kotoconn /kotoconn

FROM docker.io/library/debian:bookworm-slim@sha256:5ae3c39ebd15e229dcedd5cee596b2497182493d41ff162e824ba13fc1b2b867
COPY --from=build /kotoconn /usr/local/bin/kotoconn
ENTRYPOINT ["kotoconn"]
CMD ["run", "--config", "/config/main.ts", "--shutdown-timeout", "3"]
