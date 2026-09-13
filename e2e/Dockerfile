FROM docker.io/library/rust:1.98.0-bookworm@sha256:4e4a7e7939c17991ab35f2b8c2e67593980f771d28f6b1254b1850f860fd0c7f AS chef
ARG CARGO_TERM_COLOR=auto
RUN cargo install cargo-chef --version 0.1.78 --locked
WORKDIR /src

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY external/smoltcp ./external/smoltcp
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /src/recipe.json recipe.json
COPY external/smoltcp ./external/smoltcp
# Keep dependency artifacts in a regular layer so CI can export them with type=gha.
RUN cargo chef cook --locked -p kotoconn-cli --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY external/smoltcp ./external/smoltcp
RUN cargo build --locked -p kotoconn-cli

FROM docker.io/library/debian:bookworm-slim@sha256:5ae3c39ebd15e229dcedd5cee596b2497182493d41ff162e824ba13fc1b2b867
COPY --from=build /src/target/debug/kotoconn /usr/local/bin/kotoconn
ENTRYPOINT ["kotoconn"]
CMD ["--log-format", "json", "run", "--config", "/config/main.ts", "--shutdown-timeout", "3"]
