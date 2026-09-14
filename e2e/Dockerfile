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
ARG CARGO_PROFILE=dev
COPY --from=planner /src/recipe.json recipe.json
COPY external/smoltcp ./external/smoltcp
# Keep dependency artifacts in a regular layer so CI can export them with type=gha.
RUN cargo chef cook --locked --profile "$CARGO_PROFILE" -p kotoconn-cli -p kotoconn-tun-traffic --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY external/smoltcp ./external/smoltcp
RUN cargo build --locked --profile "$CARGO_PROFILE" -p kotoconn-cli -p kotoconn-tun-traffic \
    && profile_dir="$CARGO_PROFILE" \
    && if [ "$profile_dir" = dev ]; then profile_dir=debug; fi \
    && install -t /usr/local/bin "target/$profile_dir/kotoconn" "target/$profile_dir/kotoconn-tun-traffic"

FROM docker.io/library/debian:bookworm-slim@sha256:5ae3c39ebd15e229dcedd5cee596b2497182493d41ff162e824ba13fc1b2b867
COPY --from=build /usr/local/bin/kotoconn /usr/local/bin/kotoconn
COPY --from=build /usr/local/bin/kotoconn-tun-traffic /usr/local/bin/kotoconn-tun-traffic
ENTRYPOINT ["kotoconn"]
CMD ["--log-format", "json", "run", "--config", "/config/main.ts", "--shutdown-timeout", "3"]
