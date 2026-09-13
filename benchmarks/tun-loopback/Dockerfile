FROM docker.io/library/debian:bookworm-slim@sha256:5ae3c39ebd15e229dcedd5cee596b2497182493d41ff162e824ba13fc1b2b867

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends iperf3 iproute2 python3 util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --no-create-home benchmark

COPY benchmarks/run.py /usr/local/bin/benchmarks.py

ENTRYPOINT ["python3", "/usr/local/bin/benchmarks.py"]
