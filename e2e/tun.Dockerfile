FROM docker.io/library/python:3.13.7-slim-bookworm@sha256:781449467ffb6f04218f09b1ecdcdc7d22b289ee5da9ec498b024e24ad7a6db7
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 util-linux \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
ENTRYPOINT ["python3"]
