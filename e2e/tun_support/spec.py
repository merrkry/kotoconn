"""Command-line options shared by traffic workload recipes and runners."""

from typing import TypedDict


class TrafficSpec(TypedDict, total=False):
    source: str
    target: str
    mtu: int
    port: int
    protocol: str
    workload: str
    direction: str
    connections: int
    rounds: int
    bytes: int
    seed: int
    duration_ms: int
    timeout: float
    rate: int
    datagram_bytes: int
    allow_loss: bool
    close_mode: str
    worker_threads: int
    udp_echo_batch: int
    udp_server_receive_buffer: int
    udp_source_ports: str
