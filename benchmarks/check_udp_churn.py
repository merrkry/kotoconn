"""Verify UDP benchmark warmup reuse with only 64 outbound ephemeral ports."""

import argparse
import json
from pathlib import Path
from unittest.mock import patch

import run as benchmark
from e2e.tun_support.environment import (
    add_arguments,
    configure_network,
    enter,
    traffic,
)


def small_port_range():
    configure_network()
    Path("/proc/sys/net/ipv4/ip_local_port_range").write_text("60000 60063")


def fixed_work(binary, daemon, directory, spec, **kwargs):
    # Complete many port cycles in each phase regardless of machine speed.
    return traffic(
        binary,
        daemon,
        directory,
        {**spec, "duration_ms": 0, "rounds": 512, "timeout": 5},
        **kwargs,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_arguments(parser, release=True)
    benchmark.add_traffic_arguments(parser)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    args.traffic_binary = args.traffic_binary.resolve(strict=True)
    if enter(args, Path(__file__).resolve(), "benchmarks"):
        return

    args.profile = "full"
    args.mtu = [1500]
    args.family = [4, 6]
    args.case = ["udp-churn"]
    args.duration = 1
    args.warmup = 1
    args.repetitions = 1
    args.udp_rate = 10000
    args.sing_box = None
    with (
        patch.object(benchmark, "configure_network", small_port_range),
        patch.object(benchmark, "traffic", fixed_work),
    ):
        benchmark.run(args)

    for family in args.family:
        sample = args.output / f"mtu1500-v{family}-udp-churn-0-candidate"
        warmup = json.loads((sample / "warmup/result.json").read_text())
        measure = json.loads((sample / "measure/result.json").read_text())
        assert warmup["spec"]["port"] == measure["spec"]["port"]
        for phase in (warmup, measure):
            assert sum(flow["operations"] for flow in phase["flows"]) == 4096
            assert all(flow["lost_datagrams"] == 0 for flow in phase["flows"])
    print("PASS UDP warmup and measurement reuse across IPv4 and IPv6")


if __name__ == "__main__":
    main()
