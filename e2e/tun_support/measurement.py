"""Preserve samples and compare like-for-like completed workload windows."""

import csv
import json
import math
import os
import platform
import statistics
from collections import Counter
from pathlib import Path

from .environment import digest


def metadata(binaries, traffic_binary):
    container = json.loads(Path("/artifacts/container.json").read_text())
    cpu = next(
        (
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ),
        platform.machine(),
    )
    return {
        "kernel": platform.release(),
        "machine": platform.machine(),
        "cpu_model": cpu,
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "netns": os.readlink("/proc/self/ns/net"),
        "parent_netns": os.environ["KOTOCONN_TUN_PARENT_NETNS"],
        "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
        if Path("/sys/fs/cgroup/cpu.max").exists()
        else None,
        "source_commit": container["source_commit"],
        "source_status": container["source_status"],
        "smoltcp": container["smoltcp"],
        "container": container,
        "binaries": {
            name: {"path": str(path), "sha256": digest(path)}
            for name, path in binaries.items()
        },
        "traffic_binary_sha256": digest(traffic_binary),
    }


def distribution(histograms):
    # Merge histogram counts, never average per-flow percentiles.
    buckets = Counter()
    for histogram in histograms:
        for upper, count in histogram["buckets"]:
            buckets[upper] += count
    count = sum(buckets.values())
    result = {"count": count, "buckets": sorted(buckets.items())}
    for name, quantile in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99)):
        cumulative = 0
        result[name] = None
        for upper, number in sorted(buckets.items()):
            cumulative += number
            if cumulative >= math.ceil(count * quantile):
                result[name] = upper
                break
    return result


def metrics(result):
    flows = result["flows"]
    wall = result["wall_seconds"]
    totals = {
        field: sum(flow[field] for flow in flows)
        for field in (
            "sent_bytes",
            "received_bytes",
            "operations",
            "connections",
            "sent_datagrams",
            "received_datagrams",
            "lost_datagrams",
            "duplicates",
            "reordered",
        )
    }
    delivered = totals["sent_bytes"] + totals["received_bytes"]
    # For UDP, a lost round trip does not prove delivery at the server.
    delivered -= sum(
        (flow["sent_bytes"] - flow["received_bytes"])
        for flow in flows
        if flow["kind"].startswith("udp")
    )
    result = {
        "totals": totals,
        "confirmed_bidirectional_bytes_per_second": delivered / wall,
        "operations_per_second": totals["operations"] / wall,
        "connections_per_second": totals["connections"] / wall,
        "latency_by_kind": {
            kind: distribution(
                [flow["latency_us"] for flow in flows if flow["kind"] == kind]
            )
            for kind in sorted({flow["kind"] for flow in flows})
        },
        "scheduled_latency_us": distribution(
            [flow["scheduled_latency_us"] for flow in flows]
        ),
    }
    result["connect_us"] = distribution([flow["connect_us"] for flow in flows])
    result["first_response_us"] = distribution(
        [flow["first_response_us"] for flow in flows]
    )
    return result


def resource_metrics(result):
    metrics = {}
    for name in ("daemon", "generator"):
        before = result["resources"]["before"][name]
        after = result["resources"]["after"][name]
        cpu = sum(
            after[field] - before[field] for field in ("user_seconds", "system_seconds")
        )
        observed = [
            before,
            after,
            *(sample[name] for sample in result["resources"]["samples"]),
        ]
        metrics[name] = {
            "cpu_seconds": cpu,
            "cpu_cores": cpu / result["wall_seconds"],
            "observed_peak_rss_bytes": max(sample["rss_bytes"] for sample in observed),
            "rss_before": before["rss_bytes"],
            "rss_after": after["rss_bytes"],
            "fds_before": before["fds"],
            "fds_after": after["fds"],
        }
        pss = [
            sample["pss_bytes"]
            for sample in observed
            if sample["pss_bytes"] is not None
        ]
        metrics[name]["observed_peak_pss_bytes"] = max(pss) if pss else None
    return metrics


def comparisons(runs):
    result = []
    for case in sorted({run["case"] for run in runs}):
        samples = {}
        for run in runs:
            if run["case"] == case and run["status"] == "passed":
                samples.setdefault(run["implementation"], []).append(
                    run["metrics"]["confirmed_bidirectional_bytes_per_second"]
                )
        medians = {name: statistics.median(values) for name, values in samples.items()}
        candidate = medians.get("candidate")
        result.append(
            {
                "case": case,
                "throughput_medians": medians,
                "throughput_samples": samples,
                "candidate_relative_change": {
                    name: candidate / value - 1
                    for name, value in medians.items()
                    if name != "candidate" and value and candidate is not None
                },
            }
        )
    return result


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def write_csv(path, runs):
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "case",
                "implementation",
                "repetition",
                "bytes_per_second",
                "operations_per_second",
                "daemon_cpu_cores",
                "generator_cpu_cores",
                "daemon_peak_rss_bytes",
                "udp_lost",
                "p99_us_by_kind",
            ]
        )
        for run in runs:
            if run["status"] != "passed":
                continue
            metric = run["metrics"]
            daemon = run["resources"]["daemon"]
            writer.writerow(
                [
                    run["case"],
                    run["implementation"],
                    run["repetition"],
                    metric["confirmed_bidirectional_bytes_per_second"],
                    metric["operations_per_second"],
                    daemon["cpu_cores"],
                    run["resources"]["generator"]["cpu_cores"],
                    daemon["observed_peak_rss_bytes"],
                    metric["totals"]["lost_datagrams"],
                    json.dumps(
                        {
                            kind: histogram["p99"]
                            for kind, histogram in metric["latency_by_kind"].items()
                        }
                    ),
                ]
            )
