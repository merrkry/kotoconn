"""Measure verified TUN workloads in a Docker container."""

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from e2e.tun_support.comparison import SingBox
from e2e.tun_support.environment import (
    CLIENT,
    REMOTE,
    SERVER_PORTS,
    Daemon,
    add_arguments,
    command,
    configure_network,
    enter,
    reserve_server_port,
    traffic,
)
from e2e.tun_support.measurement import (
    comparisons,
    metadata,
    metrics,
    resource_metrics,
    save,
    write_csv,
)
from e2e.tun_support.packets import Injector


def cases(args):
    recipes = [
        ("tcp-bulk-1", {"connections": 1, "direction": "duplex"}),
        ("tcp-bulk-4", {"connections": 4, "direction": "duplex"}),
        ("tcp-churn", {"workload": "churn", "connections": 8}),
        ("tcp-sparse", {"workload": "sparse", "connections": 64}),
        (
            "udp-paced",
            {
                "protocol": "udp",
                "connections": 4,
                "rate": args.udp_rate,
                "allow_loss": True,
            },
        ),
        (
            "mixed-malformed",
            {
                "workload": "mixed",
                "connections": 16,
                "rate": args.udp_rate,
                "allow_loss": True,
            },
        ),
    ]
    if args.profile == "full":
        recipes += [
            ("tcp-upload-1", {"direction": "upload"}),
            ("tcp-download-1", {"direction": "download"}),
            ("tcp-upload-16", {"direction": "upload", "connections": 16}),
            ("tcp-download-16", {"direction": "download", "connections": 16}),
            ("tcp-sparse-256", {"workload": "sparse", "connections": 256}),
            (
                "udp-paced-1",
                {"protocol": "udp", "rate": args.udp_rate, "allow_loss": True},
            ),
            (
                "udp-small",
                {
                    "protocol": "udp",
                    "rate": args.udp_rate,
                    "datagram_bytes": 64,
                    "allow_loss": True,
                },
            ),
            ("udp-churn", {"protocol": "udp", "workload": "churn", "connections": 8}),
            (
                "udp-sparse",
                {"protocol": "udp", "workload": "sparse", "connections": 64},
            ),
            ("udp-boundaries", {"protocol": "udp", "workload": "boundaries"}),
            (
                "mixed-clean",
                {
                    "workload": "mixed",
                    "connections": 16,
                    "rate": args.udp_rate,
                    "allow_loss": True,
                },
            ),
        ]
    for mtu in args.mtu:
        for family in args.family:
            for name, spec in recipes:
                if args.case and name not in args.case:
                    continue
                yield (
                    f"mtu{mtu}-v{family}-{name}",
                    name,
                    {
                        "source": CLIENT[int(family == 6)],
                        "target": REMOTE[int(family == 6)],
                        "mtu": mtu,
                        "bytes": 1048577,
                        "close_mode": "exchange",
                        "duration_ms": round(args.duration * 1000),
                        "worker_threads": args.traffic_workers,
                        "udp_echo_batch": args.udp_echo_batch,
                        "udp_server_receive_buffer": args.udp_server_receive_buffer,
                        **spec,
                    },
                )


def implementations(args, case_name):
    if args.sing_box and case_name != "mixed-malformed":
        return ["candidate", "sing-box-go"]
    return ["candidate"]


def run(args):
    configure_network()
    binaries = {"candidate": args.binary}
    if args.sing_box:
        binaries["sing-box-go"] = args.sing_box
    result = {
        "schema_version": 2,
        "suite": "tun-workloads",
        "status": "running",
        "metadata": metadata(binaries, args.traffic_binary),
        "settings": {
            "duration_seconds": args.duration,
            "warmup_seconds": args.warmup,
            "repetitions": args.repetitions,
            "seed": args.seed,
            "daemon_cpus": args.daemon_cpus,
            "traffic_workers": args.traffic_workers,
            "udp_echo_batch": args.udp_echo_batch,
            "udp_server_receive_buffer": args.udp_server_receive_buffer,
        },
        "runs": [],
    }
    if args.sing_box:
        result["metadata"]["reference_version"] = args.reference_version
        result["metadata"]["reference_scope"] = "clean traffic only"
        result["metadata"]["reference_cleanup"] = (
            "process killed after verified measurement"
        )
    path = args.output / "results.json"
    try:
        for case_id, name, spec in cases(args):
            # Compare the mixed flow set without raw injection in mixed-clean.
            names = implementations(args, name)
            for repetition in range(args.repetitions):
                # Alternate order within each pair; every sample starts a fresh daemon.
                order = names if repetition % 2 == 0 else list(reversed(names))
                for implementation in order:
                    directory = args.output / f"{case_id}-{repetition}-{implementation}"
                    directory.mkdir()
                    # JSON report entries collect heterogeneous metrics and failure details.
                    entry: dict[str, Any] = {
                        "case": case_id,
                        "implementation": implementation,
                        "repetition": repetition,
                        "status": "running",
                        "result": str(
                            (directory / "measure" / "result.json").relative_to(
                                args.output
                            )
                        ),
                    }
                    result["runs"].append(entry)
                    save(path, result)
                    daemon = None
                    try:
                        if implementation == "sing-box-go":
                            daemon = SingBox(
                                binaries[implementation],
                                directory / "daemon",
                                spec["mtu"],
                                args.daemon_cpus,
                            )
                        else:
                            daemon = Daemon(
                                binaries[implementation],
                                directory / "daemon",
                                spec["mtu"],
                                cpus=args.daemon_cpus,
                            )
                        sample_spec = {**spec, "seed": args.seed + repetition}
                        if spec.get("protocol") == "udp":
                            # UDP has no FIN. Warmup associations retain outbound
                            # ports until idle expiry; changing the destination
                            # doubles that population and can exhaust Linux's
                            # ephemeral range before measurement completes.
                            sample_spec["port"] = reserve_server_port()
                        if args.warmup:
                            warmup = directory / "warmup"
                            warmup.mkdir()
                            traffic(
                                args.traffic_binary,
                                daemon,
                                warmup,
                                {
                                    **sample_spec,
                                    "duration_ms": round(args.warmup * 1000),
                                },
                            )
                        measure = directory / "measure"
                        measure.mkdir()
                        injector = (
                            Injector(
                                spec["source"] == CLIENT[1], seed=sample_spec["seed"]
                            )
                            if name == "mixed-malformed"
                            else None
                        )
                        try:
                            sample = traffic(
                                args.traffic_binary,
                                daemon,
                                measure,
                                sample_spec,
                                inject=injector,
                            )
                            if injector:
                                if (
                                    not injector.counts["positive-control"]
                                    or not sample["positive_controls"]
                                ):
                                    raise RuntimeError(
                                        "mixed raw input did not reach the traffic server"
                                    )
                                entry["injected"] = dict(injector.counts)
                        finally:
                            if injector:
                                injector.close()
                        entry.update(
                            status="passed",
                            metrics=metrics(sample),
                            resources=resource_metrics(sample),
                        )
                        goodput = entry["metrics"][
                            "confirmed_bidirectional_bytes_per_second"
                        ]
                        entry["resources"]["daemon"]["cpu_ns_per_confirmed_byte"] = (
                            entry["resources"]["daemon"]["cpu_seconds"]
                            * 1e9
                            / (goodput * sample["wall_seconds"])
                            if goodput
                            else None
                        )
                        daemon.finish()
                        print(
                            f"{case_id} {implementation} {repetition}: {goodput * 8 / 1e9:.3f} Gbit/s, "
                            f"lost={entry['metrics']['totals']['lost_datagrams']}",
                            flush=True,
                        )
                    except BaseException as error:
                        entry.update(status="failed", error=str(error))
                        raise
                    finally:
                        if daemon:
                            daemon.close()
                        save(path, result)
        if not result["runs"]:
            raise ValueError("no matching benchmark cases")
        result.update(status="passed", comparisons=comparisons(result["runs"]))
    except BaseException:
        result["status"] = "failed"
        raise
    finally:
        save(path, result)
        write_csv(args.output / "samples.csv", result["runs"])
    print(f"Results: {path}", flush=True)


def add_traffic_arguments(parser):
    parser.add_argument(
        "--udp-server-receive-buffer",
        type=int,
        default=1048576,
        help="echo SO_RCVBUF request in bytes; 0 inherits the host default",
    )
    parser.add_argument("--udp-echo-batch", type=int, choices=range(1, 33), default=32)
    parser.add_argument("--traffic-workers", type=int, choices=range(1, 65), default=4)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_arguments(parser, release=True)
    parser.add_argument("--profile", choices=("quick", "full"), default="quick")
    parser.add_argument(
        "--mtu", action="append", type=int, choices=(1280, 1500, 9000, 65535)
    )
    parser.add_argument("--family", action="append", type=int, choices=(4, 6))
    parser.add_argument("--case", action="append")
    parser.add_argument("--duration", type=float, default=3)
    parser.add_argument("--warmup", type=float, default=1)
    parser.add_argument("--repetitions", type=int, default=3)
    add_traffic_arguments(parser)
    parser.add_argument(
        "--udp-rate", type=int, default=10000, help="offered datagrams/s per UDP flow"
    )
    parser.add_argument(
        "--sing-box",
        type=Path,
        help="optional sing-box 1.15 reference with the go TUN stack",
    )
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    args.traffic_binary = args.traffic_binary.resolve(strict=True)
    if args.sing_box:
        args.sing_box = args.sing_box.resolve(strict=True)
    args.mtu = list(dict.fromkeys(args.mtu or [1500, 9000]))
    args.family = list(
        dict.fromkeys(args.family or ([4, 6] if args.profile == "full" else [4]))
    )
    if (
        args.duration < 0.2
        or args.warmup < 0
        or args.repetitions < 1
        or args.udp_rate < 1
        or not 0 <= args.udp_server_receive_buffer <= 2147483647
    ):
        parser.error(
            "duration must be >= 0.2s, warmup >= 0, repetitions and UDP rate positive; "
            "receive buffer must be in 0..2147483647"
        )
    selected = list(cases(args))
    if not selected:
        parser.error("no matching benchmark cases")
    required_ports = (
        sum(len(implementations(args, name)) for _, name, _ in selected)
        * args.repetitions
        * (1 + bool(args.warmup))
    )
    if required_ports > len(SERVER_PORTS):
        parser.error(
            f"selected matrix needs {required_ports} traffic server ports; "
            f"only {len(SERVER_PORTS)} are available; reduce repetitions or select fewer cases"
        )
    if not enter(args, Path(__file__).resolve(), "benchmarks"):
        if args.sing_box:
            args.reference_version = command(
                str(args.sing_box), "version"
            ).stdout.strip()
            if not args.reference_version.startswith("sing-box version 1.15."):
                parser.error(
                    "the reference must be sing-box 1.15 with the go TUN stack"
                )
        run(args)


if __name__ == "__main__":
    main()
