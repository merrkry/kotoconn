"""Run real TUN workloads in a Docker container, with unique per-run artifacts."""

import argparse
import json
import os
import socket
import struct
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from e2e.tun_support.environment import (
    CLIENT,
    REMOTE,
    SERVER_PORTS,
    Daemon,
    add_arguments,
    configure_network,
    digest,
    enter,
    network_snapshot,
    policy,
    resource,
    traffic,
)
from e2e.tun_support.lifecycle import BANNER, TRAILER, TcpEcho, client, read_exact
from e2e.tun_support.packets import Injector
from e2e.tun_support.scenarios import cases


def generic_relay(args, summary):
    directory = args.output / "generic-relay"
    daemon = Daemon(
        args.binary, directory, 1500, cpus=args.daemon_cpus, outbound="socks5"
    )
    try:
        for family in (4, 6):
            for protocol in ("tcp", "udp"):
                case = directory / f"v{family}-{protocol}"
                case.mkdir()
                entry = {
                    "id": f"generic-relay-v{family}-{protocol}",
                    "status": "running",
                    "result": str((case / "result.json").relative_to(args.output)),
                }
                summary["cases"].append(entry)
                traffic(
                    args.traffic_binary,
                    daemon,
                    case,
                    {
                        "source": CLIENT[int(family == 6)],
                        "target": REMOTE[int(family == 6)],
                        "protocol": protocol,
                        "workload": "bulk" if protocol == "tcp" else "boundaries",
                        "connections": 4 if protocol == "tcp" else 1,
                        "rounds": 4,
                        "bytes": 131073,
                        "seed": args.seed,
                    },
                )
                entry["status"] = "passed"
        daemon.finish()
    finally:
        daemon.close()
    print("PASS generic relay: dual-stack TCP and UDP through SOCKS5", flush=True)


def lifecycle(args):
    echo = TcpEcho()
    try:
        daemon = Daemon(
            args.binary, args.output / "graceful", 1500, cpus=args.daemon_cpus
        )
        try:
            # Cover immediate abort separately from graceful completion.
            echo.forced = True
            for family in (socket.AF_INET, socket.AF_INET6):
                with client(family, socket.SOCK_STREAM) as sock:
                    assert read_exact(sock, len(BANNER)) == BANNER
                    sock.setsockopt(
                        socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                    )
            echo.forced = False
            active = [
                client(family, socket.SOCK_STREAM)
                for family in (socket.AF_INET, socket.AF_INET6)
            ]
            try:
                for sock in active:
                    assert read_exact(sock, len(BANNER)) == BANNER
                daemon.process.terminate()
                daemon.event("daemon_stopping")
                for index, sock in enumerate(active):
                    payload = b"during-drain" + bytes([index])
                    sock.sendall(payload)
                    sock.shutdown(socket.SHUT_WR)
                    assert (
                        read_exact(sock, len(payload) + len(TRAILER))
                        == payload + TRAILER
                    )
                    assert sock.recv(1) == b""
                assert daemon.process.wait(timeout=15) == 0
                daemon.event("daemon_stopped")
                daemon.assert_removed()
            finally:
                for sock in active:
                    sock.close()
        finally:
            daemon.close()
        print("PASS lifecycle: reset, dual-stack drain and device release", flush=True)
        daemon = Daemon(
            args.binary,
            args.output / "forced",
            1500,
            cpus=args.daemon_cpus,
            shutdown_timeout=1,
        )
        try:
            echo.forced = True
            with client(socket.AF_INET6, socket.SOCK_STREAM) as active:
                assert read_exact(active, len(BANNER)) == BANNER
                daemon.finish(forced=True)
        finally:
            daemon.close()
        print("PASS lifecycle: forced shutdown", flush=True)
    finally:
        echo.close()
    directory = args.output / "startup-failure"
    directory.mkdir()
    path = directory / "main.ts"
    policy(path, 1500)
    path.write_text(
        path.read_text()
        + "\nk.inbound({implementation: k.tun_inbound({name: 'ktest0', mtu: 1500, addresses: []}), routing_handler: routing, udp_idle_timeout: k.timeout(30000)});\n"
    )
    result = subprocess.run(
        [str(args.binary), "run", "--config", str(path)],
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    (directory / "daemon.log").write_text(result.stderr)
    assert result.returncode != 0 and "already exists" in result.stderr, result.stderr
    Daemon.assert_removed()
    print("PASS lifecycle: failed startup releases device", flush=True)


def run(args):
    configure_network()
    summary = {
        "schema_version": 2,
        "suite": "tun-e2e",
        "seed": args.seed,
        "netns": os.readlink("/proc/self/ns/net"),
        "parent_netns": os.environ["KOTOCONN_TUN_PARENT_NETNS"],
        "binary_sha256": digest(args.binary),
        "traffic_sha256": digest(args.traffic_binary),
        "cases": [],
        "status": "running",
    }
    output = args.output / "results.json"
    selected = 0
    try:
        for mtu in args.mtu:
            daemon_dir = args.output / f"mtu-{mtu}"
            daemon = Daemon(args.binary, daemon_dir, mtu, cpus=args.daemon_cpus)
            try:
                for repetition in range(args.repeat):
                    for family in args.family:
                        for name, spec in cases(
                            mtu, family == 6, stress=args.profile == "stress"
                        ):
                            if args.case and name not in args.case:
                                continue
                            selected += 1
                            spec["seed"] = args.seed + repetition
                            case_id = f"v{family}-{name}-{repetition}"
                            directory = daemon_dir / case_id
                            directory.mkdir()
                            entry = {
                                "id": case_id,
                                "mtu": mtu,
                                "status": "running",
                                "result": str(
                                    (directory / "result.json").relative_to(args.output)
                                ),
                            }
                            summary["cases"].append(entry)
                            output.write_text(json.dumps(summary, indent=2) + "\n")
                            injector = (
                                Injector(family == 6, seed=spec["seed"])
                                if name == "mixed-malformed"
                                else None
                            )
                            try:
                                result = traffic(
                                    args.traffic_binary,
                                    daemon,
                                    directory,
                                    spec,
                                    inject=injector,
                                )
                                if injector:
                                    assert injector.counts["positive-control"] > 0, (
                                        "no mixed packets injected"
                                    )
                                    assert result["positive_controls"] > 0, (
                                        "raw injection positive control did not reach outbound"
                                    )
                                    entry["injected"] = dict(injector.counts)
                                    recovery = directory / "recovery"
                                    recovery.mkdir()
                                    traffic(
                                        args.traffic_binary,
                                        daemon,
                                        recovery,
                                        {
                                            "source": spec["source"],
                                            "target": spec["target"],
                                            "mtu": mtu,
                                            "protocol": "udp",
                                            "workload": "boundaries",
                                            "rounds": 4,
                                            "seed": spec["seed"],
                                        },
                                    )
                                entry.update(
                                    status="passed",
                                    resources_after=resource(daemon.process.pid),
                                )
                            except BaseException as error:
                                entry.update(
                                    status="failed",
                                    error=str(error),
                                    network=network_snapshot(),
                                )
                                raise
                            finally:
                                if injector:
                                    injector.close()
                            print(f"PASS mtu={mtu} {case_id}", flush=True)
                daemon.finish()
            finally:
                daemon.close()
        if not args.case or "lifecycle" in args.case:
            selected += 1
            lifecycle(args)
        if not args.case or "generic-relay" in args.case:
            selected += 1
            generic_relay(args, summary)
        if selected == 0:
            raise ValueError("no matching cases")
        summary["status"] = "passed"
    except BaseException:
        summary["status"] = "failed"
        raise
    finally:
        output.write_text(json.dumps(summary, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_arguments(parser)
    parser.add_argument("--profile", choices=("quick", "stress"), default="quick")
    parser.add_argument(
        "--mtu", action="append", type=int, choices=(1280, 1500, 9000, 65535)
    )
    parser.add_argument("--family", action="append", type=int, choices=(4, 6))
    parser.add_argument("--repeat", type=int)
    parser.add_argument(
        "--case", action="append", help="exact workload name; repeat to select several"
    )
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    args.traffic_binary = args.traffic_binary.resolve(strict=True)
    args.mtu = list(
        dict.fromkeys(
            args.mtu
            or ([1280, 1500, 9000, 65535] if args.profile == "stress" else [1500, 9000])
        )
    )
    args.family = list(dict.fromkeys(args.family or [4, 6]))
    args.repeat = (
        args.repeat
        if args.repeat is not None
        else (4 if args.profile == "stress" else 2)
    )
    if args.repeat < 1:
        parser.error("repeat must be positive")
    selected = [
        name
        for mtu in args.mtu
        for family in args.family
        for name, _ in cases(mtu, family == 6, stress=args.profile == "stress")
        if not args.case or name in args.case
    ]
    required_ports = sum(2 if name == "mixed-malformed" else 1 for name in selected)
    required_ports *= args.repeat
    if not args.case or "generic-relay" in args.case:
        required_ports += 4
    if not required_ports and args.case and "lifecycle" not in args.case:
        parser.error("no matching cases")
    if required_ports > len(SERVER_PORTS):
        parser.error(
            f"selected matrix needs {required_ports} traffic server ports; "
            f"only {len(SERVER_PORTS)} are available; reduce repeat or select fewer cases"
        )
    if not enter(args, Path(__file__).resolve(), "e2e"):
        run(args)


if __name__ == "__main__":
    main()
