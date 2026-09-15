"""Run two TUN containers concurrently and verify host networking is unchanged."""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
# Allow the launcher's two 5-second client waits and 15-second container removal,
# plus time for the runner to exit and the parent to observe completion.
CLEANUP_TIMEOUT = 35


def snapshot():
    def stable(value):
        if isinstance(value, dict):
            # DHCP/RA lifetimes count down without any network configuration change.
            return {
                key: stable(item)
                for key, item in value.items()
                if key not in ("valid_life_time", "preferred_life_time", "expires")
            }
        if isinstance(value, list):
            return [stable(item) for item in value]
        return value

    network = [
        stable(
            json.loads(
                subprocess.run(
                    ["ip", "-j", *args],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout
            )
        )
        for args in (
            ("address", "show"),
            ("-4", "rule", "show"),
            ("-6", "rule", "show"),
            ("-4", "route", "show", "table", "all"),
            ("-6", "route", "show", "table", "all"),
        )
    ]
    dns = None
    if shutil.which("resolvectl"):
        result = subprocess.run(
            ["resolvectl", "status"], capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            dns = result.stdout
    return {
        "dns": dns,
        "network": network,
        "sysctls": {
            name: Path(f"/proc/sys/net/ipv4/{name}").read_text()
            for name in (
                "conf/all/rp_filter",
                "conf/all/accept_local",
                "tcp_tw_reuse",
                "ip_local_port_range",
                "ip_local_reserved_ports",
            )
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", type=Path, default=ROOT / "target/tun/debug/kotoconn"
    )
    parser.add_argument(
        "--traffic-binary",
        type=Path,
        default=ROOT / "target/tun/debug/kotoconn-tun-traffic",
    )
    parser.add_argument("--container-image", default="kotoconn-tun:local")
    args = parser.parse_args()
    base = ROOT / "target/e2e"
    base.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="parallel-", dir=base))
    directory.chmod(0o755)
    before = snapshot()
    (directory / "parent-before.json").write_text(json.dumps(before, indent=2))
    parent = os.readlink("/proc/self/ns/net")
    children = []
    logs = []
    try:
        for index in range(2):
            log = (directory / f"runner-{index}.log").open("w")
            logs.append(log)
            children.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        str(ROOT / "e2e/tun.py"),
                        "--container-image",
                        args.container_image,
                        "--binary",
                        str(args.binary.resolve()),
                        "--traffic-binary",
                        str(args.traffic_binary.resolve()),
                        "--output",
                        str(directory),
                        "--mtu",
                        "1500",
                        "--family",
                        "4",
                        "--repeat",
                        "1",
                        "--case",
                        "mixed-malformed",
                        "--case",
                        "udp-boundaries",
                    ],
                    cwd=ROOT if index == 0 else directory,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
            )
        for child in children:
            if child.wait(timeout=90):
                raise RuntimeError(f"parallel run failed; see {directory}")
        results = [
            json.loads(path.read_text())
            for path in directory.glob("tun-*/results.json")
        ]
        assert len(results) == 2 and all(
            result["status"] == "passed" for result in results
        )
        assert len({result["netns"] for result in results}) == 2
        assert all(
            result["netns"] != parent and result["parent_netns"] == parent
            for result in results
        )
        # CI uses a rootful engine with a different container UID. Artifacts
        # must remain usable by the host runner, including in nested directories.
        for output in directory.glob("tun-*"):
            for path in output.rglob("*"):
                assert os.access(path, os.R_OK | os.W_OK), (
                    f"inaccessible artifact: {path}"
                )
                if path.is_dir():
                    assert os.access(path, os.X_OK), f"inaccessible directory: {path}"
        # Cancel a real started container and verify that it is removed, rather
        # than merely observing the host-side Docker client exit.
        cancel_output = directory / "cancelled"
        log = (directory / "cancelled.log").open("w")
        logs.append(log)
        child = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "e2e/tun.py"),
                "--binary",
                str(args.binary.resolve()),
                "--traffic-binary",
                str(args.traffic_binary.resolve()),
                "--container-image",
                args.container_image,
                "--output",
                str(cancel_output),
                "--case",
                "mixed-malformed",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        children.append(child)
        deadline = time.monotonic() + 15
        while True:
            cidfiles = list(cancel_output.glob("tun-*/container.cid"))
            identifier = cidfiles[0].read_text().strip() if cidfiles else ""
            if identifier:
                break
            if child.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError(f"container did not start; see {log.name}")
            time.sleep(0.01)
        child.terminate()
        assert child.wait(timeout=CLEANUP_TIMEOUT) != 0
        assert (
            subprocess.run(
                ["docker", "container", "inspect", identifier],
                capture_output=True,
                timeout=10,
            ).returncode
            != 0
        )

        after = snapshot()
        (directory / "parent-after.json").write_text(json.dumps(after, indent=2))
        assert after == before, (
            "host addresses, routes, rules, TCP settings or DNS changed"
        )
        print(
            f"PASS concurrent containers, cancellation cleanup and unchanged host networking: {directory}"
        )
    finally:
        for child in children:
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
                child.wait(timeout=CLEANUP_TIMEOUT)
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
