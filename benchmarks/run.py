"""Build and run the isolated TUN loopback benchmark."""

import argparse
import hashlib
import json
import os
import platform
import signal
import statistics
import subprocess
import threading
import time
from pathlib import Path


CLIENT = "192.0.2.2"
REMOTE = "198.18.0.1"
PORT = 5201
TUN = "bench0"
ROOT = Path(__file__).resolve().parent.parent
SING_BOX_REF = "68b74f9516a2b2e126065e71f31344e2802ce507"


def command(*args, check=True, timeout=15):
    return subprocess.run(
        args,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )


class Daemon:
    def __init__(self, argv, ready_text, log_path):
        self.lines = []
        self.condition = threading.Condition()
        self.log = log_path.open("w")
        self.process = subprocess.Popen(
            argv,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()
        self.wait_ready(ready_text)

    def read(self):
        for line in self.process.stdout:
            self.log.write(line)
            self.log.flush()
            with self.condition:
                self.lines.append(line)
                self.condition.notify_all()
        with self.condition:
            self.condition.notify_all()

    def wait_ready(self, ready_text):
        deadline = time.monotonic() + 20
        with self.condition:
            while ready_text not in "".join(self.lines):
                if self.process.poll() is not None:
                    raise RuntimeError("daemon exited:\n" + "".join(self.lines))
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("daemon readiness timed out")
                self.condition.wait(remaining)

    def close(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.reader.join(timeout=5)
        self.log.close()
        if self.process.returncode:
            raise RuntimeError(
                f"daemon exited with {self.process.returncode}:\n" + "".join(self.lines)
            )


def configure_network():
    command("ip", "link", "set", "lo", "up")
    command("ip", "addr", "add", f"{CLIENT}/32", "dev", "lo")
    command("ip", "addr", "add", f"{REMOTE}/32", "dev", "lo")
    command("ip", "rule", "add", "priority", "1000", "lookup", "local")
    command("ip", "rule", "del", "priority", "0")
    command(
        "ip",
        "rule",
        "add",
        "priority",
        "100",
        "uidrange",
        "1000-1000",
        "lookup",
        "100",
    )


def configure_tun_route():
    command("ip", "route", "replace", "default", "dev", TUN, "table", "100")


def wait_for_server(server):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        listeners = command("ss", "-H", "-ltn", check=False).stdout
        if f"{REMOTE}:{PORT}" in listeners:
            return
        if server.poll() is not None:
            raise RuntimeError("iperf3 server exited before listening")
        time.sleep(0.05)
    raise TimeoutError("iperf3 server did not listen")


def write_configs(directory):
    policy = directory / "kotoconn.ts"
    policy.write_text(
        """import { kotoconn as k } from '@kotoconn/bindings';
const resolver = k.resolve_handler(name => k.lookup(name));
const direct = k.dialer({dialer: null, outbound: {resolve_handler: resolver, implementation: k.direct_outbound({})}});
const routing = k.routing_handler(flow => flow.protocol === 'udp' ? k.route_udp(direct) : k.route(direct, flow.dest));
k.inbound({implementation: k.tun_inbound({name: 'bench0', mtu: 1500, addresses: [
    {address: k.ip('172.19.0.1'), prefix: 30}
]}), routing_handler: routing, udp_idle_timeout: k.timeout(30000)});
"""
    )
    config = {
        "log": {"level": "info", "timestamp": False},
        "inbounds": [
            {
                "type": "tun",
                "tag": "tun-in",
                "interface_name": TUN,
                "address": ["172.19.0.1/30"],
                "mtu": 1500,
                "auto_route": False,
                "stack": "go",
            }
        ],
        "outbounds": [{"type": "direct", "tag": "direct"}],
        "route": {"final": "direct"},
    }
    (directory / "sing-box.json").write_text(json.dumps(config, indent=2))


def start_daemon(implementation, directory):
    log_path = directory / f"{implementation}.log"
    if implementation == "kotoconn":
        return Daemon(
            [
                "/opt/kotoconn",
                "--log-format",
                "json",
                "run",
                "--config",
                str(directory / "kotoconn.ts"),
            ],
            '"event":"daemon_ready"',
            log_path,
        )
    return Daemon(
        ["/opt/sing-box", "run", "-c", str(directory / "sing-box.json")],
        "sing-box started",
        log_path,
    )


def iperf(direction, streams, duration):
    argv = [
        "setpriv",
        "--reuid=1000",
        "--regid=1000",
        "--clear-groups",
        "iperf3",
        "--client",
        REMOTE,
        "--port",
        str(PORT),
        "--bind",
        CLIENT,
        "--parallel",
        str(streams),
        "--omit",
        "1",
        "--time",
        str(duration),
        "--json",
    ]
    if direction == "download":
        argv.append("--reverse")
    result = command(*argv, check=False, timeout=duration + 20)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"iperf3 returned invalid JSON:\n{result.stdout}") from error
    if result.returncode or "error" in payload:
        raise RuntimeError(f"iperf3 failed:\n{result.stdout}")
    summary = payload["end"]["sum_received"]
    return {
        "bits_per_second": summary["bits_per_second"],
        "bytes": summary["bytes"],
        "seconds": summary["seconds"],
    }


def read_text(path):
    try:
        return Path(path).read_text().strip()
    except FileNotFoundError:
        return None


def process_cpu_seconds(process):
    fields = Path(f"/proc/{process.pid}/stat").read_text().rsplit(")", 1)[1].split()
    ticks = int(fields[11]) + int(fields[12])
    return ticks / os.sysconf("SC_CLK_TCK")


def link_stats():
    link = json.loads(command("ip", "-j", "-s", "link", "show", "dev", TUN).stdout)[0]
    return {
        f"{direction}_{metric}": link["stats64"][direction][metric]
        for direction in ("rx", "tx")
        for metric in ("bytes", "packets")
    }


def metadata(duration, repetitions):
    cpu_model = "unknown"
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name"):
            cpu_model = line.split(":", 1)[1].strip()
            break
    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "cpu_model": cpu_model,
        "available_cpus": len(os.sched_getaffinity(0)),
        "cpu_max": read_text("/sys/fs/cgroup/cpu.max"),
        "memory_max": read_text("/sys/fs/cgroup/memory.max"),
        "mtu": 1500,
        "duration_seconds": duration,
        "omit_seconds": 1,
        "repetitions": repetitions,
        "sing_box_version": command("/opt/sing-box", "version").stdout.splitlines()[0],
        "sing_box_commit": os.environ.get("SING_BOX_COMMIT"),
        "kotoconn_version": command("/opt/kotoconn", "--version").stdout.strip(),
        "kotoconn_commit": os.environ.get("KOTOCONN_COMMIT"),
        "kotoconn_source_diff_sha256": os.environ.get("KOTOCONN_SOURCE_DIFF_SHA256"),
    }


def run_inside(output, duration, repetitions):
    output.parent.mkdir(parents=True, exist_ok=True)
    work = output.parent / "run"
    work.mkdir(exist_ok=True)

    configure_network()
    write_configs(work)
    server_log = (work / "iperf3-server.log").open("w")
    server = subprocess.Popen(
        ["iperf3", "--server", "--bind", REMOTE, "--port", str(PORT)],
        stdout=server_log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    wait_for_server(server)

    results = {
        "schema_version": 1,
        "suite": "tun-loopback",
        "metadata": metadata(duration, repetitions),
        "runs": [],
    }
    implementations = ("kotoconn", "sing-box-go")
    try:
        for implementation in implementations:
            print(f"Starting {implementation}", flush=True)
            daemon = start_daemon(implementation, work)
            try:
                configure_tun_route()
                for streams in (1, 4):
                    for direction in ("upload", "download"):
                        values = []
                        for repetition in range(1, repetitions + 1):
                            cpu_before = process_cpu_seconds(daemon.process)
                            link_before = link_stats()
                            wall_before = time.monotonic()
                            value = iperf(direction, streams, duration)
                            wall_seconds = time.monotonic() - wall_before
                            cpu_seconds = process_cpu_seconds(daemon.process) - cpu_before
                            link_after = link_stats()
                            value.update(
                                implementation=implementation,
                                direction=direction,
                                streams=streams,
                                repetition=repetition,
                                daemon_cpu_seconds=cpu_seconds,
                                daemon_cpu_cores=cpu_seconds / wall_seconds,
                                daemon_cpu_ns_per_byte=cpu_seconds * 1e9 / value["bytes"],
                                **{
                                    f"tun_{key}": link_after[key] - link_before[key]
                                    for key in link_before
                                },
                            )
                            results["runs"].append(value)
                            output.write_text(json.dumps(results, indent=2) + "\n")
                            values.append(value["bits_per_second"] / 1e9)
                            print(
                                f"  {streams} stream(s) {direction} {repetition}: "
                                f"{values[-1]:.3f} Gbit/s",
                                flush=True,
                            )
                        print(f"  median: {statistics.median(values):.3f} Gbit/s")
            finally:
                daemon.close()
    finally:
        server.send_signal(signal.SIGTERM)
        server.wait(timeout=5)
        server_log.close()

    output.write_text(json.dumps(results, indent=2) + "\n")
    print(f"Results: {output}", flush=True)


def run_checked(argv, *, cwd=None, env=None):
    subprocess.run(argv, cwd=cwd, env=env, check=True)


def run_host(output, duration, repetitions):
    sing_box_source = ROOT / "target/sing-box"
    sing_box_binary = ROOT / "target/sing-box-main"
    kotoconn_binary = (
        ROOT / "target/x86_64-unknown-linux-musl/release/kotoconn"
    )
    sing_box_ref = os.environ.get("SING_BOX_REF", SING_BOX_REF)

    output.parent.mkdir(parents=True, exist_ok=True)
    if not (sing_box_source / ".git").is_dir():
        run_checked(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                "testing",
                "--filter=blob:none",
                "https://github.com/SagerNet/sing-box.git",
                str(sing_box_source),
            ]
        )
    run_checked(
        ["git", "fetch", "--depth", "1", "origin", sing_box_ref],
        cwd=sing_box_source,
    )
    run_checked(["git", "checkout", "--detach", "FETCH_HEAD"], cwd=sing_box_source)

    run_checked(
        [
            "cargo",
            "zigbuild",
            "--release",
            "--locked",
            "-p",
            "kotoconn-cli",
            "--target",
            "x86_64-unknown-linux-musl",
        ],
        cwd=ROOT,
    )
    sing_box_commit = command(
        "git", "-C", str(sing_box_source), "rev-parse", "HEAD", timeout=10
    ).stdout.strip()
    go_env = dict(os.environ, CGO_ENABLED="0")
    run_checked(
        [
            "go",
            "build",
            "-trimpath",
            "-tags",
            "with_gvisor",
            "-ldflags",
            f"-X github.com/sagernet/sing-box/constant.Version=main-{sing_box_commit[:12]} -s -w -buildid=",
            "-o",
            str(sing_box_binary),
            "./cmd/sing-box",
        ],
        cwd=sing_box_source,
        env=go_env,
    )
    run_checked(
        [
            "docker",
            "build",
            "--tag",
            "kotoconn-tun-benchmark:local",
            "--file",
            str(ROOT / "benchmarks/tun-loopback/Dockerfile"),
            str(ROOT),
        ]
    )

    source_diff = command(
        "git",
        "-C",
        str(ROOT),
        "diff",
        "--",
        "crates",
        "Cargo.toml",
        "Cargo.lock",
        timeout=10,
    ).stdout.encode()
    source_digest = hashlib.sha256(source_diff).hexdigest()
    kotoconn_commit = command(
        "git", "-C", str(ROOT), "rev-parse", "HEAD", timeout=10
    ).stdout.strip()
    run_checked(
        [
            "docker",
            "run",
            "--rm",
            "--cap-add",
            "NET_ADMIN",
            "--device",
            "/dev/net/tun",
            "--cpus",
            "4",
            "--memory",
            "2g",
            "--sysctl",
            "net.ipv4.conf.all.rp_filter=0",
            "--sysctl",
            "net.ipv4.conf.default.rp_filter=0",
            "--sysctl",
            "net.ipv4.conf.all.accept_local=1",
            "--sysctl",
            "net.ipv4.conf.default.accept_local=1",
            "--env",
            f"KOTOCONN_COMMIT={kotoconn_commit}",
            "--env",
            f"KOTOCONN_SOURCE_DIFF_SHA256={source_digest}",
            "--env",
            f"SING_BOX_COMMIT={sing_box_commit}",
            "--volume",
            f"{kotoconn_binary}:/opt/kotoconn:ro",
            "--volume",
            f"{sing_box_binary}:/opt/sing-box:ro",
            "--volume",
            f"{output.parent}:/output",
            "kotoconn-tun-benchmark:local",
            "--inside",
            "--output",
            f"/output/{output.name}",
            "--duration",
            str(duration),
            "--repetitions",
            str(repetitions),
        ]
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--duration", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args()

    if args.inside:
        run_inside(
            args.output or Path("/output/results.json"),
            args.duration,
            args.repetitions,
        )
    else:
        run_host(
            (
                args.output
                or ROOT / "target/benchmarks/tun-loopback/results.json"
            ).resolve(),
            args.duration,
            args.repetitions,
        )


if __name__ == "__main__":
    main()
