"""Run TUN workloads inside disposable Docker containers with no host network or bus."""

import argparse
import hashlib
import itertools
import json
import os
import queue
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NAME = "ktest0"
CLIENT = ("192.0.2.2", "fd00::2")
REMOTE = ("198.18.0.1", "2001:db8::1")
SERVER_PORTS = range(12000, 20000)
PORTS = itertools.count(SERVER_PORTS.start)


def isolated():
    parent = os.environ.get("KOTOCONN_TUN_PARENT_NETNS")
    container = Path("/.dockerenv").exists() or Path("/run/.containerenv").exists()
    if (
        not container
        or os.environ.get("KOTOCONN_TUN_CONTAINER") != "1"
        or not parent
        or os.readlink("/proc/self/ns/net") == parent
        or Path("/run/dbus/system_bus_socket").exists()
    ):
        raise RuntimeError(
            "TUN workloads require the Docker launcher, a private network, and no host D-Bus"
        )


def command(*argv, check=True):
    return subprocess.run(argv, check=check, text=True, capture_output=True, timeout=15)


def ip(*argv):
    isolated()
    return command("ip", *argv).stdout


def enter(args, script, category, *, sysctls=None):
    """Start one container per run; only its unique artifact directory is writable."""
    if args.inside:
        isolated()
        # Keep nested artifacts writable by the directory's inherited group.
        os.umask(0o002)
        return False
    base = (args.output or ROOT / "target" / category).resolve()
    base.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="tun-", dir=base))
    # Rootful Docker's UID 0 differs from the host runner. Grant the directory's
    # group write access and inherit that group for files created by either side.
    directory.chmod(0o2770)
    image = command(
        "docker", "image", "inspect", args.container_image, "--format", "{{.Id}}"
    )
    metadata = {
        "image": args.container_image,
        "image_id": image.stdout.strip(),
        "engine": command("docker", "--version").stdout.strip(),
        "source_commit": command(
            "git", "-C", str(ROOT), "rev-parse", "HEAD"
        ).stdout.strip(),
        "source_status": command(
            "git", "-C", str(ROOT), "status", "--porcelain"
        ).stdout,
        "smoltcp": command(
            "git", "-C", str(ROOT), "submodule", "status"
        ).stdout.strip(),
    }
    cidfile = directory / "container.cid"
    container_name = f"kotoconn-{directory.name}-{os.getpid()}"
    argv = [
        "docker",
        "run",
        "--rm",
        "--init",
        "--network",
        "none",
        "--read-only",
        "--name",
        container_name,
        "--cap-drop",
        "ALL",
        "--cap-add",
        "NET_ADMIN",
        "--cap-add",
        "NET_RAW",
        "--group-add",
        str(directory.stat().st_gid),
        "--device",
        "/dev/net/tun",
        "--security-opt",
        "no-new-privileges",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev",
        "--cidfile",
        str(cidfile),
        "--env",
        "KOTOCONN_TUN_CONTAINER=1",
        "--env",
        f"KOTOCONN_TUN_PARENT_NETNS={os.readlink('/proc/self/ns/net')}",
        "--mount",
        f"type=bind,source={ROOT},target=/workspace,readonly",
        "--mount",
        f"type=bind,source={directory},target=/artifacts",
    ]
    settings = {
        "net.ipv4.conf.all.rp_filter": "0",
        "net.ipv4.conf.default.rp_filter": "0",
        "net.ipv4.conf.all.accept_local": "1",
        "net.ipv4.conf.default.accept_local": "1",
        "net.ipv4.tcp_tw_reuse": "1",
        "net.ipv4.ip_local_port_range": f"{SERVER_PORTS.stop} 65535",
        "net.ipv4.ip_local_reserved_ports": "22222-22224",
        **(sysctls or {}),
    }
    for name, value in settings.items():
        argv += ["--sysctl", f"{name}={value}"]
    # Preserve an explicit generator affinity while leaving daemon affinity
    # independently selectable inside the same container CPU allowance.
    generator_cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))))
    overrides = []
    libraries = set()
    for option in ("binary", "traffic_binary", "sing_box"):
        binary = getattr(args, option, None)
        if binary is None:
            continue
        binary = binary.resolve(strict=True)
        target = f"/inputs/{option}"
        argv += ["--mount", f"type=bind,source={binary},target={target},readonly"]
        overrides += ["--" + option.replace("_", "-"), target]
        # Nix ELF binaries name their immutable loader and libraries by store
        # path. Mount only those runtime packages, never /run or the host root.
        linked = command("ldd", str(binary), check=False)
        libraries.update(re.findall(r"/nix/store/[^/\s]+", linked.stdout))
    for path in sorted(libraries):
        argv += ["--mount", f"type=bind,source={path},target={path},readonly"]
    argv += [
        "--entrypoint",
        "taskset",
        args.container_image,
        "-c",
        generator_cpus,
        "python3",
        str(Path("/workspace") / script.relative_to(ROOT)),
        *sys.argv[1:],
        *overrides,
        "--inside",
        "--output",
        "/artifacts",
    ]
    metadata.update(command=argv, sysctls=settings, runtime_packages=sorted(libraries))
    (directory / "container.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"Artifacts: {directory}", flush=True)
    child = subprocess.Popen(argv, start_new_session=True)

    def interrupted(_signal, _frame):
        raise KeyboardInterrupt

    # SIGTERM must run the same child cleanup as Ctrl-C, including in CI.
    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        code = child.wait()
    finally:
        signal.signal(signal.SIGTERM, previous)
        # Stop the launcher before cleanup so cancellation during startup cannot
        # leave it creating a container after we checked for its cidfile.
        if child.poll() is None:
            child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
        # The unique name also identifies a container cancelled before its ID
        # was written. Never search by image or target another run's container.
        command("docker", "rm", "--force", container_name, check=False)
    if code:
        raise SystemExit(code)
    return True


def configure_network():
    isolated()
    ip("link", "set", "lo", "up")
    for index, family in enumerate(("-4", "-6")):
        for address in (CLIENT[index], REMOTE[index]):
            ip(
                family,
                "addr",
                "add",
                address + ("/32" if index == 0 else "/128"),
                "dev",
                "lo",
                *([] if index == 0 else ["nodad"]),
            )
        ip(family, "rule", "add", "priority", "1000", "lookup", "local")
        ip(family, "rule", "del", "priority", "0")
        # Source rules retain the proxy route for kernel-generated TIME_WAIT ACKs.
        ip(
            family,
            "rule",
            "add",
            "priority",
            "100",
            "from",
            CLIENT[index],
            "lookup",
            "100",
        )


def configure_routes():
    for family in ("-4", "-6"):
        ip(family, "route", "replace", "default", "dev", NAME, "table", "100")


def policy(path, mtu, idle_ms=30000, outbound="direct"):
    path.write_text(f"""import {{ kotoconn as k }} from '@kotoconn/bindings';
const resolver = k.resolve_handler(name => k.lookup(name));
const direct = k.dialer({{dialer: null, outbound: {{resolve_handler: resolver, implementation: k.direct_outbound({{}})}}}});
const routing = k.routing_handler(flow => flow.protocol === 'udp' ? k.route_udp(direct) : k.route(direct, flow.dest));
k.inbound({{implementation: k.tun_inbound({{name: '{NAME}', mtu: {mtu}, addresses: [
    {{address: k.ip('192.0.2.1'), prefix: 30}}, {{address: k.ip('fd00::1'), prefix: 126}}
]}}), routing_handler: routing, udp_idle_timeout: k.timeout({idle_ms})}});
""")
    if outbound == "socks5":
        source = path.read_text()
        point = source.index("k.inbound(")
        relay = """k.inbound({implementation: k.socks5_inbound({listen: {address: k.ip('127.0.0.1'), port: 9100}}), routing_handler: routing, udp_idle_timeout: k.timeout(30000)});
const proxy = k.dialer({dialer: null, outbound: {resolve_handler: resolver, implementation: k.socks5_outbound({server: k.ip_target(k.ip('127.0.0.1'), 9100)})}});
const tunRouting = k.routing_handler(flow => flow.protocol === 'udp' ? k.route_udp(proxy) : k.route(proxy, flow.dest));
"""
        path.write_text(
            source[:point]
            + relay
            + source[point:].replace(
                "routing_handler: routing", "routing_handler: tunRouting"
            )
        )


class Process:
    def __init__(self, argv, log, *, json_output=False, env=None):
        self.events = queue.Queue()
        self.lines = []
        self.log = log.open("w")
        self.process = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log if json_output else subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()

    def read(self):
        try:
            for line in self.process.stdout:
                self.log.write(line)
                self.log.flush()
                self.lines.append(line)
                try:
                    value = json.loads(line)
                    self.events.put(value)
                except json.JSONDecodeError:
                    self.events.put({"text": line})
        finally:
            self.events.put(None)

    def event(self, name, timeout=30):
        end = time.monotonic() + timeout
        while True:
            try:
                value = self.events.get(timeout=max(0, end - time.monotonic()))
            except queue.Empty as error:
                raise TimeoutError(
                    f"process did not report {name}; see {self.log.name}"
                ) from error
            if value is None:
                raise RuntimeError(f"process exited before {name}; see {self.log.name}")
            if value.get("event", value.get("fields", {}).get("event")) == name:
                return value

    def send(self, value):
        self.process.stdin.write(value + "\n")
        self.process.stdin.flush()

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.thread.join(timeout=5)
        self.process.stdin.close()
        self.process.stdout.close()
        self.log.close()


class Daemon(Process):
    def __init__(
        self,
        binary,
        directory,
        mtu,
        *,
        cpus=None,
        shutdown_timeout=5,
        idle_ms=30000,
        outbound="direct",
    ):
        directory.mkdir(parents=True, exist_ok=False)
        policy(directory / "main.ts", mtu, idle_ms, outbound)
        argv = [
            str(binary),
            "--log-format",
            "json",
            "run",
            "--config",
            str(directory / "main.ts"),
            "--shutdown-timeout",
            str(shutdown_timeout),
        ]
        if cpus:
            argv = ["taskset", "--cpu-list", cpus, *argv]
        super().__init__(
            argv,
            directory / "daemon.log",
            env=dict(os.environ, RUST_LOG="info,kotoconn_tun=debug"),
        )
        try:
            self.event("daemon_ready")
            configure_routes()
        except BaseException:
            self.close()
            raise

    def finish(self, forced=False):
        self.process.send_signal(signal.SIGTERM)
        self.event("daemon_stopping")
        code = self.process.wait(timeout=15)
        if (code != 0) != forced:
            raise RuntimeError(f"unexpected shutdown exit {code}; see {self.log.name}")
        if not forced:
            self.event("daemon_stopped")
        self.assert_removed()

    @staticmethod
    def assert_removed():
        if any(item["ifname"] == NAME for item in json.loads(ip("-j", "link", "show"))):
            raise AssertionError("TUN survived daemon exit")


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def network_snapshot():
    return {
        "links": json.loads(ip("-j", "-s", "-d", "link", "show")),
        "rules_v4": ip("-4", "rule", "show"),
        "rules_v6": ip("-6", "rule", "show"),
        "tcp": command("ss", "-tin").stdout,
        "snmp": Path("/proc/net/snmp").read_text(),
        "snmp6": Path("/proc/net/snmp6").read_text(),
    }


def resource(pid):
    root = Path(f"/proc/{pid}")
    fields = (root / "stat").read_text().rsplit(")", 1)[1].split()
    status = dict(
        line.split(":", 1) for line in (root / "status").read_text().splitlines()
    )
    try:
        rollup = (root / "smaps_rollup").read_text().splitlines()
        pss = next(
            int(line.split()[1]) * 1024 for line in rollup if line.startswith("Pss:")
        )
    except PermissionError:
        pss = None
    return {
        "user_seconds": int(fields[11]) / os.sysconf("SC_CLK_TCK"),
        "system_seconds": int(fields[12]) / os.sysconf("SC_CLK_TCK"),
        "rss_bytes": int(status["VmRSS"].split()[0]) * 1024,
        "pss_bytes": pss,
        "peak_rss_bytes": int(status["VmHWM"].split()[0]) * 1024,
        "threads": int(status["Threads"]),
        "fds": len(list((root / "fd").iterdir())),
        "voluntary_switches": int(status["voluntary_ctxt_switches"]),
        "involuntary_switches": int(status["nonvoluntary_ctxt_switches"]),
    }


def reserve_server_port():
    port = next(PORTS)
    if port not in SERVER_PORTS:
        raise ValueError(
            f"run exhausted its {len(SERVER_PORTS)} reserved traffic server ports"
        )
    return port


def traffic(binary, daemon, directory, spec, *, inject=None):
    # TCP needs fresh tuple space after TIME_WAIT. UDP warmup and measurement
    # can explicitly share a server port to reuse their live associations.
    spec = dict(spec)
    if "port" not in spec:
        spec["port"] = reserve_server_port()
    if spec["port"] not in SERVER_PORTS:
        raise ValueError(
            f"run exhausted its {len(SERVER_PORTS)} reserved traffic server ports"
        )
    if inject:
        inject.port = spec["port"]
    argv = [str(binary), "--controlled"]
    for key, value in spec.items():
        argv += ["--" + key.replace("_", "-")]
        if value is not True:
            argv += [str(value)]
    tool = Process(argv, directory / "traffic.log", json_output=True)
    stop = threading.Event()
    samples = []
    errors = []

    def sample():
        try:
            while not stop.wait(0.1):
                samples.append(
                    {
                        "seconds": time.monotonic() - started,
                        "daemon": resource(daemon.process.pid),
                        "generator": resource(tool.process.pid),
                    }
                )
                if inject:
                    inject.send()
        except BaseException as error:  # noqa: BLE001 - re-raised in the controlling thread
            errors.append(error)

    thread = None
    try:
        tool.event("ready")
        before = {
            "daemon": resource(daemon.process.pid),
            "generator": resource(tool.process.pid),
        }
        network_before = network_snapshot()
        started = time.monotonic()
        tool.send("start")
        thread = threading.Thread(target=sample, daemon=True)
        thread.start()
        result = tool.event(
            "complete",
            timeout=spec.get("duration_ms", 0) / 1000 + spec.get("timeout", 30) + 10,
        )
        wall = time.monotonic() - started
        after = {
            "daemon": resource(daemon.process.pid),
            "generator": resource(tool.process.pid),
        }
        stop.set()
        thread.join()
        if errors:
            raise errors[0]
        result.update(
            wall_seconds=wall,
            resources={"before": before, "after": after, "samples": samples},
            network_before=network_before,
            network_after=network_snapshot(),
            spec=spec,
        )
        tool.send("finish")
        if tool.process.wait(timeout=5):
            raise RuntimeError(f"traffic process failed; see {tool.log.name}")
        (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        return result
    finally:
        stop.set()
        if thread:
            thread.join(timeout=5)
        tool.close()


def add_arguments(parser, *, release=False):
    profile = "release" if release else "debug"
    parser.add_argument(
        "--binary", type=Path, default=ROOT / "target" / profile / "kotoconn"
    )
    parser.add_argument(
        "--traffic-binary",
        type=Path,
        default=ROOT / "target" / profile / "kotoconn-tun-traffic",
    )
    parser.add_argument(
        "--output", type=Path, help="parent directory; each run gets a unique child"
    )
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--container-image",
        default="kotoconn-tun:local",
        help="runtime image built with e2e/tun.Dockerfile",
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--daemon-cpus",
        default=",".join(str(cpu) for cpu in sorted(os.sched_getaffinity(0))[:2]),
        help="process CPU affinity, without reserving host CPUs",
    )
