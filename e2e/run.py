"""Run isolated Compose scenarios using only Python's standard library."""

import argparse
import concurrent.futures
import json
import logging
import os
import shlex
import signal
import subprocess
import time
import uuid
from pathlib import Path

from lifecycle import has_event

LOGGER = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
KEY = "AAECAwQFBgcICQoLDA0ODw=="
SUITES = ("http", "socks5", "shadowsocks2022", "nested", "typescript")


def sing_protocol(kind, server=None):
    result = {
        "type": {"socks5": "socks", "shadowsocks2022": "shadowsocks"}.get(kind, kind)
    }
    if server:
        result.update(server=server, server_port=1080)
        if kind == "socks5":
            result["version"] = "5"
    else:
        result.update(listen="0.0.0.0", listen_port=1080)
        # Replies include 8 KiB datagrams, larger than a Docker bridge MTU.
        if kind != "http":
            result["udp_fragment"] = True
    if kind == "shadowsocks2022":
        result.update(method="2022-blake3-aes-128-gcm", password=KEY)
    return result


def sing_config(inbounds, outbound):
    return {
        "log": {"level": "info"},
        "dns": {"servers": [{"type": "local", "tag": "local"}]},
        "inbounds": inbounds,
        # sing-box disables UDP fragmentation by default on outbound sockets.
        "outbounds": [dict(outbound, tag="out", udp_fragment=True)],
        "route": {"final": "out", "default_domain_resolver": "local"},
    }


def policy(inbound, chain, rewrite=False):
    source = """import { kotoconn as k } from '@kotoconn/bindings';
import { destination } from './routing.ts';
const resolver = k.resolve_handler(async name => await k.lookup(name));
const listenPort: number = await Promise.resolve(1080);
"""
    previous = "null"
    for index, hop in enumerate(chain):
        kind, server = hop[:2]
        port = hop[2] if len(hop) == 3 else 1080
        if kind == "direct":
            config = "{}"
        else:
            # Resolve at startup so the generated policy also exercises top-level await.
            source += f"const address{index} = (await k.lookup('{server}'))[0];\n"
            config = f"{{server: k.ip_target(address{index}, {port})"
            if kind == "shadowsocks2022":
                config += f", password: '{KEY}'"
            config += "}"
        source += f"const d{index} = k.dialer({{dialer: {previous}, outbound: {{resolve_handler: resolver, implementation: k.{kind}_outbound({config})}}}});\n"
        previous = f"d{index}"
    source += f"""const routing = k.routing_handler(async flow => {{
    {"if (flow.dest.port === 9002) return k.reject();" if rewrite else ""}
    const target = await destination(flow.dest);
    return flow.protocol === 'udp' ? k.route_udp({previous}) : k.route({previous}, target);
}});
k.inbound({{implementation: k.{inbound}_inbound({{listen: {{address: k.ip('0.0.0.0'), port: listenPort}}{", password: '" + KEY + "'" if inbound == "shadowsocks2022" else ""}}}), routing_handler: routing, udp_idle_timeout: k.timeout(30000)}});
"""
    # The TypeScript suite changes the TCP destination. UDP cannot rewrite targets.
    routing = """import { kotoconn as k } from '@kotoconn/bindings';
export async function destination(target: {ip: ReturnType<typeof k.ip>, port: number}) {
    if (target.port === 9003) throw new Error('e2e handler failure');
    const port: number = REWRITE ? 9001 : target.port;
    const address = REWRITE ? (await k.lookup('echo'))[0] : target.ip;
    return await Promise.resolve(k.ip_target(address, port));
}
""".replace("REWRITE", "true" if rewrite else "false")
    return source, routing


def write_policy(directory, inbound, chain, rewrite=False):
    directory.mkdir(parents=True, exist_ok=True)
    main, routing = policy(inbound, chain, rewrite)
    (directory / "main.ts").write_text(main)
    (directory / "routing.ts").write_text(routing)


class Scenario:
    def __init__(self, args, suite, direction):
        self.suite, self.direction = suite, direction
        self.engine = shlex.split(args.engine)
        self.name = f"{suite}-{direction}"
        self.directory = args.output / self.name
        self.directory.mkdir(parents=True)
        self.env = dict(
            os.environ, CASE_DIR=str(self.directory), KOTOCONN_IMAGE=args.image
        )
        self.command = shlex.split(args.compose) + [
            "-p",
            f"koto-{args.run_id}-{self.name}",
            "-f",
            str(ROOT / "compose.yaml"),
        ]
        if suite == "nested":
            override = self.directory / "compose.json"
            override.write_text(
                json.dumps({"services": {"peer": {"network_mode": "service:gateway"}}})
            )
            self.command += ["-f", str(override), "--profile", "nested"]

    def compose(self, *args, check=True, timeout=120):
        result = subprocess.run(
            self.command + list(args),
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
        with (self.directory / "runner.log").open("a") as log:
            log.write(f"$ {' '.join(args)}\n{result.stdout}\n")
        if check and result.returncode:
            raise RuntimeError(
                f"{self.name}: {' '.join(args)} failed:\n{result.stdout}"
            )
        return result

    def ready(self, service):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            logs = self.compose("logs", "--no-color", "--no-log-prefix", service).stdout
            ready = (
                has_event(logs.splitlines(), "daemon_ready")
                if service in ("kotoconn", "gateway")
                else "sing-box started" in logs
            )
            if ready:
                return
            status = self.compose("ps", "--all", "--format", "json", service).stdout
            # Docker Compose emits either a JSON array or one object per line.
            rows = (
                json.loads(status)
                if status.lstrip().startswith("[")
                else [
                    json.loads(line)
                    for line in status.splitlines()
                    if line.startswith("{")
                ]
            )
            if any(row.get("State") in ("exited", "dead") for row in rows):
                raise RuntimeError(
                    f"{self.name}: {service} exited before readiness:\n{logs}"
                )
            # Poll external state; readiness is established by the startup event.
            time.sleep(0.1)
        raise TimeoutError(f"{self.name}: {service} did not become ready")

    def configure(self, target):
        kind = self.suite if self.suite in SUITES[:3] else "socks5"
        inbound = kind if self.direction == "server" else "socks5"
        if self.suite == "nested":
            chain = [("shadowsocks2022", "gateway"), ("socks5", "127.0.0.1", 1081)]
            write_policy(
                self.directory / "gateway", "shadowsocks2022", [("direct", "")]
            )
        elif self.direction == "server" or self.suite == "typescript":
            chain = [("direct", "")]
        else:
            chain = [(kind, "peer")]
        write_policy(
            self.directory / "kotoconn", inbound, chain, self.suite == "typescript"
        )
        entry = sing_config(
            [
                {
                    "type": "direct",
                    "listen": "0.0.0.0",
                    "listen_port": 10080 + index,
                    "override_address": target,
                    "override_port": 9000 + index,
                }
                for index in range(4 if self.suite == "typescript" else 2)
            ],
            sing_protocol(inbound, "kotoconn"),
        )
        peer_inbound = sing_protocol(kind)
        if self.suite == "nested":
            # Only the gateway can reach this loopback listener. Ignoring the
            # lower Shadowsocks dialer must fail instead of silently going direct.
            peer_inbound.update(listen="127.0.0.1", listen_port=1081)
        peer = sing_config([peer_inbound], {"type": "direct"})
        (self.directory / "entry.json").write_text(json.dumps(entry, indent=2))
        (self.directory / "peer.json").write_text(json.dumps(peer, indent=2))

    def run(self):
        print(f"START {self.name}", flush=True)
        try:
            self.compose("up", "-d", "--wait", "--wait-timeout", "45", "echo")
            target = self.compose(
                "exec",
                "-T",
                "echo",
                "python",
                "-c",
                "import socket; print(socket.gethostbyname(socket.gethostname()))",
            ).stdout.strip()
            self.configure(target)
            services = (
                (["gateway"] if self.suite == "nested" else [])
                + (
                    ["peer"]
                    if self.direction == "client" or self.suite == "nested"
                    else []
                )
                + ["kotoconn", "entry"]
            )
            for service in services:
                self.compose("up", "-d", service)
                self.ready(service)
            for transport in ("tcp", "udp"):
                if transport == "udp" and self.suite == "http":
                    continue  # HTTP CONNECT has no UDP support.
                command = [
                    "exec",
                    "-T",
                    "echo",
                    "python",
                    "/tests/traffic.py",
                    transport,
                    "--egress",
                    "gateway"
                    if self.suite == "nested"
                    else "peer"
                    if self.direction == "client"
                    else "kotoconn",
                ]
                if self.suite == "typescript" and transport == "tcp":
                    command.append("--rewrite")
                result = self.compose(*command, timeout=90)
                print(f"{self.name}: {result.stdout.strip()}", flush=True)
            if self.suite == "typescript":
                self.compose(
                    "exec", "-T", "echo", "python", "/tests/traffic.py", "reject"
                )
                # A rejected request or throwing handler must not stop the daemon.
                self.compose(
                    "exec",
                    "-T",
                    "echo",
                    "python",
                    "/tests/traffic.py",
                    "tcp",
                    "--rewrite",
                )
            if self.suite == "nested":
                peer_logs = self.compose("logs", "--no-color", "peer").stdout
                for connection in ("connection", "packet connection"):
                    for port in (9000, 9001):
                        expected = (
                            f"inbound/socks[0]: inbound {connection} to {target}:{port}"
                        )
                        if expected not in peer_logs:
                            raise AssertionError(
                                f"nested SOCKS hop was not used: {expected}"
                            )
            self.compose("stop", "-t", "8", "entry")
            # Exercise actual CLI signal handling and successful process exit.
            for service in (
                ("kotoconn", "gateway") if self.suite == "nested" else ("kotoconn",)
            ):
                self.compose("stop", "-t", "8", service)
                logs = self.compose(
                    "logs", "--no-color", "--no-log-prefix", service
                ).stdout
                if not has_event(logs.splitlines(), "daemon_stopped"):
                    raise AssertionError(f"{service} did not shut down cleanly")
                container = self.compose(
                    "ps", "--all", "--quiet", service
                ).stdout.strip()
                inspect = subprocess.run(
                    self.engine
                    + ["inspect", "--format", "{{.State.ExitCode}}", container],
                    text=True,
                    capture_output=True,
                    check=True,
                    timeout=15,
                )
                if inspect.stdout.strip() != "0":
                    raise AssertionError(f"{service} exit code: {inspect.stdout}")
            if self.suite == "typescript":
                for index, (source, expected) in enumerate(
                    (
                        (
                            "throw new Error('e2e startup failure');",
                            "e2e startup failure",
                        ),
                        ("import './missing.ts';", "missing.ts"),
                    )
                ):
                    (self.directory / "kotoconn" / f"failure{index}.ts").write_text(
                        source
                    )
                    result = self.compose(
                        "run",
                        "--rm",
                        "--no-deps",
                        "kotoconn",
                        "run",
                        "--config",
                        f"/config/failure{index}.ts",
                        check=False,
                        timeout=30,
                    )
                    if result.returncode == 0 or expected not in result.stdout:
                        raise AssertionError(
                            f"startup error not propagated: {result.stdout}"
                        )
            print(f"PASS {self.name}", flush=True)
        finally:
            try:
                logs = self.compose("logs", "--no-color", check=False).stdout
                (self.directory / "containers.log").write_text(logs)
            finally:
                self.compose("down", "--volumes", "--remove-orphans", timeout=60)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suites", nargs="*", choices=SUITES)
    parser.add_argument("--image", default="kotoconn-e2e:local")
    parser.add_argument(
        "--compose",
        default="docker compose",
        help="Compose command, e.g. docker-compose",
    )
    parser.add_argument(
        "--engine",
        default="docker",
        help="Engine command used for inspecting exit status",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=os.cpu_count() or 1,
        help="Maximum concurrent scenarios, defaults to CPU count or 1 if unknown",
    )
    parser.add_argument("--output", type=Path, default=ROOT.parent / "target" / "e2e")
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    if len(set(args.suites)) != len(args.suites):
        parser.error("specify each suite only once")
    args.run_id = uuid.uuid4().hex[:12]
    args.output = args.output.resolve() / args.run_id
    print(f"Artifacts: {args.output}", flush=True)

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    scenarios = [
        Scenario(args, suite, direction)
        for suite in (args.suites or SUITES)
        for direction in (("client", "server") if suite in SUITES[:3] else ("both",))
    ]
    failures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = {pool.submit(scenario.run): scenario.name for scenario in scenarios}
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as error:
                LOGGER.exception("Scenario %s failed", futures[future])
                failures.append(futures[future])
                print(f"FAIL {futures[future]}: {error}", flush=True)
    if failures:
        raise SystemExit(f"Failed: {', '.join(failures)}; artifacts: {args.output}")


if __name__ == "__main__":
    main()
