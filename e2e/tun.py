"""Exercise the real CLI and Linux TCP/IP stack in a disposable network namespace.

Build with cargo build -p kotoconn-cli, then run python3 e2e/tun.py.
Only the Python standard library, unshare, and iproute2 are required.
"""

import argparse
import concurrent.futures
import errno
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MARK = 42
NAME = "ktest0"
CLIENT = ("192.0.2.2", "fd00::2")
REMOTE = ("198.18.0.1", "2001:db8::1")
PORT = 9001
BANNER = b"server-first\n"
TRAILER = b"after-half-close\n"


def ip(*args):
    return subprocess.run(
        ["ip", *args], check=True, text=True, capture_output=True, timeout=10
    ).stdout


class Daemon:
    def __init__(self, binary, directory, deadline=5):
        self.lines = []
        self.condition = threading.Condition()
        self.log = (directory / f"daemon-{deadline}.log").open("w")
        self.process = subprocess.Popen(
            [
                str(binary),
                "run",
                "--config",
                str(directory / "main.ts"),
                "--shutdown-timeout",
                str(deadline),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()
        try:
            self.wait_line("Daemon ready:")
        except BaseException:
            self.close()
            raise

    def read(self):
        for line in self.process.stderr:
            self.log.write(line)
            self.log.flush()
            with self.condition:
                self.lines.append(line)
                self.condition.notify_all()
        with self.condition:
            self.condition.notify_all()

    def wait_line(self, prefix):
        deadline = time.monotonic() + 15
        with self.condition:
            while not any(line.startswith(prefix) for line in self.lines):
                if self.process.poll() is not None:
                    raise RuntimeError("daemon exited: " + "".join(self.lines))
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "daemon did not report " + prefix + ": " + "".join(self.lines)
                    )
                self.condition.wait(remaining)

    def stop(self):
        self.process.send_signal(signal.SIGTERM)
        self.wait_line("Daemon stopping.")

    def finish(self, forced=False):
        code = self.process.wait(timeout=15)
        self.reader.join(timeout=5)
        self.log.close()
        assert (code != 0) == forced, "".join(self.lines)
        assert not any(
            item["ifname"] == NAME for item in json.loads(ip("-j", "link", "show"))
        ), "TUN survived daemon exit"

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.reader.join(timeout=5)
        self.log.close()


class Echo:
    def __init__(self):
        self.stop = threading.Event()
        self.sockets = []
        self.threads = []
        self.bad = []
        self.errors = []
        self.forced = False
        self.accepted = 0
        for family, address in zip((socket.AF_INET, socket.AF_INET6), REMOTE):
            tcp = socket.socket(family, socket.SOCK_STREAM)
            tcp.bind((address, PORT))
            tcp.listen()
            tcp.settimeout(0.2)
            udp = socket.socket(family, socket.SOCK_DGRAM)
            udp.bind((address, PORT))
            udp.settimeout(0.2)
            self.sockets.extend((tcp, udp))
            for target, sock in ((self.accept, tcp), (self.datagrams, udp)):
                thread = threading.Thread(target=target, args=(sock,), daemon=True)
                thread.start()
                self.threads.append(thread)

    def accept(self, listener):
        while not self.stop.is_set():
            try:
                conn, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self.accepted += 1
            thread = threading.Thread(target=self.stream, args=(conn,), daemon=True)
            thread.start()
            self.threads.append(thread)

    def stream(self, conn):
        with conn:
            try:
                conn.settimeout(10)
                conn.sendall(BANNER)
                while data := conn.recv(65536):
                    conn.sendall(data)
                conn.sendall(TRAILER)
                conn.shutdown(socket.SHUT_WR)
            except (ConnectionError, TimeoutError):
                # Forced daemon shutdown deliberately interrupts a connection.
                pass
            except OSError as error:
                if not self.forced or error.errno not in (
                    errno.EPIPE,
                    errno.ECONNRESET,
                    errno.ENOTCONN,
                ):
                    self.errors.append(str(error))

    def datagrams(self, sock):
        while not self.stop.is_set():
            try:
                data, peer = sock.recvfrom(65536)
                if data.startswith(b"INVALID"):
                    self.bad.append(data)
                sock.sendto(data, peer)
            except TimeoutError:
                continue
            except OSError:
                return

    def close(self):
        self.stop.set()
        for sock in self.sockets:
            sock.close()
        for thread in self.threads:
            thread.join(timeout=2)
        assert all(not thread.is_alive() for thread in self.threads), (
            "echo worker did not stop"
        )
        assert not self.bad, f"malformed datagrams reached outbound: {self.bad}"
        assert not self.errors, self.errors


def client(family, kind):
    index = int(family == socket.AF_INET6)
    sock = socket.socket(family, kind)
    sock.settimeout(10)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_MARK, MARK)
    sock.bind((CLIENT[index], 0))
    sock.connect((REMOTE[index], PORT))
    return sock


def read_exact(sock, count):
    result = bytearray()
    while len(result) < count:
        data = sock.recv(count - len(result))
        if not data:
            raise EOFError(f"EOF at {len(result)} of {count} bytes")
        result.extend(data)
    return bytes(result)


def tcp_case(family):
    payload = bytes(range(251)) * 2048
    with client(family, socket.SOCK_STREAM) as sock:
        assert read_exact(sock, len(BANNER)) == BANNER
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:

            def write():
                sock.sendall(payload)
                sock.shutdown(socket.SHUT_WR)

            sent = pool.submit(write)
            received = bytearray()
            while data := sock.recv(65536):
                received.extend(data)
            sent.result(timeout=10)
        assert received == payload + TRAILER


def udp_case(family):
    with client(family, socket.SOCK_DGRAM) as sock:
        for size in (0, 1, 1200, 8192, 60000):
            payload = bytes(i % 251 for i in range(size))
            sock.send(payload)
            try:
                reply = sock.recv(65536)
            except TimeoutError as error:
                raise TimeoutError(
                    f"UDP reply timed out: family={family}, size={size}"
                ) from error
            assert reply == payload, (family, size)


def malformed():
    # AF_PACKET delivers frames to the TUN transmit path without repairing IP
    # checksums or rejecting malformed lengths in the kernel first.
    with socket.socket(socket.AF_PACKET, socket.SOCK_DGRAM) as sender:
        for data in (
            b"",
            b"\x45",
            b"\x60",
            bytes(64),
            b"\x4f" + bytes(19),
            b"\x60\x00\x00\x00\xff\xff" + bytes(34),
        ):
            if data:
                sender.sendto(data, (NAME, 0x0800 if data[0] >> 4 == 4 else 0x86DD))
        for family, source, destination in zip(
            (socket.AF_INET, socket.AF_INET6), CLIENT, REMOTE
        ):
            payload = b"INVALID-checksum"
            udp = struct.pack("!HHHH", 22222, PORT, 8 + len(payload), 1) + payload
            if family == socket.AF_INET:
                header = struct.pack(
                    "!BBHHHBBH4s4s",
                    0x45,
                    0,
                    20 + len(udp),
                    9,
                    0,
                    64,
                    17,
                    0,
                    socket.inet_pton(family, source),
                    socket.inet_pton(family, destination),
                )
                header = header[:10] + struct.pack("!H", checksum(header)) + header[12:]
                sender.sendto(header + udp, (NAME, 0x0800))
                sender.sendto(
                    header[:10] + b"\x00\x00" + header[12:] + udp, (NAME, 0x0800)
                )
            else:
                header = struct.pack(
                    "!IHBB16s16s",
                    6 << 28,
                    len(udp),
                    17,
                    64,
                    socket.inet_pton(family, source),
                    socket.inet_pton(family, destination),
                )
                sender.sendto(header + udp, (NAME, 0x86DD))
                sender.sendto(header + udp[:6] + b"\x00\x00" + udp[8:], (NAME, 0x86DD))
        # A valid UDP checksum does not make overlapping IPv6 fragments valid.
        source = socket.inet_pton(socket.AF_INET6, CLIENT[1])
        destination = socket.inet_pton(socket.AF_INET6, REMOTE[1])
        payload = b"INVALID-ipv6-overlap" + bytes(1200)
        udp = struct.pack("!HHHH", 22223, PORT, 8 + len(payload), 0) + payload
        pseudo = source + destination + struct.pack("!I3xB", len(udp), 17)
        udp = udp[:6] + struct.pack("!H", checksum(pseudo + udp) or 0xFFFF) + udp[8:]
        for offset, data, more in (
            (0, udp[:512], 1),
            (256, udp[256:768], 1),
            (768, udp[768:], 0),
        ):
            header = struct.pack(
                "!IHBB16s16s", 6 << 28, len(data) + 8, 44, 64, source, destination
            )
            fragment = struct.pack("!BBHI", 17, 0, offset | more, 12345678)
            sender.sendto(header + fragment + data, (NAME, 0x86DD))
        # SYN+FIN and truncated TCP headers must not create outbound connections.
        for flags, data_offset in ((3, 5), (2, 4)):
            tcp = struct.pack(
                "!HHIIBBHHH", 22224, PORT, 100, 0, data_offset << 4, flags, 65535, 0, 0
            )
            pseudo = source + destination + struct.pack("!I3xB", len(tcp), 6)
            tcp = tcp[:16] + struct.pack("!H", checksum(pseudo + tcp)) + tcp[18:]
            header = struct.pack(
                "!IHBB16s16s", 6 << 28, len(tcp), 6, 64, source, destination
            )
            sender.sendto(header + tcp, (NAME, 0x86DD))
        # Deterministic malformed input exercises truncation and unknown header
        # paths without depending on a random seed or expected packet loss.
        for size in range(1, 257):
            data = bytes((size * 17 + i * 31) % 256 for i in range(size))
            sender.sendto(data, (NAME, 0x0800))


def checksum(data):
    if len(data) % 2:
        data += b"\x00"
    value = sum(struct.unpack(f"!{len(data) // 2}H", data))
    while value >> 16:
        value = (value & 0xFFFF) + (value >> 16)
    return (~value) & 0xFFFF


def routes():
    for family in ("-4", "-6"):
        ip(family, "route", "add", "default", "dev", NAME, "table", "100")
    for field, value in (("rp_filter", "0"), ("accept_local", "1")):
        for iface in ("all", NAME):
            Path(f"/proc/sys/net/ipv4/conf/{iface}/{field}").write_text(value)


def run(binary, directory):
    assert os.readlink("/proc/self/ns/net") != os.environ["KOTOCONN_PARENT_NETNS"], (
        "refusing to modify the parent network namespace"
    )
    ip("link", "set", "lo", "up")
    for address in (*CLIENT, *REMOTE):
        family = "-6" if ":" in address else "-4"
        ip(
            family,
            "addr",
            "add",
            address + ("/128" if family == "-6" else "/32"),
            "dev",
            "lo",
            *(["nodad"] if family == "-6" else []),
        )
    for family in ("-4", "-6"):
        ip(family, "rule", "add", "priority", "1000", "lookup", "local")
        ip(family, "rule", "del", "priority", "0")
        ip(
            family,
            "rule",
            "add",
            "priority",
            "100",
            "fwmark",
            str(MARK),
            "lookup",
            "100",
        )
    (
        directory / "main.ts"
    ).write_text("""import { kotoconn as k } from '@kotoconn/bindings';
const resolver = k.resolve_handler(name => k.lookup(name));
const direct = k.dialer({dialer: null, outbound: {resolve_handler: resolver, implementation: k.direct_outbound({})}});
const routing = k.routing_handler(flow => flow.protocol === 'udp' ? k.route_udp(direct) : k.route(direct, flow.dest));
k.inbound({implementation: k.tun_inbound({name: 'ktest0', mtu: 1280, addresses: [
    {address: k.ip('192.0.2.1'), prefix: 30}, {address: k.ip('fd00::1'), prefix: 126}
]}), routing_handler: routing, udp_idle_timeout: k.timeout(30000)});
""")
    echo = Echo()
    daemon = None
    try:
        daemon = Daemon(binary, directory)
        routes()
        malformed()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            jobs = [
                pool.submit(tcp_case, family)
                for family in (socket.AF_INET, socket.AF_INET6)
                for _ in range(3)
            ]
            jobs += [
                pool.submit(udp_case, family)
                for family in (socket.AF_INET, socket.AF_INET6)
            ]
            for job in jobs:
                job.result(timeout=30)
        print(
            "PASS TUN dual-stack TCP, UDP, fragmentation, server-first and half-close",
            flush=True,
        )
        with client(socket.AF_INET, socket.SOCK_STREAM) as active:
            assert read_exact(active, len(BANNER)) == BANNER
            daemon.stop()
            active.sendall(b"during-drain")
            active.shutdown(socket.SHUT_WR)
            assert (
                read_exact(active, len(b"during-drain") + len(TRAILER))
                == b"during-drain" + TRAILER
            )
            assert active.recv(1) == b""
        daemon.finish()
        print(
            "PASS TUN graceful shutdown retains established connections and removes interface",
            flush=True,
        )
        daemon = Daemon(binary, directory, deadline=1)
        routes()
        with client(socket.AF_INET6, socket.SOCK_STREAM) as active:
            assert read_exact(active, len(BANNER)) == BANNER
            echo.forced = True
            daemon.stop()
            daemon.finish(forced=True)
        print("PASS TUN forced shutdown removes interface", flush=True)
        startup_failure(binary, directory)
    finally:
        if daemon:
            daemon.close()
        echo.close()
    assert echo.accepted == 8, f"unexpected TCP admission count: {echo.accepted}"
    print(
        "PASS malformed ingress did not reach outbound or crash the daemon", flush=True
    )


def startup_failure(binary, directory):
    original = (directory / "main.ts").read_text()
    duplicate = (
        original
        + """
k.inbound({implementation: k.tun_inbound({name: 'ktest0', mtu: 1280, addresses: []}),
    routing_handler: routing, udp_idle_timeout: k.timeout(30000)});
"""
    )
    path = directory / "duplicate.ts"
    path.write_text(duplicate)
    failed = subprocess.run(
        [str(binary), "run", "--config", str(path)],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    (directory / "startup-failure.log").write_text(failed.stderr)
    assert failed.returncode != 0 and "already exists" in failed.stderr, failed.stderr
    assert not any(
        item["ifname"] == NAME for item in json.loads(ip("-j", "link", "show"))
    )
    print("PASS failed startup releases already-bound TUN interfaces", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/kotoconn")
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    directory = args.output or Path(
        tempfile.mkdtemp(prefix="tun-", dir=ROOT / "target")
    )
    directory.mkdir(parents=True, exist_ok=True)
    directory = directory.resolve()
    if args.inside:
        run(binary, directory)
    else:
        print(f"Artifacts: {directory}", flush=True)
        env = dict(os.environ, KOTOCONN_PARENT_NETNS=os.readlink("/proc/self/ns/net"))
        command = ["unshare", "--net"]
        if os.getuid() != 0:
            command += ["--user", "--map-root-user"]
        command += [
            sys.executable,
            str(Path(__file__).resolve()),
            "--inside",
            "--binary",
            str(binary),
            "--output",
            str(directory),
        ]
        subprocess.run(command, env=env, check=True, timeout=120)


if __name__ == "__main__":
    main()
