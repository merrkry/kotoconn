"""Plain TCP/UDP traffic only; sing-box handles every proxy protocol."""

import argparse
import concurrent.futures
import socket
import socketserver
import threading

LIMIT = 15
PAYLOAD = bytes(range(256)) * 1024


def receive(stream, length):
    data = bytearray()
    while len(data) < length:
        part = stream.recv(length - len(data))
        if not part:
            raise AssertionError(f"unexpected EOF after {len(data)}/{length} bytes")
        data.extend(part)
    return bytes(data)


def identity(port, source):
    return f"target:{port}\nsource:{source}\n".encode()


class TCP(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(LIMIT)
        self.request.sendall(
            identity(self.server.server_address[1], self.client_address[0])
        )
        while data := self.request.recv(65536):
            self.request.sendall(data)
        self.request.shutdown(socket.SHUT_WR)


class UDP(socketserver.BaseRequestHandler):
    def handle(self):
        data, sock = self.request
        sock.sendto(
            identity(self.server.server_address[1], self.client_address[0]) + data,
            self.client_address,
        )


def serve():
    servers = []
    for port in (9000, 9001):
        for kind, handler in (
            (socketserver.ThreadingTCPServer, TCP),
            (socketserver.ThreadingUDPServer, UDP),
        ):
            server = kind(("0.0.0.0", port), handler)
            server.daemon_threads = True
            servers.append(server)
            threading.Thread(target=server.serve_forever, daemon=True).start()
    print("Echo ready", flush=True)
    threading.Event().wait()


def tcp(port, target, source):
    with socket.create_connection(("entry", port), timeout=LIMIT) as stream:
        greeting = identity(target, source)
        assert receive(stream, len(greeting)) == greeting, (
            "wrong target or server-first data"
        )
        # Concurrent reading avoids turning socket buffer sizes into a test limit.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:

            def send():
                stream.sendall(PAYLOAD)

            sending = pool.submit(send)
            assert receive(stream, len(PAYLOAD)) == PAYLOAD, "TCP payload differs"
            sending.result()
        # sing-box 1.14.0 also truncates an in-flight response in a sing-box-only
        # SOCKS chain on early half-close. Keep half-close coverage in Rust;
        # this interop test checks complete transfer followed by connection EOF.
        stream.shutdown(socket.SHUT_WR)
        assert stream.recv(1) == b"", "missing EOF after completed transfer"


def udp(port, target, source):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(LIMIT)
        sock.connect(("entry", port))
        # sing-box's direct/SOCKS chain drops empty datagrams even without
        # Kotoconn. The Rust protocol tests cover zero-length payloads.
        for payload in (b"x", bytes(range(256)) * 32):
            sock.send(payload)
            actual = sock.recv(65536)
            assert actual == identity(target, source) + payload, (
                "UDP payload or target differs"
            )


def check(transport, rewrite, egress):
    source = socket.gethostbyname(egress)
    operation = tcp if transport == "tcp" else udp
    # Separate connections/associations exercise concurrency and multiple targets.
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [
            pool.submit(
                operation, 10080 + i % 2, 9001 if rewrite else 9000 + i % 2, source
            )
            for i in range(4)
        ]
        for future in futures:
            future.result()
    print(
        f"PASS {transport}: concurrent targets, payloads"
        + (", server-first, EOF" if transport == "tcp" else ", UDP datagrams")
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("serve", "ready", "tcp", "udp", "reject"))
    parser.add_argument("--egress", default="kotoconn")
    parser.add_argument("--rewrite", action="store_true")
    args = parser.parse_args()
    if args.mode == "serve":
        serve()
    elif args.mode == "ready":
        # Readiness is an observable response, not elapsed startup time.
        for port in (9000, 9001):
            with socket.create_connection(("127.0.0.1", port), timeout=2) as stream:
                expected = identity(port, "127.0.0.1")
                assert receive(stream, len(expected)) == expected
    elif args.mode == "reject":
        for port in (10082, 10083):
            with socket.create_connection(("entry", port), timeout=LIMIT) as stream:
                try:
                    assert stream.recv(1) == b"", "rejected request returned data"
                except ConnectionResetError:
                    pass
        print("PASS policy rejection and handler exception")
    else:
        check(args.mode, args.rewrite, args.egress)
