"""Real socket lifecycle checks, separate from measured traffic."""

import errno
import socket
import threading

from .environment import CLIENT, REMOTE

PORT = 9001
BANNER = b"server-first\n"
TRAILER = b"after-half-close\n"


class TcpEcho:
    def __init__(self):
        self.stop = threading.Event()
        self.sockets = []
        self.threads = []
        self.errors = []
        self.forced = False
        for family, address in zip((socket.AF_INET, socket.AF_INET6), REMOTE):
            tcp = socket.socket(family, socket.SOCK_STREAM)
            tcp.bind((address, PORT))
            tcp.listen()
            tcp.settimeout(0.2)
            self.sockets.append(tcp)
            thread = threading.Thread(target=self.accept, args=(tcp,), daemon=True)
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
            thread = threading.Thread(target=self.stream, args=(conn,), daemon=True)
            thread.start()
            self.threads.append(thread)

    def stream(self, conn):
        forced = self.forced
        with conn:
            try:
                conn.settimeout(10)
                conn.sendall(BANNER)
                while data := conn.recv(65536):
                    conn.sendall(data)
                conn.sendall(TRAILER)
                conn.shutdown(socket.SHUT_WR)
            except (ConnectionError, TimeoutError) as error:
                if not forced:
                    self.errors.append(str(error))
            except OSError as error:
                if not forced or error.errno not in (
                    errno.EPIPE,
                    errno.ECONNRESET,
                    errno.ENOTCONN,
                ):
                    self.errors.append(str(error))

    def close(self):
        self.stop.set()
        for sock in self.sockets:
            sock.close()
        for thread in self.threads:
            thread.join(timeout=2)
        assert all(not thread.is_alive() for thread in self.threads), (
            "echo worker did not stop"
        )
        assert not self.errors, self.errors


def client(family, kind):
    index = int(family == socket.AF_INET6)
    sock = socket.socket(family, kind)
    sock.settimeout(10)
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
