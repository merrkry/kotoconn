"""Small independent wire corpus. Normal application data uses kernel sockets."""

import socket
import struct
from collections import Counter

from .environment import CLIENT, NAME, REMOTE, isolated


def checksum(data):
    if len(data) % 2:
        data += b"\0"
    value = sum(struct.unpack(f"!{len(data) // 2}H", data))
    while value >> 16:
        value = (value & 0xFFFF) + (value >> 16)
    return (~value) & 0xFFFF


def corpus(ipv6, sequence, port):
    family = socket.AF_INET6 if ipv6 else socket.AF_INET
    source = socket.inet_pton(family, CLIENT[int(ipv6)])
    target = socket.inet_pton(family, REMOTE[int(ipv6)])

    def pseudo(protocol, length):
        if ipv6:
            return source + target + struct.pack("!I3xB", length, protocol)
        return source + target + struct.pack("!BBH", 0, protocol, length)

    def ip(protocol, payload):
        if ipv6:
            return (
                struct.pack(
                    "!IHBB16s16s", 6 << 28, len(payload), protocol, 64, source, target
                )
                + payload
            )
        header = struct.pack(
            "!BBHHHBBH4s4s",
            0x45,
            0,
            20 + len(payload),
            sequence % 65536,
            0,
            64,
            protocol,
            0,
            source,
            target,
        )
        return header[:10] + struct.pack("!H", checksum(header)) + header[12:] + payload

    def udp(payload):
        segment = struct.pack("!HHHH", 22222, port, len(payload) + 8, 0) + payload
        value = checksum(pseudo(17, len(segment)) + segment) or 0xFFFF
        return segment[:6] + struct.pack("!H", value) + segment[8:]

    # This independent valid twin must reach the socket server during mixed runs.
    yield "positive-control", ip(17, udp(b"CONTROL" + sequence.to_bytes(8, "big")))
    segment = udp(b"INVALID-checksum" + sequence.to_bytes(8, "big"))
    value = int.from_bytes(segment[6:8], "big")
    wrong = 2 if value == 1 else 1
    yield "udp-checksum", ip(17, segment[:6] + struct.pack("!H", wrong) + segment[8:])
    yield "udp-length", ip(17, segment[:4] + b"\0\x07" + segment[6:])
    if ipv6:
        yield "ipv6-zero-udp-checksum", ip(17, segment[:6] + b"\0\0" + segment[8:])
        payload = udp(b"INVALID-overlap" + bytes(1200))
        for offset, data, more in (
            (0, payload[:512], 1),
            (256, payload[256:768], 1),
            (768, payload[768:], 0),
        ):
            fragment = struct.pack("!BBHI", 17, 0, offset | more, sequence)
            yield "ipv6-overlap", ip(44, fragment + data)
    else:
        valid = ip(17, segment)
        wrong = int.from_bytes(valid[10:12], "big") ^ 0xFFFF
        yield "ipv4-checksum", valid[:10] + struct.pack("!H", wrong) + valid[12:]
    for flags, data_offset in ((3, 5), (2, 4)):
        segment = struct.pack(
            "!HHIIBBHHH", 22224, port, sequence, 0, data_offset << 4, flags, 65535, 0, 0
        )
        segment = (
            segment[:16]
            + struct.pack("!H", checksum(pseudo(6, len(segment)) + segment))
            + segment[18:]
        )
        yield "tcp-flags" if flags == 3 else "tcp-offset", ip(6, segment)
    for length in (1, 8, 19, 39):
        yield "truncated-header", ip(17, udp(b"INVALID-truncated"))[:length]


class Injector:
    def __init__(self, ipv6, port=9001, seed=1):
        isolated()
        self.ipv6 = ipv6
        self.port = port
        self.sequence = seed & 0xFFFFFFFF
        self.counts = Counter()
        self.socket = socket.socket(socket.AF_PACKET, socket.SOCK_DGRAM)
        self.socket.settimeout(5)

    def send(self):
        for name, packet in corpus(self.ipv6, self.sequence, self.port):
            # AF_PACKET avoids the ordinary IP send path repairing our bad packets.
            count = self.socket.sendto(packet, (NAME, 0x86DD if self.ipv6 else 0x0800))
            if count != len(packet):
                raise RuntimeError("partial raw packet injection")
            self.counts[name] += 1
        self.sequence = (self.sequence + 1) & 0xFFFFFFFF

    def close(self):
        self.socket.close()
