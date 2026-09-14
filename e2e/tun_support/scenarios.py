"""Named workload recipes; execution and correctness decisions live in the runners."""

from .environment import CLIENT, REMOTE


def cases(mtu, ipv6, *, stress=False):
    common = {"source": CLIENT[int(ipv6)], "target": REMOTE[int(ipv6)], "mtu": mtu}
    rounds = 32 if stress else 4
    specs = []
    for protocol in ("tcp", "udp"):
        for connections in (1, 16 if stress else 4):
            for workload in ("bulk", "churn", "sparse"):
                specs.append(
                    (
                        f"{protocol}-{workload}-{connections}",
                        {
                            "protocol": protocol,
                            "workload": workload,
                            "connections": connections,
                            "rounds": rounds * (8 if workload == "churn" else 1),
                            "bytes": 1048577,
                        },
                    )
                )
    specs += [
        (
            "udp-fragmented-4",
            {
                "protocol": "udp",
                "datagram_bytes": 8192,
                "connections": 4,
                "rounds": rounds,
            },
        ),
        ("tcp-upload", {"direction": "upload", "bytes": 4194305, "rounds": rounds}),
        ("tcp-download", {"direction": "download", "bytes": 4194305, "rounds": rounds}),
        (
            "udp-boundaries",
            {"protocol": "udp", "workload": "boundaries", "rounds": rounds},
        ),
        (
            "udp-paced",
            {"protocol": "udp", "rate": 500, "rounds": 128 if stress else 32},
        ),
        (
            "mixed-malformed",
            {
                "workload": "mixed",
                "connections": 16 if stress else 8,
                "duration_ms": 3000 if stress else 1000,
                "bytes": 262145,
                "rate": 500,
                "allow_loss": True,
            },
        ),
    ]
    return [(name, {**common, **spec}) for name, spec in specs]
