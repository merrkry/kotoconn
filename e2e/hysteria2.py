"""Official Hysteria interop fixtures; all endpoints stay on the Compose network."""

import json
from pathlib import Path

IMAGE = "docker.io/tobyxdd/hysteria:v2.12.3@sha256:8e3e46bab28aa9f62a488b81f7e509f70a834914fb2e70a42277c57c165eaa94"
FIXTURES = Path(__file__).resolve().parent / "hysteria2"
PASSWORD = "hysteria-interop-password"
OBFS = "salamander-interop-password"
SUITES = ("hysteria2", "hysteria2-salamander")


def policy_options(kind, outbound=False, obfuscated=False):
    if kind != "hysteria2":
        return ""
    values = {"password": PASSWORD}
    if outbound:
        values.update(
            server_name="hysteria.test",
            ca_certificate=(FIXTURES / "cert.pem").read_text(),
        )
    else:
        values.update(
            certificate=(FIXTURES / "cert.pem").read_text(),
            private_key=(FIXTURES / "key.pem").read_text(),
        )
    if obfuscated:
        values["obfs_password"] = OBFS
    return ", " + ", ".join(
        f"{key}: {json.dumps(value)}" for key, value in values.items()
    )


def override(directory, direction):
    service = "peer" if direction == "client" else "entry"
    mode = "server" if direction == "client" else "client"
    path = directory / "hysteria-compose.json"
    path.write_text(
        json.dumps(
            {
                "services": {
                    service: {
                        "image": IMAGE,
                        "command": [
                            mode,
                            "--disable-update-check",
                            "-c",
                            f"/config/hysteria-{mode}.json",
                        ],
                    }
                }
            }
        )
    )
    return path


def configure(directory, target, obfuscated):
    for name in ("cert.pem", "key.pem"):
        (directory / name).write_text((FIXTURES / name).read_text())
    server: dict[str, object] = {
        "listen": ":1080",
        "tls": {"cert": "/config/cert.pem", "key": "/config/key.pem"},
        "auth": {"type": "password", "password": PASSWORD},
        "ignoreClientBandwidth": True,
        "congestion": {"type": "bbr"},
    }
    client: dict[str, object] = {
        "server": "kotoconn:1080",
        "auth": PASSWORD,
        "tls": {"sni": "hysteria.test", "ca": "/config/cert.pem"},
        "congestion": {"type": "bbr"},
    }
    for transport in ("tcp", "udp"):
        client[f"{transport}Forwarding"] = [
            {"listen": f":{10080 + index}", "remote": f"{target}:{9000 + index}"}
            for index in range(2)
        ]
    if obfuscated:
        for config in (server, client):
            config["obfs"] = {"type": "salamander", "salamander": {"password": OBFS}}
    for mode, config in (("server", server), ("client", client)):
        (directory / f"hysteria-{mode}.json").write_text(json.dumps(config, indent=2))


def ready(logs, direction):
    if direction == "client":
        return "server up and running" in logs
    # Each TCP and UDP forwarding listener emits its own observable ready event.
    return logs.count("forwarding listening") >= 4
