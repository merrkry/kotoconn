"""Optional sing-box adapter; binaries are supplied by the caller."""

import json

from .environment import NAME, Daemon, Process, configure_routes


class SingBox(Process):
    def __init__(self, binary, directory, mtu, cpus):
        directory.mkdir(parents=True)
        path = directory / "sing-box.json"
        path.write_text(
            json.dumps(
                {
                    "log": {"level": "info"},
                    "inbounds": [
                        {
                            "type": "tun",
                            "interface_name": NAME,
                            "mtu": mtu,
                            "auto_route": False,
                            "stack": "go",
                            "address": ["192.0.2.1/30", "fd00::1/126"],
                        }
                    ],
                    "outbounds": [{"type": "direct", "tag": "direct"}],
                    "route": {"final": "direct"},
                },
                indent=2,
            )
        )
        argv = [str(binary), "run", "-c", str(path)]
        if cpus:
            argv = ["taskset", "--cpu-list", cpus, *argv]
        super().__init__(argv, directory / "daemon.log")
        try:
            self.event("sing-box started")
            configure_routes()
        except BaseException:
            self.close()
            raise

    def finish(self):
        self.process.terminate()
        if self.process.wait(timeout=15):
            raise RuntimeError("sing-box shutdown failed")
        Daemon.assert_removed()
