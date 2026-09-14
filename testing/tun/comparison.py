"""Performance reference only. E2E never starts a reference proxy."""

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
        # Verified measurement is complete. Reference lifecycle conformance is
        # outside this benchmark; process exit releases its nonpersistent TUN.
        self.process.kill()
        self.process.wait(timeout=5)
        Daemon.assert_removed()
