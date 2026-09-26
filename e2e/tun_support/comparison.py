"""Performance reference only. E2E never starts a reference proxy."""

import json
import time

from .environment import NAME, Daemon, Process, configure_routes, ip


class SingBox(Process):
    def __init__(self, binary, directory, mtu, cpus):
        directory.mkdir(parents=True)
        path = directory / "sing-box.json"
        path.write_text(
            json.dumps(
                {
                    "log": {"level": "warn"},
                    "inbounds": [
                        {
                            "type": "tun",
                            "interface_name": NAME,
                            "mtu": mtu,
                            "auto_route": False,
                            "dns_mode": "disabled",
                            "stack": "go",
                            "multi_queue": True,
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
            self.ready()
            configure_routes()
        except BaseException:
            self.close()
            raise

    def ready(self):
        # INFO logs include every connection and distort churn measurements.
        # Observe the real device instead; socket workloads verify end-to-end I/O.
        deadline = time.monotonic() + 15
        while self.process.poll() is None:
            links = json.loads(ip("-j", "link", "show"))
            if any(link["ifname"] == NAME and "UP" in link["flags"] for link in links):
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"reference TUN did not come up; see {self.log.name}"
                )
            time.sleep(0.01)
        raise RuntimeError(f"reference exited during startup; see {self.log.name}")

    def finish(self):
        # Verified measurement is complete. Reference lifecycle conformance is
        # outside this benchmark; process exit releases its nonpersistent TUN.
        self.process.kill()
        self.process.wait(timeout=5)
        Daemon.assert_removed()
