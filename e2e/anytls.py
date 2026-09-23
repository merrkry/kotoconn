"""Run the adapter's bidirectional interoperability tests against official Go code."""

import shlex
import subprocess

TESTS = {
    "client": "tests::interop::rust_client_interoperates_with_official_go_server",
    "server": "tests::interop::official_go_client_interoperates_with_rust_server",
}


class Scenario:
    def __init__(self, args, direction):
        self.name = f"anytls-{direction}"
        self.test = TESTS[direction]
        self.engine = shlex.split(args.engine)
        self.image = args.anytls_image
        self.container = f"koto-{args.run_id}-{self.name}"
        self.directory = args.output / self.name
        self.directory.mkdir(parents=True)

    def run(self):
        command = self.engine + [
            "run",
            "--rm",
            "--init",
            "--name",
            self.container,
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev",
            self.image,
            self.test,
            "--exact",
            "--nocapture",
        ]
        log_path = self.directory / "runner.log"
        try:
            with log_path.open("w") as log:
                log.write(f"$ {shlex.join(command)}\n")
                log.flush()
                subprocess.run(
                    command,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=120,
                )

            # libtest exits successfully even if a stale filter matches no tests.
            if "test result: ok. 1 passed; 0 failed;" not in log_path.read_text():
                raise RuntimeError(f"expected one passing test; see {log_path}")
            print(f"PASS {self.name}", flush=True)
        finally:
            subprocess.run(
                self.engine + ["rm", "--force", self.container],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=30,
            )
