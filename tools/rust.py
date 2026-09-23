"""Run Cargo through Zig with an explicit target, or stage Linux binaries."""

import argparse
import json
import shutil
import subprocess
from pathlib import Path

import cronet

ROOT = Path(__file__).resolve().parent.parent


def host_target():
    version = subprocess.check_output(["rustc", "-vV"], text=True)
    return next(
        line.removeprefix("host: ")
        for line in version.splitlines()
        if line.startswith("host: ")
    )


def zig_target(target):
    # Pin the Linux glibc baseline for both C dependencies and the final linker.
    return target + ".2.28" if target.endswith("-linux-gnu") else target


def run(args, env=None):
    subprocess.run(args, cwd=ROOT, env=env, check=True)


def linux_target(target):
    architecture = target.split("-", 1)[0]
    if architecture not in {"x86_64", "aarch64"}:
        raise ValueError(f"unsupported container architecture: {architecture}")
    return f"{architecture}-unknown-linux-gnu"


def stage_anytls_interop(target):
    target = linux_target(target)
    run(["rustup", "target", "add", target])
    result = subprocess.run(
        [
            "cargo-zigbuild",
            "test",
            "--locked",
            "--target-dir",
            str(ROOT / "target"),
            "--target",
            zig_target(target),
            "-p",
            "kotoconn-anytls",
            "--features",
            "interop-tests",
            "--lib",
            "--no-run",
            "--message-format=json-render-diagnostics",
        ],
        cwd=ROOT,
        env=cronet.environment(target),
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    executables = [
        Path(message["executable"])
        for line in result.stdout.splitlines()
        if (message := json.loads(line)).get("reason") == "compiler-artifact"
        and message.get("executable")
        and message["target"]["name"] == "kotoconn_anytls"
        and message["profile"]["test"]
    ]
    if len(executables) != 1:
        raise RuntimeError(f"expected one AnyTLS test executable, found {executables}")

    destination = ROOT / "target/anytls-interop/anytls-tests"
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(executables[0], destination)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command")
    args, extra = parser.parse_known_args()
    target = host_target()

    if args.command == "stage":
        stage = argparse.ArgumentParser()
        stage.add_argument("profile", choices=["debug", "release"])
        options = stage.parse_args(extra)
        target = linux_target(target)
        run(["rustup", "target", "add", target])
        command = [
            "cargo-zigbuild",
            "zigbuild",
            "--locked",
            "--target-dir",
            str(ROOT / "target"),
            "--target",
            zig_target(target),
            "-p",
            "kotoconn-cli",
            "-p",
            "kotoconn-tun-traffic",
        ]
        if options.profile == "release":
            command.append("--release")
        run(command, env=cronet.environment(target))

        destination = ROOT / "target/tun" / options.profile
        destination.mkdir(parents=True, exist_ok=True)
        for binary in ("kotoconn", "kotoconn-tun-traffic"):
            shutil.copy2(
                ROOT / "target" / target / options.profile / binary,
                destination / binary,
            )
        native = cronet.library(target)
        if native is not None:
            shutil.copy2(native, destination / native.name)
    elif args.command == "stage-anytls-interop":
        if extra:
            parser.error("stage-anytls-interop takes no arguments")
        stage_anytls_interop(target)
    else:
        run(
            ["cargo-zigbuild", args.command, "--target", zig_target(target), *extra],
            env=cronet.environment(target),
        )


if __name__ == "__main__":
    main()
