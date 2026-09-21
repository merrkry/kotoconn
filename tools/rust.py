"""Run Cargo through Zig with an explicit target, or stage Linux binaries."""

import argparse
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command")
    args, extra = parser.parse_known_args()
    target = host_target()

    if args.command == "stage":
        stage = argparse.ArgumentParser()
        stage.add_argument("profile", choices=["debug", "release"])
        options = stage.parse_args(extra)
        architecture = target.split("-", 1)[0]
        if architecture not in {"x86_64", "aarch64"}:
            parser.error(f"unsupported container architecture: {architecture}")
        target = f"{architecture}-unknown-linux-gnu"
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
    else:
        run(
            ["cargo-zigbuild", args.command, "--target", zig_target(target), *extra],
            env=cronet.environment(target),
        )


if __name__ == "__main__":
    main()
