"""Fetch libnode and its embedding shim, built by the libnode workflow.

Node.js publishes no shared library, so `.github/workflows/libnode.yml` builds one
for the glibc baseline in `rust.py` and publishes it as a release of this
repository. The shim exposes the C++ embedder API as a C ABI and must be built
with the same C++ standard library as libnode. After changing the version or the
shim source, run the workflow and pin the digests it reports.
"""

import argparse
import lzma
import os
import shutil
from pathlib import Path
from typing import BinaryIO

import fetch

ROOT = Path(__file__).resolve().parent.parent
VERSION = "26.13.1"
# The shared library name carries NODE_MODULE_VERSION.
SONAME = "libnode.so.147"
SHIM = "libkotoconn_node.so"
SHIM_SOURCE = ROOT / "crates/node/src/embed.cc"
RELEASE = f"libnode-v{VERSION}"
ARCHITECTURES = ("x86_64", "aarch64")

# Release asset digests by architecture.
LIBNODE = {
    "x86_64": "",
    "aarch64": "",
}
# The shim source these builds came from, and their digests by architecture.
SHIM_BUILT_FROM = ""
SHIM_BUILDS = {
    "x86_64": "",
    "aarch64": "",
}


def shim_id() -> str:
    return fetch.digest(SHIM_SOURCE)[:16]


def asset_names(architecture: str) -> tuple[str, str]:
    """Release assets for libnode and the current shim source."""
    return (
        f"libnode-v{VERSION}-linux-{architecture}.so.xz",
        f"kotoconn-node-{shim_id()}-linux-{architecture}.so",
    )


def library_dir(target: str) -> Path:
    architecture = target.split("-", 1)[0]
    if not target.endswith("-linux-gnu") or architecture not in ARCHITECTURES:
        raise ValueError(f"no libnode build for {target}")
    if shim_id() != SHIM_BUILT_FROM:
        raise ValueError(
            f"{SHIM_SOURCE.relative_to(ROOT)} changed since its builds were pinned; "
            "run the libnode workflow and pin the new digests in tools/libnode.py"
        )

    libnode_asset, shim_asset = asset_names(architecture)
    base = f"https://github.com/merrkry/kotoconn/releases/download/{RELEASE}"
    directory = ROOT / "target" / "libnode" / f"v{VERSION}-{shim_id()}" / target

    def decompress(source: BinaryIO, output: BinaryIO) -> None:
        with lzma.open(source) as unpacked:
            shutil.copyfileobj(unpacked, output)

    fetch.pinned(
        f"{base}/{libnode_asset}", LIBNODE[architecture], directory / SONAME, decompress
    )
    fetch.pinned(f"{base}/{shim_asset}", SHIM_BUILDS[architecture], directory / SHIM)
    return directory


def environment(target: str, env: dict[str, str]) -> dict[str, str]:
    directory = library_dir(target)
    env = env | {"KOTOCONN_LIBNODE_DIR": str(directory)}
    env["LD_LIBRARY_PATH"] = os.pathsep.join(
        filter(None, [str(directory), env.get("LD_LIBRARY_PATH")])
    )
    return env


def main() -> None:
    parser = argparse.ArgumentParser(description="Report what the libnode workflow builds.")
    parser.add_argument("query", choices=["version", "soname", "shim-id", "assets"])
    parser.add_argument("architecture", nargs="?", choices=ARCHITECTURES)
    args = parser.parse_args()

    if args.query == "version":
        print(VERSION)
    elif args.query == "soname":
        print(SONAME)
    elif args.query == "shim-id":
        print(shim_id())
    else:
        if args.architecture is None:
            parser.error("assets needs an architecture")
        print("\n".join(asset_names(args.architecture)))


if __name__ == "__main__":
    main()
