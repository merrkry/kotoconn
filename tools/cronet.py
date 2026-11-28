"""Fetch the checksummed SagerNet Cronet library. Never build Chromium."""

import os
from pathlib import Path

import fetch

ROOT = Path(__file__).resolve().parent.parent
VERSION = "v150.0.7871.63-2"
# GitHub release asset digests, pinned alongside the ABI consumer in Cargo.lock.
ASSETS = {
    "x86_64": (
        "amd64",
        "c3949c6ad64e1d8fcd1e3b1fae4e302b2e553d769665a4bd7576483564c3f026",
    ),
    "aarch64": (
        "arm64",
        "8f13a6186aca498d37ee5e1f410282f587d663995aca60d6bf29a2d4f5536f2b",
    ),
    "i686": ("386", "46becdd1b89a2d73a003907f581278f4b29ae16f35161013dcbf2188bc22aaf4"),
    "armv7": (
        "arm",
        "566c9ec0a18a83e3f08d99b77e3437d6cf21e58a4ec3fed60b11975b3ee8ea97",
    ),
    "riscv64gc": (
        "riscv64",
        "18bb9a9cdbb9035cc3ed55ef549c3ab8a4e5468c15b56a9d81073cd54b2399ce",
    ),
    "loongarch64": (
        "loong64",
        "636aea6299de091966c08f118894664e416c3f7de9cb2b6bbc09cdce14d44d21",
    ),
    "mipsel": (
        "mipsle",
        "dd28b25d17ec4ba9c5390ea44aed4dcbeed4422c4ac74377dc7153bbad79de48",
    ),
    "mips64el": (
        "mips64le",
        "396dadd8d567795ae691c907da94d349bcb665123d13ad1b533a50e633b17a0d",
    ),
}


def library(target: str) -> Path | None:
    if "-linux-" not in target:
        return None
    architecture = target.split("-", 1)[0]
    if "-gnu" not in target or architecture not in ASSETS:
        raise ValueError(f"no pinned SagerNet Cronet shared library for {target}")

    asset, expected = ASSETS[architecture]
    url = (
        "https://github.com/SagerNet/cronet-go/releases/download/"
        f"{VERSION}/libcronet-linux-{asset}.so"
    )
    destination = ROOT / "target" / "cronet" / VERSION / target / "libcronet.so"
    return fetch.pinned(url, expected, destination)


def environment(target: str) -> dict[str, str]:
    env = os.environ.copy()
    native = library(target)
    if native is not None:
        env["CRONET_LIB_DIR"] = str(native.parent)
        env["CRONET_LIB_NAME"] = "cronet"
        env.pop("CRONET_STATIC", None)
        env["LD_LIBRARY_PATH"] = os.pathsep.join(
            filter(None, [str(native.parent), env.get("LD_LIBRARY_PATH")])
        )
    return env
