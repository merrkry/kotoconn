"""Download pinned release assets into the build cache."""

import hashlib
import shutil
import ssl
import tempfile
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def pinned(
    url: str,
    expected: str,
    destination: Path,
    unpack: Callable[[BinaryIO, BinaryIO], None] = shutil.copyfileobj,
) -> Path:
    """Fetch `url` once its SHA-256 matches `expected`, then store it unpacked."""
    stamp = destination.with_name(destination.name + ".sha256")
    if destination.is_file() and stamp.is_file() and stamp.read_text() == expected:
        # Repair caches created with NamedTemporaryFile's owner-only mode.
        destination.chmod(0o644)
        return destination

    print(f"Fetching {url}", flush=True)
    context = ssl.create_default_context()
    # Managed Python uses /etc/ssl/cert.pem; Debian stores its bundle here.
    bundle = Path("/etc/ssl/certs/ca-certificates.crt")
    if ssl.get_default_verify_paths().cafile is None and bundle.is_file():
        context.load_verify_locations(bundle)

    destination.parent.mkdir(parents=True, exist_ok=True)
    # Unique staging files and atomic replacement permit concurrent invocations.
    with tempfile.TemporaryDirectory(dir=destination.parent) as staging:
        download = Path(staging) / "download"
        with (
            urllib.request.urlopen(url, timeout=60, context=context) as response,
            download.open("wb") as output,
        ):
            shutil.copyfileobj(response, output)
        if digest(download) != expected:
            raise ValueError(f"SHA-256 mismatch for {url}")

        unpacked = Path(staging) / "unpacked"
        with download.open("rb") as source, unpacked.open("wb") as output:
            unpack(source, output)
        # Containers may load this file as a different UID without DAC_OVERRIDE.
        unpacked.chmod(0o644)
        unpacked.replace(destination)
        stamp_staging = Path(staging) / "stamp"
        stamp_staging.write_text(expected)
        stamp_staging.replace(stamp)
    return destination
