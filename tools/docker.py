"""Build runtime images, optionally exporting BuildKit layers for CI."""

import argparse
import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=["app", "test-runtime"])
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    cache_root = os.environ.get("KOTOCONN_DOCKER_CACHE")
    command = ["docker", "build"]
    cache = None
    export = None

    if cache_root:
        cache = Path(cache_root).resolve() / args.target
        export = cache.with_name(cache.name + "-next")
        if export.exists():
            shutil.rmtree(export)
        command = ["docker", "buildx", "build", "--load"]
        if (cache / "index.json").exists():
            command += ["--cache-from", f"type=local,src={cache}"]
        command += ["--cache-to", f"type=local,dest={export},mode=max"]

    command += ["-f", "e2e/Dockerfile", "--target", args.target, "-t", args.tag, "."]
    subprocess.run(command, cwd=ROOT, check=True)

    if cache is not None and export is not None:
        # Replace the previous export so unreferenced layers do not accumulate.
        if cache.exists():
            shutil.rmtree(cache)
        export.rename(cache)


if __name__ == "__main__":
    main()
