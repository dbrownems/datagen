"""Build the datagen wheel and push it to the Fabric lakehouse via `fab cp`.

The wheel is uploaded to `<WORKSPACE>.Workspace/<LAKEHOUSE>.Lakehouse/Files/datagen/`
where the Datagen Main notebook's install cell can pick it up.

Reads `.env` for environment settings:
    WORKSPACE   Fabric workspace name (required)
    LAKEHOUSE   Lakehouse name (required)

Usage:
    python deploy_wheel.py            # build + upload (deletes old wheels in target)
    python deploy_wheel.py --no-build # upload existing dist/*.whl
"""
from __future__ import annotations

import argparse
import glob
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent
DIST = REPO / "dist"


def load_env(path: Path) -> dict:
    env = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def build_wheel() -> Path:
    for old in glob.glob(str(DIST / "*.whl")):
        os.remove(old)
    print("$ python -m build --wheel", flush=True)
    subprocess.check_call([sys.executable, "-m", "build", "--wheel"], cwd=str(REPO))
    whls = sorted(DIST.glob("datagen_fabric-*.whl"))
    if not whls:
        raise RuntimeError("Build produced no wheel")
    return whls[-1]


def find_wheel() -> Path | None:
    whls = sorted(DIST.glob("datagen_fabric-*.whl"))
    return whls[-1] if whls else None


def fab(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    fab_exe = shutil.which("fab") or "fab"
    cmd = [fab_exe, *args]
    print(f"$ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, check=check, text=True)


def cleanup_old_wheels(target_dir: str, keep: str) -> None:
    """Remove other datagen_fabric-*.whl files from the target Files/datagen folder."""
    fab_exe = shutil.which("fab") or "fab"
    proc = subprocess.run(
        [fab_exe, "ls", target_dir],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return
    for line in proc.stdout.splitlines():
        name = line.strip()
        if (name.startswith("datagen_fabric-") and name.endswith(".whl")
                and name != keep):
            print(f"  Removing old wheel: {name}")
            subprocess.run([fab_exe, "rm", "-f", f"{target_dir}/{name}"],
                           check=False)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-build", action="store_true",
                    help="Skip build; upload existing dist/*.whl")
    ap.add_argument("--workspace", help="Override WORKSPACE from .env")
    ap.add_argument("--lakehouse", help="Override LAKEHOUSE from .env")
    ap.add_argument("--env", default=str(REPO / ".env"))
    args = ap.parse_args()

    env = load_env(Path(args.env))
    workspace = args.workspace or env.get("WORKSPACE") or os.environ.get("WORKSPACE")
    lakehouse = args.lakehouse or env.get("LAKEHOUSE") or os.environ.get("LAKEHOUSE")
    if not workspace:
        sys.exit("ERROR: WORKSPACE must be set (in .env or via --workspace)")
    if not lakehouse:
        sys.exit("ERROR: LAKEHOUSE must be set (in .env or via --lakehouse)")

    if args.no_build:
        whl = find_wheel()
        if whl is None:
            sys.exit("ERROR: --no-build but no wheel in dist/")
    else:
        whl = build_wheel()

    print(f"Wheel: {whl.name} ({whl.stat().st_size:,} bytes)")

    target_dir = f"/{workspace}.Workspace/{lakehouse}.Lakehouse/Files/datagen"
    print(f"Target: {target_dir}/")

    # Ensure target dir exists (mkdir errors if it already does — ignore).
    fab("mkdir", target_dir, check=False)

    cleanup_old_wheels(target_dir, whl.name)
    fab("cp", str(whl), f"{target_dir}/{whl.name}", "-f")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
