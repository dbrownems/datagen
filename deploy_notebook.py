"""Deploy the Datagen Main notebook to a Fabric workspace.

Reads environment-specific settings from a `.env` file (gitignored). If the
target workspace contains exactly one lakehouse, that lakehouse is attached
as the notebook's default lakehouse before upload — so the source-controlled
notebook never carries environment-specific lakehouse IDs.

The wheel is NOT embedded in the notebook — use `deploy_wheel.py` to push
it to the lakehouse `Files/datagen/` folder, where the notebook's install
cell will pick it up.

`.env` keys:
    WORKSPACE   Fabric workspace name or ID (required)

Usage:
    python deploy_notebook.py                 # uses .env in repo root
    python deploy_notebook.py --workspace X   # override workspace
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent
NOTEBOOK_DIR = REPO / "notebook" / "Datagen Main.Notebook"
NOTEBOOK_NAME = "Datagen Main"
FABRIC_API = "https://api.fabric.microsoft.com/v1"


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


def fabric_token() -> str:
    """Read token from .token (preferred) or fall back to az."""
    token_file = REPO / ".token"
    if token_file.exists():
        tok = token_file.read_text(encoding="utf-8").strip()
        if tok:
            return tok
    out = subprocess.check_output(
        ["az", "account", "get-access-token",
         "--resource", "https://api.fabric.microsoft.com",
         "--query", "accessToken", "-o", "tsv"],
        shell=True,
    )
    return out.decode().strip()


def _fab_api(method: str, path: str, body: dict | None = None) -> tuple[int, dict, dict]:
    """Invoke `fab api` and return (status, json_body, headers)."""
    import shutil
    import tempfile
    fab_exe = shutil.which("fab") or "fab"
    cmd = [fab_exe, "api", "-X", method.lower(), path, "--show_headers",
           "--output_format", "json"]
    tmp = None
    if body is not None:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json",
                                          delete=False, encoding="utf-8")
        json.dump(body, tmp)
        tmp.close()
        cmd.extend(["-i", tmp.name])
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
    if proc.returncode != 0:
        raise RuntimeError(f"fab api failed: {proc.stderr or proc.stdout}")

    out = proc.stdout
    start = out.rfind("\n{")
    candidate = out[start + 1:] if start >= 0 else out
    try:
        envelope = json.loads(candidate)
    except json.JSONDecodeError:
        raise RuntimeError(f"fab api: unparseable output: {out[-500:]}")

    data = envelope.get("result", {}).get("data", [{}])[0]
    status = data.get("status_code", 0)
    text = data.get("text", {})
    if isinstance(text, str):
        try:
            body_json = json.loads(text)
        except json.JSONDecodeError:
            body_json = {"_raw": text}
    elif isinstance(text, dict):
        body_json = text
    else:
        body_json = {}
    headers = data.get("headers", {}) or {}
    return status, body_json, headers


def get_workspace_id(token: str, workspace: str) -> str:
    """Resolve a workspace name or GUID to its ID."""
    if "-" in workspace and len(workspace) == 36:
        return workspace
    status, body, _ = _fab_api("get", "workspaces")
    if status != 200:
        raise RuntimeError(f"List workspaces failed: HTTP {status}: {body}")
    for ws in body.get("value", []):
        if ws["displayName"].lower() == workspace.lower():
            return ws["id"]
    raise RuntimeError(f"Workspace not found: {workspace}")


def list_lakehouses(token: str, workspace_id: str) -> list[dict]:
    status, body, _ = _fab_api("get", f"workspaces/{workspace_id}/lakehouses")
    if status != 200:
        raise RuntimeError(f"List lakehouses failed: HTTP {status}: {body}")
    return body.get("value", [])


def find_notebook_id(token: str, workspace_id: str, name: str) -> str | None:
    status, body, _ = _fab_api("get", f"workspaces/{workspace_id}/notebooks")
    if status != 200:
        raise RuntimeError(f"List notebooks failed: HTTP {status}: {body}")
    for nb in body.get("value", []):
        if nb["displayName"] == name:
            return nb["id"]
    return None


def build_definition(workspace_id: str, lakehouse: dict | None) -> dict:
    """Build the updateDefinition payload, optionally injecting a default lakehouse."""
    nb_path = NOTEBOOK_DIR / "notebook-content.ipynb"
    platform_path = NOTEBOOK_DIR / ".platform"

    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    if lakehouse is not None:
        nb.setdefault("metadata", {})["dependencies"] = {
            "lakehouse": {
                "default_lakehouse": lakehouse["id"],
                "default_lakehouse_name": lakehouse["displayName"],
                "default_lakehouse_workspace_id": workspace_id,
            }
        }
    nb_bytes = json.dumps(nb, indent=2).encode("utf-8")

    parts = [{
        "path": "notebook-content.ipynb",
        "payload": base64.b64encode(nb_bytes).decode("ascii"),
        "payloadType": "InlineBase64",
    }]
    if platform_path.exists():
        parts.append({
            "path": ".platform",
            "payload": base64.b64encode(platform_path.read_bytes()).decode("ascii"),
            "payloadType": "InlineBase64",
        })
    return {"definition": {"format": "ipynb", "parts": parts}}


def deploy(token: str, workspace_id: str, notebook_id: str | None,
           lakehouse: dict | None) -> None:
    body = build_definition(workspace_id, lakehouse)

    if notebook_id is None:
        create_body = {"displayName": NOTEBOOK_NAME, "definition": body["definition"]}
        status, resp, headers = _fab_api(
            "post", f"workspaces/{workspace_id}/notebooks", create_body)
    else:
        path = (f"workspaces/{workspace_id}/items/{notebook_id}"
                f"/updateDefinition?updateMetadata=true")
        status, resp, headers = _fab_api("post", path, body)

    if status not in (200, 201, 202):
        raise RuntimeError(f"HTTP {status}: {resp}")

    if status == 202:
        loc = headers.get("Location") or headers.get("location")
        if loc:
            for _ in range(60):
                time.sleep(2)
                pstatus, presp, _ = _fab_api("get", loc, None)
                if pstatus == 200:
                    s = presp.get("status", "")
                    if s in ("Succeeded", "Completed"):
                        return
                    if s == "Failed":
                        raise RuntimeError(f"Deploy failed: {presp}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workspace", help="Workspace name or ID (overrides .env)")
    ap.add_argument("--env", default=str(REPO / ".env"),
                    help="Path to .env file (default: ./.env)")
    args = ap.parse_args()

    env = load_env(Path(args.env))
    workspace = args.workspace or env.get("WORKSPACE") or os.environ.get("WORKSPACE")
    if not workspace:
        sys.exit("ERROR: WORKSPACE not set. Provide --workspace or set it in .env")

    print(f"Target workspace: {workspace}")
    token = fabric_token()
    workspace_id = get_workspace_id(token, workspace)
    print(f"  Workspace ID: {workspace_id}")

    lakehouses = list_lakehouses(token, workspace_id)
    if len(lakehouses) == 1:
        lh = lakehouses[0]
        print(f"  Attaching default lakehouse: {lh['displayName']} ({lh['id']})")
    elif len(lakehouses) == 0:
        lh = None
        print("  No lakehouses in workspace — notebook will deploy unattached.")
    else:
        lh = None
        names = ", ".join(l["displayName"] for l in lakehouses)
        print(f"  Multiple lakehouses found ({names}) — leaving notebook unattached.")
        print("  Attach manually in Fabric or temporarily remove others.")

    notebook_id = find_notebook_id(token, workspace_id, NOTEBOOK_NAME)
    action = "Updating" if notebook_id else "Creating"
    print(f"  {action} notebook '{NOTEBOOK_NAME}' ...")
    deploy(token, workspace_id, notebook_id, lh)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
