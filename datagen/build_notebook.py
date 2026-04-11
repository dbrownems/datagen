"""Build the 'Datagen Main.Notebook' artifact for deployment via `fab import`.

Usage:
    python -m datagen build-notebook
    python -m datagen build-notebook --output-dir ./my-dist
"""

import json
import os
import shutil


def build_notebook(output_dir=None):
    """Build the Fabric notebook artifact.

    Args:
        output_dir: Directory to write the notebook artifact.
                    Default: dist/ relative to current working directory.
    """
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), "dist")

    nb_dir = os.path.join(output_dir, "Datagen Main.Notebook")
    if os.path.exists(nb_dir):
        shutil.rmtree(nb_dir)
    os.makedirs(nb_dir)

    cells = [
        # ── Markdown: title ──
        {
            "cell_type": "markdown",
            "source": [
                "# Datagen\n",
                "\n",
                "Generate realistic Delta tables and a Direct Lake semantic model from a Power BI `.vpax` file.\n",
                "\n",
                "## Setup\n",
                "\n",
                "1. **Attach a Lakehouse** — click the Lakehouse icon in the left sidebar and select (or create) a Lakehouse\n",
                "2. **Upload your `.vpax` file** — place it in `Files/datagen/` in the attached Lakehouse\n",
                "   - Export a `.vpax` from [DAX Studio](https://daxstudio.org/) → Advanced → Export Metrics\n",
                "3. **Run Cell 1** below to install datagen (auto-downloads from GitHub if needed)\n",
                "4. **Edit Cell 2** — change the `.vpax` filename to match yours\n",
                "5. **Run Cell 2** — generates Delta tables, deploys a semantic model, and prints a comparison report\n",
            ],
            "metadata": {},
        },
        # ── Code: setup ──
        {
            "cell_type": "code",
            "source": [
                "# Install datagen — downloads the latest release from GitHub if not cached locally\n",
                "import glob, subprocess, sys, os, urllib.request, json\n",
                "\n",
                "if not os.path.isdir('/lakehouse/default'):\n",
                "    raise RuntimeError(\n",
                "        'No lakehouse attached. Click the Lakehouse icon in the left sidebar, '\n",
                "        'then select or create a Lakehouse before running this notebook.'\n",
                "    )\n",
                "\n",
                'DATAGEN_DIR = "/lakehouse/default/Files/datagen"\n',
                "os.makedirs(DATAGEN_DIR, exist_ok=True)\n",
                "\n",
                "USE_LATEST = True  # Set False to use a cached .whl instead of downloading\n",
                "\n",
                "def _get_latest_whl():\n",
                '    api_url = "https://api.github.com/repos/dbrownems/datagen/releases/latest"\n',
                "    with urllib.request.urlopen(api_url) as resp:\n",
                "        release = json.loads(resp.read())\n",
                '    asset = next(a for a in release["assets"] if a["name"].endswith(".whl"))\n',
                '    local = f"{DATAGEN_DIR}/{asset[\'name\']}"\n',
                "    if not os.path.exists(local):\n",
                '        print(f"Downloading {asset[\'name\']} ...")\n',
                '        urllib.request.urlretrieve(asset["browser_download_url"], local)\n',
                "    return local\n",
                "\n",
                "if USE_LATEST:\n",
                "    whl = _get_latest_whl()\n",
                "else:\n",
                '    whls = sorted(glob.glob(f"{DATAGEN_DIR}/datagen_fabric-*.whl"))\n',
                "    whl = whls[-1] if whls else _get_latest_whl()\n",
                "\n",
                'print(f"Installing {os.path.basename(whl)}")\n',
                "subprocess.check_call([\n",
                '    sys.executable, "-m", "pip", "install", whl, "semantic-link-labs",\n',
                '    "-q", "--no-warn-conflicts", "--disable-pip-version-check", "--force-reinstall", "--no-deps",\n',
                "])\n",
                'subprocess.check_call([sys.executable, "-m", "pip", "install", "semantic-link-labs", "-q", "--no-warn-conflicts", "--disable-pip-version-check"])\n',
                'import datagen; print(f"Ready — datagen v{datagen.__version__}")\n',
                "\n",
                "# Verify the installed version matches the loaded version\n",
                "import importlib.metadata\n",
                "installed = importlib.metadata.version('datagen-fabric')\n",
                "loaded = datagen.__version__\n",
                "if installed != loaded:\n",
                "    raise RuntimeError(\n",
                "        f'Version mismatch: installed v{installed} but loaded v{loaded}. '\n",
                "        f'Please restart the Spark session (Session → Stop session) and re-run.'\n",
                "    )\n",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        # ── Markdown: generate instructions ──
        {
            "cell_type": "markdown",
            "source": [
                "## Generate\n",
                "\n",
                "Edit the `.vpax` filename below, then run the cell. This will:\n",
                "\n",
                "1. **Parse** the `.vpax` and infer data distributions from column statistics\n",
                "2. **Generate** Delta tables matching the original row counts and cardinality\n",
                "3. **Deploy** a Direct Lake semantic model with all measures, relationships, and column metadata\n",
                "4. **Compare** the generated tables against the expected statistics and print a report\n",
                "\n",
                "### Options\n",
                "\n",
                "| Parameter | Default | Description |\n",
                "|---|---|---|\n",
                "| `mode` | `\"direct_lake\"` | `\"import\"` for Power Query import mode |\n",
                "| `deploy_model` | `True` | `False` to skip semantic model deployment |\n",
                "| `compare` | `True` | `False` to skip comparison report |\n",
                "| `overwrite_tables` | `True` | `False` to skip existing tables, only generate missing |\n",
                "| `overwrite_model` | `True` | `False` to fail if semantic model already exists |\n",
                "| `seed` | `42` | Random seed for reproducible generation |\n",
            ],
            "metadata": {},
        },
        # ── Code: generate ──
        {
            "cell_type": "code",
            "source": [
                "from datagen import generate\n",
                "\n",
                "VPAX_FILE = \"\"  # ← set your .vpax filename, or leave blank to auto-detect\n",
                "\n",
                "# Auto-detect: use the first .vpax file in the datagen folder\n",
                "if not VPAX_FILE:\n",
                "    vpax_files = sorted(glob.glob(f\"{DATAGEN_DIR}/*.vpax\"))\n",
                "    if not vpax_files:\n",
                "        raise FileNotFoundError(f\"No .vpax files found in {DATAGEN_DIR}. Upload one and re-run.\")\n",
                "    vpax_path = vpax_files[0]\n",
                "    print(f\"Auto-detected: {os.path.basename(vpax_path)}\")\n",
                "else:\n",
                "    vpax_path = f\"{DATAGEN_DIR}/{VPAX_FILE}\"\n",
                "\n",
                "report = generate(\n",
                "    spark,\n",
                "    vpax_path,\n",
                "    compare=False,\n",
                "    overwrite_tables=False,\n",
                "    overwrite_model=True,\n",
                ")",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
    ]

    notebook = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "kernelspec": {"display_name": "synapse_pyspark", "name": "synapse_pyspark"},
            "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
        },
        "cells": cells,
    }

    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": "Datagen Main"},
        "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
    }

    with open(os.path.join(nb_dir, "notebook-content.ipynb"), "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2)
    with open(os.path.join(nb_dir, ".platform"), "w", encoding="utf-8") as f:
        json.dump(platform, f, indent=2)

    print(f"Notebook built: {nb_dir}")
    return nb_dir


def build_load_notebook(output_dir=None):
    """Build the 'Datagen Load Test.Notebook' artifact."""
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), "dist")

    nb_dir = os.path.join(output_dir, "Datagen Load Test.Notebook")
    if os.path.exists(nb_dir):
        shutil.rmtree(nb_dir)
    os.makedirs(nb_dir)

    cells = [
        {
            "cell_type": "markdown",
            "source": [
                "# Datagen Load Test\n",
                "\n",
                "Runs DAX queries against a semantic model simulating concurrent Power BI users.\n",
                "\n",
                "## Prerequisites\n",
                "\n",
                "1. Upload `queries.json` and `users.json` to `Files/datagen/` in the attached lakehouse\n",
                "2. `queries.json`: array of DAX query strings\n",
                "3. `users.json`: array of `{\"email\": \"...\", \"role\": \"...\"}` objects\n",
                "4. The semantic model must have RLS rules using `CUSTOMDATA()`\n",
            ],
            "metadata": {},
        },
        # ── Cell 1: Setup (disabled — deploy via OneLake explorer) ──
        {
            "cell_type": "code",
            "source": [
                "# Install datagen — disabled, deploy .whl via OneLake explorer instead\n",
                "DEPLOY_FROM_GITHUB = False\n",
                "\n",
                "if DEPLOY_FROM_GITHUB:\n",
                "    import subprocess, sys, os, glob, urllib.request, json\n",
                "\n",
                '    DATAGEN_DIR = "/lakehouse/default/Files/datagen"\n',
                "\n",
                "    def _get_latest_whl():\n",
                '        api_url = "https://api.github.com/repos/dbrownems/datagen/releases/latest"\n',
                "        with urllib.request.urlopen(api_url) as resp:\n",
                "            release = json.loads(resp.read())\n",
                '        asset = next(a for a in release["assets"] if a["name"].endswith(".whl"))\n',
                '        local = f"{DATAGEN_DIR}/{asset[\'name\']}"\n',
                "        if not os.path.exists(local):\n",
                '            urllib.request.urlretrieve(asset["browser_download_url"], local)\n',
                "        return local\n",
                "\n",
                "    whl = _get_latest_whl()\n",
                '    print(f"Installing {os.path.basename(whl)}")\n',
                "    subprocess.check_call([\n",
                '        sys.executable, "-m", "pip", "install", whl,\n',
                '        "-q", "--disable-pip-version-check", "--force-reinstall", "--no-deps",\n',
                "    ])\n",
                '    import datagen; print(f"datagen v{datagen.__version__}")\n',
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        # ── Cell 2: Download QueryRunner DLL (disabled — deploy via OneLake explorer) ──
        {
            "cell_type": "code",
            "source": [
                "# Download QueryRunner DLL — disabled, deploy via OneLake explorer instead\n",
                "DEPLOY_FROM_GITHUB = False\n",
                "\n",
                "if DEPLOY_FROM_GITHUB:\n",
                "    import urllib.request, json, os\n",
                "\n",
                '    dll_path = f"{DATAGEN_DIR}/DatagenQueryRunner.dll"\n',
                "    if not os.path.exists(dll_path):\n",
                '        api_url = "https://api.github.com/repos/dbrownems/datagen/releases/latest"\n',
                "        with urllib.request.urlopen(api_url) as resp:\n",
                "            release = json.loads(resp.read())\n",
                '        asset = next(a for a in release["assets"] if a["name"] == "DatagenQueryRunner.dll")\n',
                '        urllib.request.urlretrieve(asset["browser_download_url"], dll_path)\n',
                '        print(f"Downloaded {asset[\'name\']}")\n',
                "    else:\n",
                '        print(f"QueryRunner already downloaded")\n',
                '    print(f"DLL: {dll_path}")\n',
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        # ── Cell 3: Configure and Run ──
        {
            "cell_type": "code",
            "source": [
                "# ── Configuration ──\n",
                'DATAGEN_DIR = "/lakehouse/default/Files/datagen"\n',
                "\n",
                "# Install datagen from lakehouse\n",
                "import subprocess, sys, os, glob, json\n",
                "whls = sorted(glob.glob(f\"{DATAGEN_DIR}/datagen_fabric-*.whl\"))\n",
                "if not whls:\n",
                "    raise FileNotFoundError(f\"No .whl found in {DATAGEN_DIR}. Deploy via OneLake explorer.\")\n",
                "whl = whls[-1]\n",
                "subprocess.check_call([\n",
                "    sys.executable, \"-m\", \"pip\", \"install\", whl,\n",
                "    \"-q\", \"--disable-pip-version-check\", \"--force-reinstall\", \"--no-deps\",\n",
                "])\n",
                "import datagen; print(f\"datagen v{datagen.__version__}\")\n",
                "\n",
                'XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld"\n',
                'DATASET = "Sales Leader Dataset"\n',
                "DURATION_SECONDS = 60      # total test duration\n",
                "CONCURRENT_USERS = 3      # number of simulated concurrent users\n",
                "QUERIES_PER_BATCH = 4      # concurrent queries per user\n",
                "PAUSE_BETWEEN_ITERATIONS_MS = 1000  # pause between iterations (ms)\n",
                "PAUSE_BETWEEN_QUERIES_MS = 0        # pause after each query (ms), 0=no pause\n",
                'TARGET_REPLICA = "readonly"  # "readonly" for read-only replica (scale-out), "" for default\n',
                "\n",
                "# Load queries and users\n",
                "import json\n",
                'with open(f"{DATAGEN_DIR}/queries.json", "r") as f:\n',
                "    queries = json.load(f)\n",
                'with open(f"{DATAGEN_DIR}/users.json", "r") as f:\n',
                "    all_users = json.load(f)\n",
                "\n",
                "# Expand or trim users to match CONCURRENT_USERS\n",
                "users = [all_users[i % len(all_users)] for i in range(CONCURRENT_USERS)]\n",
                "\n",
                'print(f"Queries: {len(queries)}")\n',
                'print(f"Users: {len(users)} (from {len(all_users)} in users.json)")\n',
                'print(f"Duration: {DURATION_SECONDS}s")\n',
                'print(f"Concurrency: {QUERIES_PER_BATCH} queries/user")\n',
                "\n",
                "# Get access token\n",
                "import notebookutils\n",
                'token = notebookutils.credentials.getToken("pbi")\n',
                'print(f"Token acquired ({len(token)} chars)")\n',
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        # ── Cell 4: Run Load Test ──
        {
            "cell_type": "code",
            "source": [
                "# Run the load test\n",
                "from pythonnet import load\n",
                "load('coreclr')\n",
                "import clr, os, json, time\n",
                "\n",
                "# Bootstrap CLR and ADOMD.NET via sempy (knows how to find the assemblies)\n",
                "import sempy.fabric as fabric\n",
                "fabric.create_tom_server()\n",
                "\n",
                'dll_path = os.path.join(DATAGEN_DIR, "DatagenQueryRunner.dll")\n',
                "clr.AddReference(dll_path)\n",
                "from Datagen import QueryRunner\n",
                "from System import Array, String\n",
                "\n",
                "# Version check — ensure DLL matches the datagen package\n",
                "import System.Reflection\n",
                "asm = System.Reflection.Assembly.GetAssembly(QueryRunner)\n",
                "dll_version = str(asm.GetName().Version)\n",
                "pkg_version = datagen.__version__\n",
                "# Compare major.minor.patch (DLL has 4-part version)\n",
                "dll_short = '.'.join(dll_version.split('.')[:3])\n",
                "if dll_short != pkg_version:\n",
                "    print(f'⚠ Version mismatch: DatagenQueryRunner.dll v{dll_short} vs datagen v{pkg_version}')\n",
                "    print(f'  Delete Files/datagen/DatagenQueryRunner.dll and re-run Cell 2 to update.')\n",
                "else:\n",
                "    print(f'QueryRunner v{dll_short} ✓')\n",
                "\n",
                "# Build XMLA endpoint (append ?readonly for scale-out testing)\n",
                "xmla = f'{XMLA_ENDPOINT}?{TARGET_REPLICA}' if TARGET_REPLICA else XMLA_ENDPOINT\n",
                "\n",
                "# Prepare .NET arrays\n",
                "q_arr = Array[String]([q if isinstance(q, str) else q['query'] for q in queries])\n",
                "email_arr = Array[String]([u['email'] for u in users])\n",
                "role_arr = Array[String]([u['role'] for u in users])\n",
                "\n",
                'print(f"Starting load test: {len(users)} users × {len(queries)} queries, {DURATION_SECONDS}s...")\n',
                "if TARGET_REPLICA:\n",
                '    print(f"Target: {TARGET_REPLICA} replica")\n',
                "print(flush=True)\n",
                "\n",
                "# Log directory for telemetry CSV\n",
                "LOG_DIR = f\"{DATAGEN_DIR}/logs\"\n",
                "os.makedirs(LOG_DIR, exist_ok=True)\n",
                "\n",
                "t0 = time.time()\n",
                "result_json = QueryRunner.RunLoadTest(\n",
                "    q_arr, xmla, DATASET, token,\n",
                "    email_arr, role_arr,\n",
                "    DURATION_SECONDS,\n",
                "    QUERIES_PER_BATCH,\n",
                "    PAUSE_BETWEEN_ITERATIONS_MS,\n",
                "    PAUSE_BETWEEN_QUERIES_MS,\n",
                "    LOG_DIR,\n",
                ")\n",
                "elapsed = time.time() - t0\n",
                "\n",
                "stats = json.loads(result_json)\n",
                'print(f"\\n=== Load Test Results ({elapsed:.0f}s) ===")\n',
                'print(f"Total executions: {stats[\'totalExecutions\']}")\n',
                'print(f"Successful:       {stats[\'successfulExecutions\']}")\n',
                'print(f"Failed:           {stats[\'failedExecutions\']}")\n',
                'print(f"QPS:              {stats[\'qps\']}")\n',
                'print(f"Max iteration:    {stats[\'maxIteration\']}")\n',
                "\n",
                "if 'latency' in stats:\n",
                "    lat = stats['latency']\n",
                '    print(f"\\nLatency:")\n',
                '    print(f"  Min:    {lat[\'min\']}ms")\n',
                '    print(f"  Median: {lat[\'median\']}ms")\n',
                '    print(f"  Mean:   {lat[\'mean\']}ms")\n',
                '    print(f"  P95:    {lat[\'p95\']}ms")\n',
                '    print(f"  P99:    {lat[\'p99\']}ms")\n',
                '    print(f"  Max:    {lat[\'max\']}ms")\n',
                "\n",
                'print(f"\\nPer-user:")\n',
                "for u in stats.get('perUser', []):\n",
                "    user = users[u['userIndex']]\n",
                '    print(f"  {user[\'email\'][:30]:30s} iters={u[\'iterations\']} execs={u[\'executions\']} errs={u[\'errors\']} avg={u[\'meanLatencyMs\']}ms")\n',
                "\n",
                "if 'sampleErrors' in stats:\n",
                '    print(f"\\nSample errors:")\n',
                "    for e in stats['sampleErrors']:\n",
                '        print(f"  User {e[\'UserIndex\']}, Q{e[\'QueryIndex\']}: {str(e[\'Error\'])[:100]}")\n',
                "\n",
                "# Save full results\n",
                'with open(f"{DATAGEN_DIR}/load_test_results.json", "w") as f:\n',
                "    json.dump(stats, f, indent=2)\n",
                'print(f"\\nFull results saved to Files/datagen/load_test_results.json")\n',
                "if 'logFile' in stats:\n",
                '    print(f"Telemetry log: {stats[\'logFile\']}")\n',
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
    ]

    notebook = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "kernelspec": {"display_name": "synapse_pyspark", "name": "synapse_pyspark"},
            "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
        },
        "cells": cells,
    }

    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": "Datagen Load Test"},
        "config": {"version": "2.0", "logicalId": "11111111-1111-1111-1111-111111111111"},
    }

    with open(os.path.join(nb_dir, "notebook-content.ipynb"), "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2)
    with open(os.path.join(nb_dir, ".platform"), "w", encoding="utf-8") as f:
        json.dump(platform, f, indent=2)

    print(f"Notebook built: {nb_dir}")
    return nb_dir


def build_scaleout_notebook(output_dir=None):
    """Build the 'Datagen Scale-Out Test.Notebook' artifact."""
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), "dist")

    nb_dir = os.path.join(output_dir, "Datagen Scale-Out Test.Notebook")
    if os.path.exists(nb_dir):
        shutil.rmtree(nb_dir)
    os.makedirs(nb_dir)

    cells = [
        {
            "cell_type": "markdown",
            "source": [
                "# Datagen Scale-Out Test\n",
                "\n",
                "Configure a semantic model for scale-out with manual sync, then trigger refresh and sync.\n",
                "\n",
                "**Scenario:** Run alongside the Load Test notebook to measure the impact of refresh + sync\n",
                "on an active query workload hitting the read-only replica.\n",
                "\n",
                "## Workflow\n",
                "\n",
                "1. **Cell 1** \u2014 Configure: set workspace and dataset, acquire token\n",
                "2. **Cell 2** \u2014 Enable scale-out with manual sync (one-time setup)\n",
                "3. **Cell 3** \u2014 Check sync status\n",
                "4. Start the **Load Test** notebook (with `TARGET_REPLICA = \"readonly\"`)\n",
                "5. **Cell 4** \u2014 Trigger refresh on the read/write node and wait\n",
                "6. **Cell 5** \u2014 Trigger manual sync to propagate to read-only replicas\n",
                "7. **Cell 6** \u2014 Disable scale-out (cleanup when done)\n",
            ],
            "metadata": {},
        },
        # -- Cell 1: Configuration --
        {
            "cell_type": "code",
            "source": [
                "# -- Configuration --\n",
                "import requests, json, time\n",
                "import notebookutils\n",
                "\n",
                "WORKSPACE_ID = \"5de52008-bfaf-4e48-882e-d18ec06496fd\"\n",
                "DATASET_NAME = \"Sales Leader Dataset\"\n",
                "\n",
                "# Acquire Power BI token\n",
                "token = notebookutils.credentials.getToken(\"pbi\")\n",
                "headers = {\"Authorization\": f\"Bearer {token}\", \"Content-Type\": \"application/json\"}\n",
                "BASE = \"https://api.powerbi.com/v1.0/myorg\"\n",
                "\n",
                "# Find dataset ID\n",
                "resp = requests.get(f\"{BASE}/groups/{WORKSPACE_ID}/datasets\", headers=headers)\n",
                "resp.raise_for_status()\n",
                "dataset = next(d for d in resp.json()['value'] if d['name'] == DATASET_NAME)\n",
                "DATASET_ID = dataset['id']\n",
                "\n",
                "# Check current scale-out config\n",
                "resp = requests.get(f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}\", headers=headers)\n",
                "resp.raise_for_status()\n",
                "qso = resp.json().get('queryScaleOutSettings', {})\n",
                "\n",
                "print(f\"Dataset: {DATASET_NAME}\")\n",
                "print(f\"Dataset ID: {DATASET_ID}\")\n",
                "print(f\"Scale-out: maxReadOnlyReplicas={qso.get('maxReadOnlyReplicas')}, \"\n",
                "      f\"autoSync={qso.get('autoSyncReadOnlyReplicas')}\")\n",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        # -- Cell 2: Enable Scale-Out --
        {
            "cell_type": "code",
            "source": [
                "# Enable scale-out with manual sync\n",
                "body = {\n",
                "    \"queryScaleOutSettings\": {\n",
                "        \"maxReadOnlyReplicas\": -1,\n",
                "        \"autoSyncReadOnlyReplicas\": False\n",
                "    }\n",
                "}\n",
                "resp = requests.patch(\n",
                "    f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}\",\n",
                "    headers=headers, json=body\n",
                ")\n",
                "resp.raise_for_status()\n",
                "\n",
                "# Verify\n",
                "resp = requests.get(f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}\", headers=headers)\n",
                "resp.raise_for_status()\n",
                "qso = resp.json().get('queryScaleOutSettings', {})\n",
                "replicas = qso.get('maxReadOnlyReplicas', 0)\n",
                "auto_sync = qso.get('autoSyncReadOnlyReplicas', True)\n",
                "\n",
                "print(f\"maxReadOnlyReplicas: {replicas}\")\n",
                "print(f\"autoSyncReadOnlyReplicas: {auto_sync}\")\n",
                "if replicas != 0 and not auto_sync:\n",
                "    print(\"\\u2713 Scale-out enabled with manual sync\")\n",
                "else:\n",
                "    print(\"\\u26a0 Configuration may not be correct\")\n",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        # -- Cell 3: Check Sync Status --
        {
            "cell_type": "code",
            "source": [
                "# Check sync status\n",
                "resp = requests.get(\n",
                "    f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/queryScaleOut/syncStatus\",\n",
                "    headers=headers\n",
                ")\n",
                "resp.raise_for_status()\n",
                "status = resp.json()\n",
                "print(json.dumps(status, indent=2))\n",
                "\n",
                "if status.get('commitVersion') == status.get('minActiveReadVersion'):\n",
                "    print(\"\\n\\u2713 Read-write and read-only replicas are in sync\")\n",
                "else:\n",
                "    print(f\"\\n\\u26a0 Out of sync: commit v{status.get('commitVersion')} \"\n",
                "          f\"vs read v{status.get('minActiveReadVersion')}\")\n",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        # -- Cell 4: Trigger Refresh --
        {
            "cell_type": "code",
            "source": [
                "# Trigger a full refresh on the read/write node\n",
                "# With manual sync, this does NOT affect read-only replicas until sync\n",
                "resp = requests.post(\n",
                "    f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/refreshes\",\n",
                "    headers=headers, json={\"type\": \"full\"}\n",
                ")\n",
                "print(f\"Refresh triggered: HTTP {resp.status_code}\")\n",
                "resp.raise_for_status()\n",
                "\n",
                "# Poll for refresh completion\n",
                "print(\"Waiting for refresh to complete\", end=\"\", flush=True)\n",
                "while True:\n",
                "    time.sleep(10)\n",
                "    print(\".\", end=\"\", flush=True)\n",
                "    resp = requests.get(\n",
                "        f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/refreshes?$top=1\",\n",
                "        headers=headers\n",
                "    )\n",
                "    resp.raise_for_status()\n",
                "    refreshes = resp.json().get('value', [])\n",
                "    if refreshes and refreshes[0].get('status') != 'Unknown':\n",
                "        print()\n",
                "        rstat = refreshes[0]['status']\n",
                "        if rstat == 'Completed':\n",
                "            print(f\"\\u2713 Refresh completed\")\n",
                "        else:\n",
                "            print(f\"\\u26a0 Refresh status: {rstat}\")\n",
                "            if 'serviceExceptionJson' in refreshes[0]:\n",
                "                print(refreshes[0]['serviceExceptionJson'])\n",
                "        break\n",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        # -- Cell 5: Trigger Sync --
        {
            "cell_type": "code",
            "source": [
                "# Trigger manual sync to propagate refresh to read-only replicas\n",
                "resp = requests.post(\n",
                "    f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/queryScaleOut/sync\",\n",
                "    headers=headers, data=\"\"\n",
                ")\n",
                "print(f\"Sync triggered: HTTP {resp.status_code}\")\n",
                "resp.raise_for_status()\n",
                "\n",
                "# Poll for sync completion\n",
                "print(\"Waiting for sync to complete\", end=\"\", flush=True)\n",
                "while True:\n",
                "    time.sleep(5)\n",
                "    print(\".\", end=\"\", flush=True)\n",
                "    resp = requests.get(\n",
                "        f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/queryScaleOut/syncStatus\",\n",
                "        headers=headers\n",
                "    )\n",
                "    resp.raise_for_status()\n",
                "    status = resp.json()\n",
                "    if status.get('commitVersion') == status.get('minActiveReadVersion'):\n",
                "        print()\n",
                "        print(f\"\\u2713 Sync completed\")\n",
                "        print(f\"  syncStartTime: {status.get('syncStartTime')}\")\n",
                "        print(f\"  syncEndTime: {status.get('syncEndTime')}\")\n",
                "        break\n",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        # -- Cell 6: Disable Scale-Out --
        {
            "cell_type": "code",
            "source": [
                "# Disable scale-out (run when done with testing)\n",
                "body = {\"queryScaleOutSettings\": {\"maxReadOnlyReplicas\": 0}}\n",
                "resp = requests.patch(\n",
                "    f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}\",\n",
                "    headers=headers, json=body\n",
                ")\n",
                "resp.raise_for_status()\n",
                "\n",
                "# Verify\n",
                "resp = requests.get(f\"{BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}\", headers=headers)\n",
                "resp.raise_for_status()\n",
                "qso = resp.json().get('queryScaleOutSettings', {})\n",
                "if qso.get('maxReadOnlyReplicas', -1) == 0:\n",
                "    print(\"\\u2713 Scale-out disabled\")\n",
                "else:\n",
                "    print(f\"\\u26a0 Still enabled: {qso}\")\n",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
    ]

    notebook = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "kernelspec": {"display_name": "synapse_pyspark", "name": "synapse_pyspark"},
            "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
        },
        "cells": cells,
    }

    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": "Datagen Scale-Out Test"},
        "config": {"version": "2.0", "logicalId": "22222222-2222-2222-2222-222222222222"},
    }

    with open(os.path.join(nb_dir, "notebook-content.ipynb"), "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2)
    with open(os.path.join(nb_dir, ".platform"), "w", encoding="utf-8") as f:
        json.dump(platform, f, indent=2)

    print(f"Notebook built: {nb_dir}")
    return nb_dir
