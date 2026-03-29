"""Build the Fabric notebook artifact for deployment via `fab import`."""
import json
import os
import shutil

nb_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "dist", "GenerateAdventureWorks.Notebook"))
if os.path.exists(nb_dir):
    shutil.rmtree(nb_dir)
os.makedirs(nb_dir)

cell1 = [
    "import glob, subprocess, sys, os, urllib.request, json\n",
    "\n",
    'DATAGEN_DIR = "/lakehouse/default/Files/datagen"\n',
    "os.makedirs(DATAGEN_DIR, exist_ok=True)\n",
    "\n",
    "# Find or download the latest wheel\n",
    'whls = sorted(glob.glob(f"{DATAGEN_DIR}/datagen_fabric-*.whl"))\n',
    "if whls:\n",
    "    whl = whls[-1]\n",
    "else:\n",
    '    api_url = "https://api.github.com/repos/dbrownems/datagen/releases/latest"\n',
    "    with urllib.request.urlopen(api_url) as resp:\n",
    "        release = json.loads(resp.read())\n",
    '    asset = next(a for a in release["assets"] if a["name"].endswith(".whl"))\n',
    '    whl = f"{DATAGEN_DIR}/{asset[\'name\']}"\n',
    '    print(f"Downloading {asset[\'name\']} ...")\n',
    '    urllib.request.urlretrieve(asset["browser_download_url"], whl)\n',
    "\n",
    'print(f"Installing {os.path.basename(whl)}")\n',
    "subprocess.check_call([\n",
    '    sys.executable, "-m", "pip", "install", whl, "semantic-link-labs",\n',
    '    "-q", "--no-warn-conflicts", "--disable-pip-version-check",\n',
    "])",
]

cell2 = [
    "from datagen import generate\n",
    "\n",
    "report = generate(\n",
    "    spark,\n",
    '    f"{DATAGEN_DIR}/AdventureWorks.vpax",\n',
    "    overwrite=True,\n",
    ")",
]

notebook = {
    "nbformat": 4, "nbformat_minor": 5,
    "metadata": {
        "language_info": {"name": "python"},
        "kernel_info": {"name": "synapse_pyspark"},
        "kernelspec": {"display_name": "synapse_pyspark", "name": "synapse_pyspark"},
        "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
    },
    "cells": [
        {"cell_type": "code", "source": cell1, "execution_count": None, "outputs": [], "metadata": {}},
        {"cell_type": "code", "source": cell2, "execution_count": None, "outputs": [], "metadata": {}},
    ],
}

platform = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "Notebook", "displayName": "Generate AdventureWorks"},
    "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
}

with open(os.path.join(nb_dir, "notebook-content.ipynb"), "w", encoding="utf-8") as f:
    json.dump(notebook, f, indent=2)
with open(os.path.join(nb_dir, ".platform"), "w", encoding="utf-8") as f:
    json.dump(platform, f, indent=2)

print(f"Notebook built: {nb_dir}")
