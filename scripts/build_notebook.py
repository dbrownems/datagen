"""Build the 'Datagen Main.Notebook' artifact for deployment via `fab import`."""
import json
import os
import shutil

nb_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "dist", "Datagen Main.Notebook"))
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
            'DATAGEN_DIR = "/lakehouse/default/Files/datagen"\n',
            "os.makedirs(DATAGEN_DIR, exist_ok=True)\n",
            "\n",
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
            '    "-q", "--no-warn-conflicts", "--disable-pip-version-check", "--force-reinstall", "--no-deps",\n',
            "])\n",
            'subprocess.check_call([sys.executable, "-m", "pip", "install", "semantic-link-labs", "-q", "--no-warn-conflicts", "--disable-pip-version-check"])\n',
            'print("Ready.")',
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
            "| `overwrite` | `False` | `True` to overwrite an existing semantic model |\n",
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
            "VPAX_FILE = \"AdventureWorks.vpax\"  # ← change this to your .vpax filename\n",
            "\n",
            "report = generate(\n",
            "    spark,\n",
            '    f"{DATAGEN_DIR}/{VPAX_FILE}",\n',
            "    overwrite=True,\n",
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
