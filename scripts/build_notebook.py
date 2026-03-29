"""Build the Fabric notebook artifact for deployment via `fab import`."""
import json
import os
import shutil

nb_dir = os.path.join(os.path.dirname(__file__), "..", "dist", "GenerateAdventureWorks.Notebook")
nb_dir = os.path.normpath(nb_dir)
if os.path.exists(nb_dir):
    shutil.rmtree(nb_dir)
os.makedirs(nb_dir)

notebook = {
    "nbformat": 4, "nbformat_minor": 5,
    "metadata": {
        "language_info": {"name": "python"},
        "kernel_info": {"name": "synapse_pyspark"},
        "kernelspec": {"display_name": "synapse_pyspark", "name": "synapse_pyspark"},
        "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
    },
    "cells": [
        {
            "cell_type": "code",
            "source": [
                "import glob, subprocess, sys\n",
                "\n",
                'whl = sorted(glob.glob("/lakehouse/default/Files/datagen_fabric-*.whl"))[-1]\n',
                'print(f"Installing {whl}")\n',
                "subprocess.check_call([\n",
                '    sys.executable, "-m", "pip", "install", whl, "semantic-link-labs",\n',
                '    "-q", "--no-warn-conflicts", "--disable-pip-version-check",\n',
                "])",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
        {
            "cell_type": "code",
            "source": [
                "from datagen import generate\n",
                "\n",
                "report = generate(\n",
                "    spark,\n",
                '    "/lakehouse/default/Files/AdventureWorks.vpax",\n',
                "    overwrite=True,\n",
                ")",
            ],
            "execution_count": None, "outputs": [], "metadata": {},
        },
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
