# Datagen — Fabric Notebook
#
# Single-call pipeline:  .vpax → Delta tables → semantic model → comparison report
#
# Upload to lakehouse Files/:
#   - datagen_fabric-*.whl    (any version)
#   - model.vpax              (from DAX Studio / VertiPaq Analyzer)

# %% Cell 1 — Setup
import glob, subprocess, sys

whl = sorted(glob.glob("/lakehouse/default/Files/datagen_fabric-*.whl"))[-1]
print(f"Installing {whl}")
subprocess.check_call([sys.executable, "-m", "pip", "install", whl, "semantic-link-labs", "-q", "--no-warn-conflicts", "--disable-pip-version-check"])

# %% Cell 2 — Generate everything and compare
from datagen import generate

report = generate(spark, "/lakehouse/default/Files/AdventureWorks.vpax", overwrite=True)

# Options:
#   generate(spark, "model.vpax",
#       mode="import",               # "direct_lake" (default) or "import"
#       deploy_model=False,          # skip model deployment
#       compare=False,               # skip comparison report
#       seed=42,                     # reproducible generation
#   )
