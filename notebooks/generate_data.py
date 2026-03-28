# Datagen — Fabric Notebook
#
# Single-call pipeline:  .vpax → Delta tables → Direct Lake semantic model → comparison report
#
# Upload to lakehouse Files/:
#   - datagen_fabric-0.2.0-py3-none-any.whl
#   - model.vpax   (from DAX Studio / VertiPaq Analyzer)

# %% Cell 1 — Setup
import sys
sys.path.insert(0, "/lakehouse/default/Files")
%pip install /lakehouse/default/Files/datagen_fabric-0.2.0-py3-none-any.whl semantic-link-labs -q

# %% Cell 2 — Generate everything and compare
from datagen import generate

report = generate(spark, "/lakehouse/default/Files/model.vpax")

# `report` is a pandas DataFrame with per-column accuracy metrics.
# The comparison report is also printed to stdout automatically.
#
# To skip model deployment (just generate data):
#   generate(spark, "model.vpax", deploy_model=False, compare=False)
#
# To compare an already-deployed model against the source VPAX:
#   from datagen.compare import compare_model
#   report = compare_model("model.vpax", dataset="MyModel")
