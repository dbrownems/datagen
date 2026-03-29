# Datagen — Fabric Notebook
#
# Single-call pipeline:  .vpax → Delta tables → semantic model → comparison report
#
# Upload your .vpax file to lakehouse Files/datagen/
# The wheel is auto-downloaded from GitHub if not present.

# %% Cell 1 — Setup
import glob, subprocess, sys, os, urllib.request

DATAGEN_DIR = "/lakehouse/default/Files/datagen"
os.makedirs(DATAGEN_DIR, exist_ok=True)

# Find or download the latest wheel
whls = sorted(glob.glob(f"{DATAGEN_DIR}/datagen_fabric-*.whl"))
if whls:
    whl = whls[-1]
else:
    # Download latest release from GitHub
    import json
    api_url = "https://api.github.com/repos/dbrownems/datagen/releases/latest"
    with urllib.request.urlopen(api_url) as resp:
        release = json.loads(resp.read())
    asset = next(a for a in release["assets"] if a["name"].endswith(".whl"))
    whl = f"{DATAGEN_DIR}/{asset['name']}"
    print(f"Downloading {asset['name']} ...")
    urllib.request.urlretrieve(asset["browser_download_url"], whl)

print(f"Installing {os.path.basename(whl)}")
subprocess.check_call([
    sys.executable, "-m", "pip", "install", whl, "semantic-link-labs",
    "-q", "--no-warn-conflicts", "--disable-pip-version-check",
])

# %% Cell 2 — Generate everything and compare
from datagen import generate

report = generate(
    spark,
    f"{DATAGEN_DIR}/AdventureWorks.vpax",
    overwrite=True,
)

# Options:
#   generate(spark, f"{DATAGEN_DIR}/model.vpax",
#       mode="import",               # "direct_lake" (default) or "import"
#       deploy_model=False,          # skip model deployment
#       compare=False,               # skip comparison report
#       seed=42,                     # reproducible generation
#   )
