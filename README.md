# Datagen

Generate realistic test data and semantic models from Power BI `.vpax` files in Microsoft Fabric.

Datagen reads a `.vpax` export (from [DAX Studio](https://daxstudio.org/) or [VertiPaq Analyzer](https://www.sqlbi.com/tools/vertipaq-analyzer/)), generates Delta tables in a Fabric Lakehouse that match the original model's table sizes, column cardinality, and data distributions, then deploys a semantic model (Direct Lake or Import) with all the measures, relationships, format strings, and display folders from the original model.

## Quick Start

### 1. Download the notebook

Download `Datagen Main.Notebook` from the [latest release](https://github.com/dbrownems/datagen/releases/latest).

### 2. Deploy the notebook to Fabric

Using the [Fabric CLI](https://learn.microsoft.com/fabric/cicd/fabric-cli/fabric-cli-overview):

```bash
fab import "YourWorkspace.Workspace/Datagen Main.Notebook" -i "Datagen Main.Notebook" -f
```

Or import manually: in your Fabric workspace, click **New → Import notebook** and select the `notebook-content.ipynb` file from inside the downloaded folder.

### 3. Attach a Lakehouse

Open the notebook in Fabric. In the left sidebar, click the Lakehouse icon and attach (or create) a Lakehouse.

### 4. Upload your `.vpax` file

Upload your `.vpax` file to `Files/datagen/` in the attached Lakehouse.

> **How to get a `.vpax` file:** Open [DAX Studio](https://daxstudio.org/), connect to your model, then go to **Advanced → Export Metrics**.

### 5. Run the notebook

- **Cell 1** installs datagen (auto-downloads the latest release from GitHub if needed)
- **Cell 2** — edit the `.vpax` filename, then run. It will:
  1. Parse the `.vpax` and infer data distributions
  2. Generate Delta tables matching the original sizes and cardinality
  3. Deploy a Direct Lake semantic model with all measures and relationships
  4. Print a comparison report showing accuracy

That's it — no Python packaging or development required.

## Semantic Model Modes

### Direct Lake (default)

Tables reference Delta files directly — no data import. Queries read from OneLake at query time.

```python
generate(spark, f"{DATAGEN_DIR}/model.vpax", mode="direct_lake")
```

### Import

Tables use Power Query (M) expressions to read from the Lakehouse SQL endpoint. Data is imported into the model and cached in memory.

```python
generate(spark, f"{DATAGEN_DIR}/model.vpax", mode="import")
```

## Options

```python
generate(spark, f"{DATAGEN_DIR}/model.vpax",
    mode="direct_lake",              # "direct_lake" or "import"
    deploy_model=True,               # False to skip semantic model deployment
    compare=True,                    # False to skip comparison report
    overwrite_tables=True,           # False to skip tables that already exist
    overwrite_model=True,            # False to fail if model already exists
    seed=42,                         # reproducible generation
    output_path="Tables/",           # Delta table location
    output_format="delta",           # "delta" or "parquet"
    dataset="MyModel",               # override model name
    workspace=None,                  # target Fabric workspace (default: current)
    lakehouse=None,                  # lakehouse name (default: attached)
    include_hidden=False,            # include hidden columns/tables from VPAX
    include_calculated=False,        # include calculated columns
)
```

## Advanced Workflows

### Tweak distributions before generating

Export a YAML config, edit it, then generate from the config:

```bash
python -m datagen parse model.vpax -o config.yaml
# edit config.yaml — change row counts, cardinality, distributions, etc.
python -m datagen generate config.yaml
```

Then deploy the semantic model in a notebook:

```python
from datagen.model_builder import deploy_semantic_model
deploy_semantic_model("/lakehouse/default/Files/datagen/model.vpax", overwrite=True)
```

### Specify fixed values

Pin specific values in a column with optional frequency control:

```yaml
columns:
  - name: "Priority"
    data_type: "string"
    cardinality: 4
    values:
      - value: "Critical"
        frequency: 5          # 5% of rows
      - value: "High"
        frequency: 15         # 15% of rows
      - value: "Medium"
        frequency: 50         # 50% of rows
      - "Low"                 # gets remaining 30%
```

### Generate TMDL offline

```bash
python -m datagen model model.vpax --mode import --lakehouse MyLakehouse
```

## How Data Generation Works

### Key Column Detection

Columns are automatically identified as keys using name patterns (Key, ID, Code, etc.), relationship membership, and cardinality ratio. Key styles:

| Data Type | Style | Example |
|---|---|---|
| `int64` | `sequential` | 1, 2, 3, … |
| `string` | `prefixed` | PROD-001, PROD-002, … |
| `string` (GUID) | `guid` | `550e8400-e29b-…` |

### Date Tables

Date dimensions are auto-detected and generated with derived columns (Year, Quarter, MonthName, DayOfWeek, etc.) from a contiguous date spine. DateKey uses smart integer format (`20260101`).

### Geography Columns

Country, State, City, and PostalCode columns are auto-detected and generated with valid hierarchies — cities appear in their correct states and countries. Uses real places (60 cities across US, Canada, Mexico, UK, France, Germany).

### String Values

Docker-style human-readable names: `bold_raven`, `ancient_ivory_cascade`. Length varies naturally around the target average.

### Email Columns

Email and username columns are auto-detected by name (e.g. `Email`, `UserPrincipalName`, `Login`) and populated with realistic addresses. Internal/employee tables use Marvel hero names at `@contoso.com`; external tables (customers, suppliers) use DC hero names at `@adventureworks.com` or `@wideworldimporters.com`.

### Numeric Distributions

Skew-normal distributions with configurable mean, std_dev, and skewness. Value frequency uses Zipf power-law selection for realistic skew.

### Spark Performance

- `mapInPandas` single-pass generation (no N-way joins)
- Broadcast value pools to executors
- Vectorized numpy operations
- Deterministic — same seed produces identical data

## Building from Source

```bash
git clone https://github.com/dbrownems/datagen.git
cd datagen
pip install build
python -m build --wheel
# Output: dist/datagen_fabric-*.whl
```

Deploy to Fabric:

```bash
python scripts/build_notebook.py
fab copy dist/datagen_fabric-*.whl "MyWorkspace.Workspace/MyLakehouse.Lakehouse/Files/datagen/" -f
fab import "MyWorkspace.Workspace/Datagen Main.Notebook" -i "dist/Datagen Main.Notebook" -f
```

## API Reference

```python
from datagen import generate
from datagen.model_builder import deploy_semantic_model, build_tmdl
from datagen.compare import compare_tables
from datagen.vpax_parser import parse_vpax
from datagen.config_generator import generate_config
from datagen.config import save_config, load_config
from datagen.spark_generator import generate_all_tables
```

## CLI Reference

```bash
# Parse a .vpax → YAML config
python -m datagen parse model.vpax [-o config.yaml] [--output-path Tables/] [--seed 42] [--include-hidden] [--include-calculated]

# Generate Delta tables from a YAML config
python -m datagen generate config.yaml [-o Tables/]

# Generate a TMDL semantic model definition
python -m datagen model model.vpax [--mode direct_lake|import] [--lakehouse MyLakehouse] [--endpoint <sql-endpoint>] [--model-name MyModel] [-o MyModel.SemanticModel] [--include-hidden] [--include-calculated]
```
