# Datagen

Generate realistic test data and semantic models from Power BI `.vpax` files in Microsoft Fabric.

Datagen reads a `.vpax` export (from [DAX Studio](https://daxstudio.org/) or [VertiPaq Analyzer](https://www.sqlbi.com/tools/vertipaq-analyzer/)), generates Delta tables in a Fabric Lakehouse that match the original model's table sizes, column cardinality, and data distributions, then deploys a semantic model (Direct Lake or Import) with all the measures, relationships, format strings, and display folders from the original model.

## Requirements

- A Microsoft Fabric workspace on a capacity that supports Spark notebooks (F2+ or trial).
- A Lakehouse attached to the notebook for table output.
- A `.vpax` file describing the source model (see step 4 below).

## Quick Start

### 1. Get the notebook

Either:

- **Clone this repo** — the notebook lives at [`notebook/Datagen Main.Notebook/`](notebook/Datagen%20Main.Notebook/), or
- **Download** `Datagen Main.Notebook` from the [latest release](https://github.com/dbrownems/datagen/releases/latest).

The first code cell auto-downloads the latest `datagen` wheel from GitHub, so no Python packaging is required.

### 2. Deploy the notebook to Fabric

Using the [Fabric CLI](https://learn.microsoft.com/fabric/cicd/fabric-cli/fabric-cli-overview):

```bash
fab import "YourWorkspace.Workspace/Datagen Main.Notebook" -i "notebook/Datagen Main.Notebook" -f
```

Or import manually: in your Fabric workspace, click **New → Import notebook** and select `notebook/Datagen Main.Notebook/notebook-content.ipynb`.

### 3. Attach a Lakehouse

Open the notebook in Fabric. In the left sidebar, click the Lakehouse icon and attach (or create) a Lakehouse.

### 4. Upload your `.vpax` file

Upload your `.vpax` file to `Files/datagen/` in the attached Lakehouse.

> **How to get a `.vpax` file:** Open [DAX Studio](https://daxstudio.org/), connect to your model, then go to **Advanced → Export Metrics**.

### 5. Run the notebook

The notebook walks through:

1. Install `datagen` (auto-downloads the latest release wheel)
2. Parse the `.vpax` and write a companion YAML config beside it
3. Generate Delta tables matching the original sizes and cardinality
4. Deploy a semantic model with all measures and relationships
5. Print a comparison report showing accuracy

The first run produces `<your-model>.yaml` next to the `.vpax`. Edit
that file to tweak distributions, then re-run — the YAML is reused as
the source of truth on subsequent runs. Delete the YAML to start over
from the `.vpax`.

## Semantic Model Modes

### Import (default)

Tables use Power Query (M) expressions to read from the Lakehouse SQL endpoint. Data is imported into the model and cached in memory.

```python
generate(spark, f"{DATAGEN_DIR}/model.vpax")               # mode="import"
```

### Direct Lake

Tables reference Delta files directly — no data import. Queries read from OneLake at query time.

```python
generate(spark, f"{DATAGEN_DIR}/model.vpax", mode="direct_lake")
```

> Direct Lake refuses any relationship whose two columns have different
> data types. Datagen auto-aligns these by **PK-wins** — the FK side
> (the "many" column) is converted to match the PK side (the "one"
> column) — and logs a warning with the exact YAML override applied,
> plus the alternative "FK wins" override you can paste if you want
> the opposite conversion. PK-wins minimises cascading changes since
> a single PK is usually referenced by many FKs. The same `data_type`
> values are used to build the Spark schema, so the generated Delta
> tables match what the BIM expects at deploy time. Pass
> `auto_fix_relationship_types=False` to disable and just log
> mismatches.

## Options

```python
generate(spark, f"{DATAGEN_DIR}/model.vpax",
    mode="import",                       # "import" (default) or "direct_lake"
    deploy_model=True,                   # False to skip semantic model deployment
    compare=True,                        # False to skip comparison report
    overwrite_tables=True,               # False to skip tables that already exist
    overwrite_model=True,                # False to fail if model already exists
    seed=42,                             # reproducible generation
    output_path="Tables/",               # Delta table location
    dataset="MyModel",                   # override model name
    workspace=None,                      # target Fabric workspace (default: current)
    lakehouse=None,                      # lakehouse name (default: attached)
    auto_fix_relationship_types=True,    # PK-wins: align FK column type to PK (DL only)
)
```

## Advanced Workflows

### Tweak distributions before generating

`generate()` always parses the `.vpax` (it's the source of measures,
relationships, and BIM metadata for the semantic model), then looks
for a sibling `<vpax-name>.yaml` in the same folder:

- **No sibling YAML** → infer the config from the `.vpax` column
  statistics, save it as `<vpax-name>.yaml`, and use it.
- **Sibling YAML present** → load it as the source of truth for table
  sizes, cardinality, distributions, fixed values, and histograms.
  Inferred values, query-seeding, and BIM cross-referencing are
  **skipped** so your edits aren't clobbered.

The natural workflow:

1. Drop a `.vpax` into `Files/datagen/` and run the notebook —
   datagen writes `<your-model>.yaml` beside it.
2. Edit the YAML to tweak row counts, cardinality, distributions,
   `values:` overrides, or `histogram:` entries.
3. Re-run the notebook — your edits are picked up.
4. To start over from the `.vpax`, delete the `.yaml`.

See [`datagen.example.yaml`](datagen.example.yaml) for a fully
commented sample.

From the CLI:

```bash
python -m datagen parse model.vpax -o config.yaml
# edit config.yaml — change row counts, cardinality, distributions, histograms, etc.
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

### Histograms (table-level value pinning)

Pin specific column-value tuples to a target row count or fraction
of the table. Useful for shaping fact tables to match an observed
slicer/filter distribution, or for guaranteeing that specific
combinations exist in the generated data.

```yaml
- name: Sales
  row_count: 100000
  histogram:
    - values: {ProductId: 1, RegionId: 5}
      rows: 0.15            # 15% of row_count
    - values: {ProductId: 2, RegionId: 5}
      rows: 0.05            # 5% of row_count
```

Or as absolute counts:

```yaml
- name: Sales
  row_count: 100000
  histogram:
    - values: {ProductId: 1, RegionId: 5}
      rows: 15000
    - values: {ProductId: 2, RegionId: 5}
      rows: 5000
```

Rules:

- All `rows` values in one table's histogram must be either fractions
  in `[0, 1)` or integer counts `>= 1`. Mixing is an error.
- Sum of fractions must be `<= 1.0`; sum of counts must be
  `<= row_count`. Remaining rows are generated by the normal
  distribution.
- Pinned values override the column's normal value pool only for
  the rows in that histogram entry; other columns in those rows are
  still generated normally.
- If a pinned column is a foreign key, the referenced parent table's
  primary-key column is automatically seeded with those values, so
  the FK references always resolve.
- Multiple child tables may have histograms that reference the same
  parent; the required PK values from each child are accumulated
  and de-duplicated on the parent.

**Limitation:** if a parent table has its *own* histogram on the same
PK column being seeded by a child, the parent's pins may displace the
child's seeded values at low row IDs. Avoid this combination for now.

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

Deploy the wheel and notebook to Fabric:

```bash
fab copy dist/datagen_fabric-*.whl "MyWorkspace.Workspace/MyLakehouse.Lakehouse/Files/datagen/" -f
fab import "MyWorkspace.Workspace/Datagen Main.Notebook" -i "notebook/Datagen Main.Notebook" -f
```

> **Releases** include the wheel and `Datagen Main.Notebook.zip`. Cell 1 of the notebook downloads the latest wheel automatically, so end users don't normally need to build from source.

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
python -m datagen parse model.vpax [-o config.yaml] [--output-path Tables/] [--seed 42] \
    [--dax-trace trace.jsonl] [--top-n 200] [--observed-share 0.7]

# Generate Delta tables from a YAML config
python -m datagen generate config.yaml [-o Tables/]

# Generate a TMDL semantic model definition
python -m datagen model model.vpax [--mode direct_lake|import] [--lakehouse MyLakehouse] \
    [--endpoint <sql-endpoint>] [--model-name MyModel] [-o MyModel.SemanticModel]

# Re-apply DAX literal seeding to an existing YAML config
python -m datagen seed-literals config.yaml --dax-trace trace.jsonl
```

## Companion: Load Testing

For DAX load testing against the generated semantic models, see the separate [DaxLoadGen](https://github.com/dbrownems/DaxLoadGen) repo (.NET tool that drives concurrent XMLA queries with optional RLS user impersonation and read-only replica routing).

## License

[MIT](LICENSE)
