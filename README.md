# Datagen

Generate realistic test data and semantic models from Power BI `.vpax` files in Microsoft Fabric.

Datagen reads a `.vpax` export (from [DAX Studio](https://daxstudio.org/) or [VertiPaq Analyzer](https://www.sqlbi.com/tools/vertipaq-analyzer/)), generates Delta tables in a Fabric Lakehouse that match the original model's table sizes, column cardinality, and data distributions, then deploys a semantic model (Direct Lake on OneLake or Import) with all the measures, relationships, format strings, and display folders from the original model.

## Requirements

- A Microsoft Fabric workspace on a capacity that supports Spark notebooks (F2+ or trial).
- A Lakehouse attached to the notebook for table output.
- A `.vpax` file describing the source model.

## Quick Start

1. **Get the notebook** — clone this repo (`notebook/Datagen Main.Notebook/`) or download `Datagen Main.Notebook.zip` from the [latest release](https://github.com/dbrownems/datagen/releases/latest). The first cell auto-downloads the latest `datagen` wheel from GitHub, so no Python packaging is required.

2. **Deploy the notebook to Fabric** — using the [Fabric CLI](https://learn.microsoft.com/fabric/cicd/fabric-cli/fabric-cli-overview):

   ```bash
   fab import "YourWorkspace.Workspace/Datagen Main.Notebook" -i "notebook/Datagen Main.Notebook" -f
   ```

   Or import manually: **New → Import notebook** → select `notebook-content.ipynb`.

3. **Attach a Lakehouse** in Fabric (left sidebar).

4. **Upload your `.vpax`** to `Files/datagen/` in the attached Lakehouse.

   > **How to get a `.vpax`:** in [DAX Studio](https://daxstudio.org/), connect to your model, then **Advanced → Export Metrics**.

5. **Run the notebook.** It installs `datagen`, parses the `.vpax`, generates tables, deploys the semantic model, and prints a comparison report.

The first run produces `<your-model>.yaml` next to the `.vpax`. Edit that file to tweak distributions, then re-run — the YAML is reused as the source of truth on subsequent runs. Delete it to start over from the `.vpax`.

## Semantic Model Modes

### Import (default)

Tables use Power Query (M) expressions to read from the Lakehouse SQL endpoint. Data is imported into the model and cached in memory.

```python
generate(spark, f"{DATAGEN_DIR}/model.vpax")               # mode="import"
```

### Direct Lake on OneLake

Tables reference Delta files in OneLake directly — no data import, no SQL endpoint. Queries read from OneLake at query time.

```python
generate(spark, f"{DATAGEN_DIR}/model.vpax", mode="direct_lake")
```

> Direct Lake refuses any relationship whose two columns have different data types. Datagen aligns these directly in the BIM using **PK-wins** (the FK column's data type is changed to match the PK), so all original relationships survive deployment. Pass `auto_fix_relationship_types=False` to disable.

`mode` is validated strictly. Common aliases (`"directlake"`, `"direct-lake"`, `"dl"`) are accepted; anything else raises `ValueError` immediately so typos surface at the top of the run.

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
    queries_path=None,                   # seed column values from real DAX queries — see below
)
```

### Seeding column values from real DAX queries

Pass `queries_path` to make the generator pin the literal values it sees in actual DAX queries (e.g. `'Region'[Country] = "France"` → "France" gets pinned into the `Country` column's value pool). Useful when you want generated tables to return non-empty results for a captured workload.

Two input formats:

1. **List/tuple of DAX query strings** — convenient for a small set of queries from code:

   ```python
   generate(spark, "model.vpax", queries_path=[
       "EVALUATE FILTER('Sales', 'Region'[Country] = \"France\")",
       "EVALUATE CALCULATETABLE('Customer', 'Customer'[Segment] IN {\"SMB\", \"Enterprise\"})",
   ])
   ```

2. **Path to an XEvents JSONL trace** (e.g. captured from DAX Studio's All Queries tracer or Profiler) — one JSON object per line. Only the DAX query text is read; everything else is ignored. Lines must look like:

   ```jsonc
   {"eventClass": "QueryEnd", "cols": {"TextData": "EVALUATE ..."}}
   ```

   Other fields (timestamps, durations, user names, etc.) may be present but are not used. Lines whose `eventClass` is not `"QueryEnd"`, or whose `cols.TextData` is missing/empty, are skipped silently.

   ```python
   generate(spark, "model.vpax", queries_path="/lakehouse/default/Files/datagen/trace.jsonl")
   ```

Seeding is only applied on a fresh run (no sibling YAML present) — once the YAML exists it's the source of truth and `queries_path` is ignored.

## Tweaking the Generated Data

`generate()` always parses the `.vpax` (it's the source of measures, relationships, and BIM metadata for the semantic model), then looks for a sibling `<vpax-name>.yaml`:

- **No sibling YAML** → infer the config from the `.vpax` and write it.
- **Sibling YAML present** → load it as the source of truth. Inferred values, query-seeding, and BIM cross-referencing are skipped so your edits aren't clobbered.

Workflow: drop the `.vpax`, run, edit the `.yaml`, re-run. Delete the YAML to start over.

See [`datagen.example.yaml`](datagen.example.yaml) for a fully commented sample.

### Fixed values

```yaml
columns:
  - name: Priority
    data_type: string
    cardinality: 4
    values:
      - {value: Critical, frequency: 5}    # 5% of rows
      - {value: High,     frequency: 15}
      - {value: Medium,   frequency: 50}
      - Low                                # remaining 30%
```

### Histograms (table-level value pinning)

Pin specific column-value tuples to a target row count or fraction of the table. Useful for shaping fact tables to match an observed slicer/filter distribution.

```yaml
- name: Sales
  row_count: 100000
  histogram:
    - values: {ProductId: 1, RegionId: 5}
      rows: 0.15            # 15% of row_count (or use an integer like 15000)
    - values: {ProductId: 2, RegionId: 5}
      rows: 0.05
```

Rules:

- All `rows` values in one histogram must be either fractions in `[0, 1)` or integer counts `>= 1` (no mixing).
- Sum of fractions must be `<= 1.0`; sum of counts must be `<= row_count`. Remaining rows fall back to the normal distribution.
- If a pinned column is a foreign key, the parent table's PK is automatically seeded with those values so the FK always resolves.

## How Generation Works

**Key columns** are detected by name pattern (`Key`, `Id`, `Code`, …), relationship membership, and cardinality ratio. Integer keys are sequential (`1, 2, …`); string keys are prefixed (`PROD-001, …`); GUID-shaped string keys generate random GUIDs.

**Date dimensions** are auto-detected and built from a contiguous spine with derived columns (Year, Quarter, MonthName, DayOfWeek, …) and an integer-format `DateKey` (`20260101`).

**Geography columns** (Country/State/City/PostalCode) are populated from a hierarchy of real places so cities appear in the right state and country.

**String values** use Docker-style human-readable names (`bold_raven`, `ancient_ivory_cascade`) with length varying around the column average.

**Email/username columns** are detected by name and populated with `@contoso.com` (internal) or `@adventureworks.com` / `@wideworldimporters.com` (external).

**Numeric distributions** are skew-normal (configurable mean / std_dev / skewness). Value-frequency selection uses Zipf for realistic heavy tails.

**Spark execution** uses `mapInPandas` single-pass generation, broadcast value pools, and vectorised numpy. Same seed produces identical data.

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
fab cp dist/datagen_fabric-*.whl "MyWorkspace.Workspace/MyLakehouse.Lakehouse/Files/datagen/" -f
fab import "MyWorkspace.Workspace/Datagen Main.Notebook" -i "notebook/Datagen Main.Notebook" -f
```

Releases include the wheel and `Datagen Main.Notebook.zip`. Cell 1 of the notebook downloads the latest wheel automatically, so end users don't normally need to build from source.

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

For DAX load testing against the generated semantic models, see the separate [DaxLoadGen](https://github.com/dbrownems/DaxLoadGen) repo.

## License

[MIT](LICENSE)
