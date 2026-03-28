# Datagen

Generate realistic test data and semantic models from Power BI `.vpax` files in Microsoft Fabric.

Datagen reads a `.vpax` export (from [DAX Studio](https://daxstudio.org/) or [VertiPaq Analyzer](https://www.sqlbi.com/tools/vertipaq-analyzer/)), generates Delta tables in a Fabric Lakehouse that match the original model's table sizes, column cardinality, and data distributions, then deploys a semantic model (Direct Lake or Import) with all the measures, relationships, format strings, and display folders from the original model.

## Getting Started

### Prerequisites

- A Microsoft Fabric workspace with a Lakehouse
- A `.vpax` file exported from an existing Power BI semantic model
- A Fabric notebook attached to the Lakehouse

### Step 1 — Get the wheel

Build from source:

```bash
git clone https://github.com/dbrownems/datagen.git
cd datagen
pip install build
python -m build --wheel
```

This produces `dist/datagen_fabric-0.3.0-py3-none-any.whl`.

### Step 2 — Upload to your Lakehouse

Upload these files to your Lakehouse **Files/** folder:

| File | Purpose |
|---|---|
| `datagen_fabric-0.3.0-py3-none-any.whl` | The datagen package |
| `YourModel.vpax` | Your model export from DAX Studio |

### Step 3 — Run in a Fabric notebook

```python
# Cell 1 — Install
%pip install /lakehouse/default/Files/datagen_fabric-0.3.0-py3-none-any.whl semantic-link-labs -q

# Cell 2 — Generate everything
from datagen import generate

report = generate(spark, "/lakehouse/default/Files/YourModel.vpax", overwrite=True)
```

That's it. This single call:

1. **Parses** the `.vpax` and infers data distributions from column statistics
2. **Generates** Delta tables matching the original row counts and cardinality
3. **Deploys** a Direct Lake semantic model with all measures, relationships, and column metadata
4. **Compares** the generated model against the source `.vpax` and prints an accuracy report

### Options

```python
generate(spark, "model.vpax",
    output_path="Tables/",           # where Delta tables are written
    seed=42,                         # reproducible generation
    mode="direct_lake",              # "direct_lake" or "import"
    deploy_model=True,               # False to skip model deployment
    compare=True,                    # False to skip comparison report
    dataset="MyModel",               # override the model name
    overwrite=True,                  # overwrite existing model
)
```

## Semantic Model Modes

### Direct Lake (default)

Tables reference Delta files directly through entity partitions — no data import needed. Queries read from OneLake at query time.

```python
generate(spark, "model.vpax", mode="direct_lake")
```

### Import

Tables use Power Query (M) expressions to read from the Lakehouse SQL endpoint. Data is imported into the model and cached in memory. The model is automatically refreshed after deployment.

```python
generate(spark, "model.vpax", mode="import")
```

## Exporting a VPAX File

You need a `.vpax` file from an existing Power BI semantic model. Two ways to get one:

### From DAX Studio

1. Open [DAX Studio](https://daxstudio.org/) and connect to your model
2. Go to **Advanced → Export Metrics** (or **View → Model Metrics → Export**)
3. Save the `.vpax` file

### From VertiPaq Analyzer (Bravo)

1. Open [Bravo for Power BI](https://bravo.bi/)
2. Connect to your model
3. Export the VertiPaq statistics as a `.vpax` file

## Advanced Workflows

### Tweak distributions before generating

If you want to adjust row counts, cardinality, skewness, or other parameters before generating data, use the multi-step workflow:

```bash
# 1. Parse VPAX → editable YAML config
python -m datagen parse model.vpax -o config.yaml

# 2. Edit config.yaml as needed (change row counts, distributions, etc.)

# 3. Generate Delta tables
python -m datagen generate config.yaml -o Tables/
```

Then deploy the semantic model separately in a notebook:

```python
from datagen.model_builder import deploy_semantic_model

deploy_semantic_model("/lakehouse/default/Files/model.vpax", overwrite=True)
```

The config YAML controls data generation only. The `.vpax` is always the source for the semantic model definition (measures, relationships, column metadata).

### Compare a deployed model against the source

```python
from datagen.compare import compare_model

report = compare_model(
    source_vpax_path="/lakehouse/default/Files/model.vpax",
    dataset="MyModel",
)
```

This runs `vertipaq_analyzer` on the deployed model and prints a report like:

```
Table Row Counts:
  ✓ DimCustomer       expected=  18,484  actual=  18,484  (100.0%)
  ✓ FactSales         expected=  60,398  actual=  60,398  (100.0%)

Column Cardinality:
  FactSales:
    ✓ SalesKey         expected=  60,398  actual=  60,398  (100.0%)
    ✓ ProductKey       expected=     158  actual=     158  (100.0%)
    ≈ SalesAmount      expected=   1,021  actual=     987  ( 96.7%)

Overall Accuracy: 98.3%
  ✓ Exact matches: 28    ≈ Close: 4    ✗ Misses: 0
```

### Generate TMDL offline (for version control)

```bash
python -m datagen model model.vpax -o MyModel.SemanticModel --mode import
```

This generates a TMDL folder that can be committed to Git, imported via MCP `ImportFromTmdlFolder`, or used as a Power BI Project (`.pbip`) definition.

## How Data Generation Works

### Key Column Detection

Columns are automatically identified as keys using a scoring heuristic:

| Signal | Score |
|---|---|
| Name ends with Key, ID, Code, Number, etc. | +3 |
| Column participates in a model relationship | +3 |
| Cardinality > 90% of row count | +2 |

Score ≥ 3 → marked as a key column. The key generation style is inferred from the data type:

| Data Type | Key Style | Example Values |
|---|---|---|
| `int64` | `sequential` | 1, 2, 3, 4, … |
| `string` (short) | `prefixed` | PROD-001, PROD-002, … |
| `string` (GUID-like name or length 32-40) | `guid` | `550e8400-e29b-41d4-a716-…` |

Primary keys (cardinality ≥ row count) use direct ID-based indexing so every value appears exactly once. Foreign keys use pool-based selection with realistic frequency skew.

### Value Generation

| Data Type | Method |
|---|---|
| **Strings** | Docker-style names: `bold_raven`, `ancient_ivory_cascade`. Length varies naturally around the target average. |
| **Integers/Doubles** | Skew-normal distribution (scipy if available, numpy fallback). Parameters inferred from VPAX min/max. |
| **Dates** | Random sampling within a date range anchored to Jan 1 of the current year. |
| **Booleans** | Bernoulli sampling with configurable `true_ratio`. |

### Value Frequency

Values are selected from pre-generated pools using either:

- **`uniform`** — equal probability for each value
- **`zipf`** — power-law distribution (some values appear much more frequently)

The `zipf_exponent` controls the skew: `1.0` = moderate, `2.0` = heavy concentration on the most common values.

### Spark Performance

- **`mapInPandas`** — single-pass generation of all columns per partition (no N-way joins)
- **Broadcast value pools** — unique values generated on the driver, broadcast to executors
- **Vectorized numpy** — all value selection uses numpy array operations, not Python loops
- **Deterministic** — same seed produces identical data across runs

## Config Reference

The YAML config is optional — it's auto-generated from the VPAX. But you can edit it to fine-tune generation parameters:

```yaml
model_name: "AdventureWorks"
output_path: "Tables/"
seed: 42

tables:
  - name: "Sales"
    row_count: 1000000
    columns:
      - name: "SalesKey"
        data_type: "int64"
        cardinality: 1000000
        is_key: true
        key_style: "sequential"
        selection: "uniform"
        distribution:
          min: 1

      - name: "SalesAmount"
        data_type: "double"
        cardinality: 50000
        selection: "zipf"
        zipf_exponent: 1.2
        distribution:
          mean: 150.0
          std_dev: 75.0
          skewness: 1.0
          min: 0.01
          max: 10000.0

      - name: "ProductName"
        data_type: "string"
        cardinality: 5000
        selection: "zipf"
        zipf_exponent: 1.5
        distribution:
          avg_length: 18
          style: "docker"        # docker | hex | alpha

      - name: "OrderDate"
        data_type: "datetime"
        cardinality: 730
        distribution:
          start: "2024-01-01"
          end: "2026-01-01"

      - name: "IsReturned"
        data_type: "boolean"
        cardinality: 2
        distribution:
          true_ratio: 0.05
```

### Config Parameters

| Parameter | Description |
|---|---|
| `cardinality` | Number of unique values in the column |
| `is_key` / `key_style` | Key column detection override. Styles: `sequential`, `prefixed`, `guid` |
| `selection` | Value frequency: `uniform` or `zipf` |
| `zipf_exponent` | Skew strength (`1.0` = moderate, `2.0` = heavy) |
| `distribution.mean/std_dev/skewness` | Numeric value pool shape |
| `distribution.min/max` | Value bounds |
| `distribution.prefix` | Prefix for `prefixed` key style (auto-derived from column name) |
| `distribution.avg_length` | Target average string length |
| `distribution.style` | String style: `docker`, `hex`, `alpha` |
| `distribution.start/end` | Date range boundaries |
| `distribution.true_ratio` | Boolean probability of `True` |
| `null_ratio` | Fraction of null values (0.0–1.0) |

## Architecture

```
                    ┌──────────────────┐    ┌────────────┐    ┌─────────────┐
               ┌───▶│ config_generator │───▶│ config.yaml│───▶│ Spark       │───▶ Delta Tables
┌──────────┐   │    └──────────────────┘    │ (optional) │    │ generator   │
│ .vpax    │───┤                            └────────────┘    └─────────────┘
│ file     │   │
└──────────┘   │    ┌──────────────────┐    ┌─────────────────────────────────┐
               ├───▶│ model_builder    │───▶│ Semantic Model (Direct Lake     │
               │    │ (sempy_labs)     │    │ or Import via Power Query)      │
               │    └──────────────────┘    └─────────────────────────────────┘
               │
               │    ┌──────────────────┐    ┌─────────────────────────────────┐
               └───▶│ compare          │───▶│ Accuracy Report (row counts,    │
                    │ (vertipaq_analyzer)   │ cardinality, sizes)             │
                    └──────────────────┘    └─────────────────────────────────┘
```

## API Reference

```python
# ── One call does everything ──
from datagen import generate

report = generate(spark, "model.vpax")
report = generate(spark, "model.vpax", mode="import", overwrite=True)

# ── Individual components ──
from datagen.vpax_parser import parse_vpax
from datagen.config_generator import generate_config
from datagen.config import save_config, load_config
from datagen.spark_generator import generate_all_tables
from datagen.model_builder import deploy_semantic_model, build_tmdl
from datagen.compare import compare_model, compare_vpax

# Parse VPAX
model = parse_vpax("model.vpax")

# Generate config (optionally save/edit)
config = generate_config(model)
save_config(config, "config.yaml")

# Generate Delta tables
generate_all_tables(spark, config)
# or: generate_all_tables(spark, load_config("config.yaml"))

# Deploy semantic model
deploy_semantic_model("model.vpax", mode="import", overwrite=True)

# Compare
report = compare_model("model.vpax", dataset="MyModel")

# Offline TMDL
build_tmdl("model.vpax", "MyModel.SemanticModel", mode="import")
```

## CLI Reference

```bash
# Parse VPAX → YAML config
python -m datagen parse model.vpax -o config.yaml

# Generate Delta tables from config
python -m datagen generate config.yaml -o Tables/

# Generate TMDL folder
python -m datagen model model.vpax --mode direct_lake
python -m datagen model model.vpax --mode import --lakehouse MyLakehouse
```
