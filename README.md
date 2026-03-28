# Datagen

Generate realistic Delta tables and Direct Lake semantic models from Power BI model metadata (`.vpax` files).

## Overview

Datagen is a pipeline with three commands:

1. **`parse`** — Extract table/column statistics from a `.vpax` file → tweakable YAML config
2. **`generate`** — Create Delta tables in Fabric Spark matching size, cardinality, and distributions
3. **`model`** — Generate a Direct Lake semantic model definition (TMDL) from the `.vpax` metadata

## Installation

```bash
pip install -r requirements.txt
```

In Fabric, only `pyyaml` needs to be installed — `pyspark`, `numpy`, `pandas`, and `delta-spark` are pre-installed.

## Quick Start

### One call does everything (Fabric notebook)

```python
from datagen import generate

generate(spark, "/lakehouse/default/Files/model.vpax")
```

This single call:
1. Reads the `.vpax` and infers data distributions from the column statistics
2. Generates Delta tables matching the original size and cardinality
3. Deploys a Direct Lake semantic model with all measures, relationships,
   format strings, and display folders from the `.vpax`

Options:
```python
generate(spark, "model.vpax",
    output_path="Tables/",       # Delta table location
    seed=42,                     # reproducible generation
    deploy_model=True,           # False to skip model deployment
    dataset="MyModel",           # override model name
    overwrite=True,              # overwrite existing model
)
```

### Advanced: tweak distributions before generating

If you want to adjust the inferred distributions (cardinality, skewness,
row counts, etc.), use the multi-step workflow:

```bash
# 1. Parse VPAX → editable YAML config
python -m datagen parse model.vpax -o config.yaml

# 2. Edit config.yaml as needed ...

# 3. Generate Delta tables from the config
python -m datagen generate config.yaml
```

Then deploy the semantic model separately in a Fabric notebook:
```python
from datagen.model_builder import deploy_semantic_model
deploy_semantic_model("/lakehouse/default/Files/model.vpax")
```

The config YAML controls data generation only; the `.vpax` is always the
source for the semantic model definition.

**Offline TMDL generation** (for version control or manual import):

```bash
python -m datagen model model.vpax --lakehouse MyLakehouse -o MyModel.SemanticModel
```

## Config Reference

```yaml
model_name: "AdventureWorks"
output_path: "Tables/"
seed: 42

tables:
  - name: "Sales"
    row_count: 1000000
    columns:
      # --- Sequential integer key (primary) ---
      - name: "SalesKey"
        data_type: "int64"
        cardinality: 1000000
        is_key: true
        key_style: "sequential"       # 1, 2, 3, ...
        selection: "uniform"
        distribution:
          min: 1

      # --- Foreign key (references DimProduct) ---
      - name: "ProductKey"
        data_type: "int64"
        cardinality: 5000
        is_key: true
        key_style: "sequential"
        selection: "zipf"             # some products sold more often
        zipf_exponent: 1.3
        distribution:
          min: 1

      # --- Prefixed string key ---
      - name: "InvoiceID"
        data_type: "string"
        cardinality: 1000000
        is_key: true
        key_style: "prefixed"         # INV-000001, INV-000002, ...
        selection: "uniform"
        distribution:
          prefix: "INV"

      # --- GUID key ---
      - name: "TransactionGUID"
        data_type: "string"
        cardinality: 1000000
        is_key: true
        key_style: "guid"             # random UUID v4 strings
        selection: "uniform"

      # --- Numeric column ---
      - name: "SalesAmount"
        data_type: "double"
        cardinality: 50000
        selection: "zipf"
        zipf_exponent: 1.2
        nullable: false
        distribution:
          mean: 150.0
          std_dev: 75.0
          skewness: 1.0
          min: 0.01
          max: 10000.0

      # --- String column (docker-style names) ---
      - name: "ProductName"
        data_type: "string"
        cardinality: 5000
        selection: "zipf"
        zipf_exponent: 1.5
        distribution:
          avg_length: 18
          style: "docker"             # docker | hex | alpha

      # --- Date column ---
      - name: "OrderDate"
        data_type: "datetime"
        cardinality: 730
        selection: "uniform"
        distribution:
          start: "2024-01-01"
          end: "2026-01-01"

      # --- Boolean column ---
      - name: "IsReturned"
        data_type: "boolean"
        cardinality: 2
        distribution:
          true_ratio: 0.05

      # --- Integer column ---
      - name: "Quantity"
        data_type: "int64"
        cardinality: 100
        selection: "zipf"
        zipf_exponent: 1.8
        distribution:
          mean: 5
          std_dev: 3
          min: 1
          max: 100
```

### Key Parameters

| Parameter | Description |
|---|---|
| `cardinality` | Number of unique values in the column |
| `is_key` | `true` if the column is a key (auto-detected from name, relationships, cardinality) |
| `key_style` | `sequential` (1,2,3…), `prefixed` (PROD-001,PROD-002…), or `guid` (UUID v4 strings) |
| `selection` | How values are picked: `uniform` (equal frequency) or `zipf` (power-law skew) |
| `zipf_exponent` | Controls skew in `zipf` selection. `1.0` = moderate, `2.0` = heavy concentration on top values |
| `distribution.mean/std_dev/skewness` | Shape of the numeric value pool |
| `distribution.min` | Start value for sequential keys, or lower bound for numeric distributions |
| `distribution.prefix` | String prefix for `prefixed` key style (auto-derived from column name) |
| `distribution.avg_length` | Target average string length for text columns |
| `distribution.style` | String generation style: `docker` (human-readable adj_noun), `hex`, `alpha` |
| `distribution.true_ratio` | Probability of `True` for boolean columns |
| `distribution.start/end` | Date range (defaults anchor to Jan 1 of the current year) |
| `null_ratio` | Fraction of values that should be null (0.0–1.0) |

### Key Column Detection

When parsing a `.vpax` file, columns are scored as potential keys using:
- **Name** (+3): ends with Key, ID, Code, Number, etc.
- **Relationships** (+3): column participates in a model relationship
- **Cardinality** (+2): unique values > 90% of row count

Score ≥ 3 → marked as key. Key style is then inferred:
- `int64` → `sequential`
- `string` with GUID-like name or avg length 32–40 → `guid`
- `string` otherwise → `prefixed`

Primary keys (cardinality ≥ row_count) use direct ID-based indexing so every
value appears exactly once. Foreign keys use normal pool-based selection.

### String Styles

- **`docker`** — Human-readable names like `bold_raven`, `ancient_ivory_cascade`. Length varies naturally.
- **`hex`** — Random hex strings like `a3f7b2c1`. Fixed length.
- **`alpha`** — Random alphabetic strings like `qvxmtb`. Fixed length.

## Architecture

```
                    ┌──────────────────┐    ┌────────────┐    ┌─────────────┐
               ┌───▶│ config_generator │───▶│ config.yaml│───▶│ Spark       │───▶ Delta Tables
┌──────────┐   │    └──────────────────┘    │ (tweak me) │    │ generator   │
│ .vpax    │───┤                            └────────────┘    └─────────────┘
│ file     │   │                                                    │
└──────────┘   │    ┌──────────────────┐    ┌────────────┐    ┌─────┴───────┐
               └───▶│ model_builder    │───▶│ TMDL folder│───▶│ MCP Deploy  │───▶ Semantic Model
                    │ (Direct Lake)    │    │            │    │ to Fabric   │
                    └──────────────────┘    └────────────┘    └─────────────┘
```

### Performance Design

- **`mapInPandas`** — Single-pass generation of all columns per partition. No N-way joins.
- **Broadcast pools** — Unique value pools are pre-generated on the driver and broadcast to executors.
- **Vectorized numpy** — All value selection uses numpy array operations, not Python loops.
- **Deterministic** — Same seed produces the same data across runs.

## API Usage

```python
# ── Simplest: one call ──
from datagen import generate

generate(spark, "model.vpax")


# ── Fine-grained: separate steps ──
from datagen.vpax_parser import parse_vpax
from datagen.config_generator import generate_config
from datagen.config import save_config, load_config
from datagen.spark_generator import generate_all_tables
from datagen.model_builder import deploy_semantic_model

# Infer config from VPAX (optionally save/edit it)
model = parse_vpax("model.vpax")
config = generate_config(model)
save_config(config, "config.yaml")       # optional — edit then reload

generate_all_tables(spark, config)        # or load_config("config.yaml")
deploy_semantic_model("model.vpax")       # model definition from VPAX
```
