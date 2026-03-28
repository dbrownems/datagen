"""PySpark data generation engine for Delta tables.

Uses mapInPandas for single-pass, multi-column generation with broadcast
value pools. Designed for Fabric Spark at scale.
"""

import numpy as np
import pandas as pd
from pathlib import Path

from .pool_generator import generate_value_pool, _parse_fixed_values


def _compute_weights(values_spec, pool_size):
    """Build a probability array for weighted value selection.

    Fixed values with explicit frequency get their specified share.
    All other values (fixed without frequency + generated) split the
    remaining probability equally.

    Returns None if no explicit frequencies are specified (use normal
    selection logic instead).
    """
    if not values_spec or pool_size <= 0:
        return None

    fixed_values, freq_list = _parse_fixed_values(values_spec)
    n_fixed = len(fixed_values)

    # Check if any entry has an explicit frequency
    has_freq = any(f is not None for f in freq_list)
    if not has_freq:
        return None

    # Build weights array for the full pool
    weights = np.zeros(pool_size, dtype=np.float64)

    # Sum of explicit frequency percentages
    explicit_total = sum(f for f in freq_list if f is not None)
    remaining_pct = max(0.0, 100.0 - explicit_total)

    # Count of values without explicit frequency
    n_unweighted = pool_size - sum(1 for f in freq_list if f is not None)
    per_value_pct = remaining_pct / n_unweighted if n_unweighted > 0 else 0.0

    for i in range(pool_size):
        if i < n_fixed and freq_list[i] is not None:
            weights[i] = freq_list[i] / 100.0
        else:
            weights[i] = per_value_pct / 100.0

    # Normalize to sum to 1
    total = weights.sum()
    if total > 0:
        weights /= total

    return weights.tolist()


def _spark_type(data_type):
    """Map data type string to PySpark type."""
    from pyspark.sql.types import (
        StringType, LongType, DoubleType, TimestampType, BooleanType,
    )
    return {
        "string": StringType(),
        "int64": LongType(),
        "double": DoubleType(),
        "datetime": TimestampType(),
        "boolean": BooleanType(),
    }.get(data_type, StringType())


def _build_schema(columns):
    """Build a PySpark StructType from column configs."""
    from pyspark.sql.types import StructType, StructField

    fields = []
    for col in columns:
        name = col.name if hasattr(col, "name") else col["name"]
        dtype = col.data_type if hasattr(col, "data_type") else col["data_type"]
        nullable = col.nullable if hasattr(col, "nullable") else col.get("nullable", False)
        null_ratio = col.null_ratio if hasattr(col, "null_ratio") else col.get("null_ratio", 0)
        fields.append(StructField(name, _spark_type(dtype), nullable or null_ratio > 0))

    return StructType(fields)


def _col_to_dict(col):
    """Convert a ColumnConfig dataclass to a plain dict for broadcast."""
    if isinstance(col, dict):
        return col
    from dataclasses import asdict
    return asdict(col)


def _make_partition_generator(pools_bc, cols_bc, seed_bc, weights_bc):
    """Build the partition generator closure for mapInPandas.

    Keeping this as a factory avoids serializing the broadcast variables
    into the closure — only the broadcast *references* are captured.
    """

    def partition_generator(iterator):
        import numpy as np
        import pandas as pd

        local_pools = pools_bc.value
        local_cols = cols_bc.value
        base_seed = seed_bc.value
        local_weights = weights_bc.value

        # Pre-convert pools to numpy arrays once per executor task
        pool_arrays = {}
        for col in local_cols:
            name = col["name"]
            dt = col["data_type"]
            pool = local_pools[name]
            if dt in ("string", "datetime"):
                pool_arrays[name] = np.array(pool, dtype=object)
            elif dt == "boolean":
                pool_arrays[name] = np.array(pool, dtype=bool)
            elif dt == "int64":
                pool_arrays[name] = np.array(pool, dtype=np.int64)
            else:
                pool_arrays[name] = np.array(pool, dtype=np.float64)

        for pdf in iterator:
            n = len(pdf)
            if n == 0:
                yield pd.DataFrame(columns=[c["name"] for c in local_cols])
                continue

            ids = pdf["id"].values
            result = {}

            for col in local_cols:
                name = col["name"]
                data_type = col["data_type"]
                pool = pool_arrays[name]
                cardinality = len(pool)

                # Deterministic, per-partition, per-column seed
                partition_seed = (base_seed + hash(name) + int(ids[0])) & 0x7FFFFFFF
                rng = np.random.default_rng(partition_seed)

                # --- Boolean: use true_ratio directly ---
                if data_type == "boolean":
                    dist = col.get("distribution") or {}
                    true_ratio = dist.get("true_ratio", 0.5) if isinstance(dist, dict) else 0.5
                    if cardinality == 1:
                        values = np.full(n, pool[0])
                    else:
                        values = rng.random(n) < true_ratio

                # --- Primary key: direct ID-based indexing (each value once) ---
                elif col.get("_primary_key_mode"):
                    indices = (ids % cardinality).astype(np.int64)
                    values = pool[indices]

                # --- All other types: pool-based selection ---
                else:
                    col_weights = local_weights.get(name)
                    if col_weights is not None:
                        # Weighted selection (fixed values with explicit frequencies)
                        w = np.array(col_weights, dtype=np.float64)
                        indices = rng.choice(cardinality, size=n, p=w)
                    elif col.get("selection") == "zipf":
                        exponent = max(col.get("zipf_exponent", 1.0), 0.01)
                        uniform = rng.random(n)
                        indices = np.floor(
                            cardinality * np.power(uniform, exponent)
                        ).astype(np.int64)
                        indices = np.clip(indices, 0, cardinality - 1)
                    else:
                        indices = rng.integers(0, cardinality, size=n)

                    values = pool[indices]

                # --- Null injection ---
                null_ratio = col.get("null_ratio", 0.0) or 0.0
                if null_ratio > 0:
                    null_mask = rng.random(n) < null_ratio
                    values = values.copy()
                    if data_type in ("string", "datetime", "boolean"):
                        values = values.astype(object)
                        values[null_mask] = None
                    else:
                        values = values.astype(np.float64)
                        values[null_mask] = np.nan

                # --- Datetime conversion ---
                if data_type == "datetime":
                    values = pd.to_datetime(pd.Series(values, dtype=object))

                result[name] = values

            yield pd.DataFrame(result)

    return partition_generator


def generate_table(spark, table_config, output_path, global_seed=42, output_format="delta"):
    """Generate a single table and write it as a Delta (or parquet) table.

    Args:
        spark: Active SparkSession.
        table_config: TableConfig dataclass or equivalent dict.
        output_path: Base directory for output.
        global_seed: Seed for reproducible generation.
        output_format: "delta" (default) or "parquet".
    """
    # Normalize access (support both dataclass and dict)
    if hasattr(table_config, "name"):
        table_name = table_config.name
        row_count = table_config.row_count
        columns = table_config.columns
    else:
        table_name = table_config["name"]
        row_count = table_config["row_count"]
        columns = table_config["columns"]

    if not columns:
        print(f"  Skipping table '{table_name}': no columns defined")
        return

    print(f"Generating table '{table_name}' ({row_count:,} rows, {len(columns)} columns)")

    # Phase 1 — generate value pools on the driver
    pools = {}
    weights_map = {}
    for col in columns:
        col_name = col.name if hasattr(col, "name") else col["name"]
        pool = generate_value_pool(col, global_seed)
        pools[col_name] = pool
        print(f"  Pool: {col_name} → {len(pool):,} unique values")

        # Compute per-value weights if fixed values have frequencies
        values_spec = col.values if hasattr(col, "values") else (col.get("values") if isinstance(col, dict) else None)
        w = _compute_weights(values_spec, len(pool))
        if w is not None:
            weights_map[col_name] = w

    # Phase 2 — build output schema
    schema = _build_schema(columns)

    # Phase 3 — broadcast pools and column metadata
    col_dicts = [_col_to_dict(c) for c in columns]

    # Mark primary-key columns so the partition generator can use direct
    # ID-based indexing (guarantees every pool value appears exactly once).
    for cd in col_dicts:
        is_key = cd.get("is_key", False)
        key_style = cd.get("key_style")
        card = cd.get("cardinality", 0)
        if is_key and key_style and card >= row_count:
            cd["_primary_key_mode"] = True

    pools_bc = spark.sparkContext.broadcast(pools)
    cols_bc = spark.sparkContext.broadcast(col_dicts)
    seed_bc = spark.sparkContext.broadcast(global_seed)
    weights_bc = spark.sparkContext.broadcast(weights_map)

    # Phase 4 — generate with mapInPandas (single pass, all columns at once)
    gen_fn = _make_partition_generator(pools_bc, cols_bc, seed_bc, weights_bc)
    df = spark.range(0, row_count)
    result_df = df.mapInPandas(gen_fn, schema)

    # Phase 5 — write output
    table_path = f"{output_path.rstrip('/')}/{table_name}"
    fmt = output_format.lower()
    print(f"  Writing {fmt} table → {table_path}")
    result_df.write.format(fmt).mode("overwrite").save(table_path)
    print(f"  ✓ {table_name} complete")

    # Cleanup
    pools_bc.unpersist()
    cols_bc.unpersist()
    seed_bc.unpersist()
    weights_bc.unpersist()


def generate_all_tables(spark, config, output_path=None, output_format="delta", vpax_model=None):
    """Generate all tables defined in the configuration.

    Args:
        spark: Active SparkSession.
        config: GenerationConfig dataclass, dict, or path to a YAML config file.
        output_path: Override the output_path in the config (optional).
        output_format: "delta" (default) or "parquet".
        vpax_model: Parsed VPAX model dict (optional). When provided,
            date dimension tables are auto-detected and generated with
            derived column values instead of random data.
    """
    # Load from file if a path is given
    if isinstance(config, (str, Path)):
        from .config import load_config
        config = load_config(str(config))

    # Normalize access
    if hasattr(config, "tables"):
        tables = config.tables
        model_name = config.model_name
        out_path = output_path or config.output_path
        seed = config.seed
    else:
        tables = config.get("tables", [])
        model_name = config.get("model_name", "Model")
        out_path = output_path or config.get("output_path", "Tables/")
        seed = config.get("seed", 42)

    # Build a lookup of VPAX table metadata for date table detection
    vpax_tables = {}
    if vpax_model:
        for t in vpax_model.get("tables", []):
            vpax_tables[t["name"]] = t

    n = len(tables)
    print(f"{'=' * 60}")
    print(f"Datagen: generating {n} table{'s' if n != 1 else ''} for '{model_name}'")
    print(f"Output : {out_path}")
    print(f"Seed   : {seed}")
    print(f"{'=' * 60}")
    print()

    for i, table in enumerate(tables, 1):
        tname = table.name if hasattr(table, "name") else table["name"]
        print(f"[{i}/{n}] {tname}")

        # Check if this is a date table (using VPAX metadata for column roles)
        vpax_table = vpax_tables.get(tname)
        if vpax_table and _is_date_table(vpax_table):
            _generate_date_table_spark(spark, vpax_table, out_path, output_format)
        else:
            generate_table(spark, table, out_path, seed, output_format)
        print()

    print(f"{'=' * 60}")
    print(f"All {n} tables generated successfully.")
    print(f"{'=' * 60}")


def _is_date_table(vpax_table):
    """Check if a VPAX table is a date dimension."""
    from .date_table import is_date_table
    return is_date_table(vpax_table)


def _generate_date_table_spark(spark, vpax_table, output_path, output_format):
    """Generate a date table from VPAX metadata and write it."""
    from .date_table import generate_date_table

    tname = vpax_table["name"]
    row_count = vpax_table.get("row_count", 365)

    print(f"  Date table detected — generating {row_count:,} days with derived columns")

    rows = generate_date_table(vpax_table)

    pdf = pd.DataFrame(rows)

    # Convert datetime columns
    for col in vpax_table.get("columns", []):
        cname = col["name"]
        if cname not in pdf.columns:
            continue
        if col.get("data_type") == "datetime":
            pdf[cname] = pd.to_datetime(pdf[cname])
        elif col.get("data_type") == "int64":
            pdf[cname] = pdf[cname].astype("int64")
        elif col.get("data_type") == "boolean":
            pdf[cname] = pdf[cname].astype("bool")

    sdf = spark.createDataFrame(pdf)

    table_path = f"{output_path.rstrip('/')}/{tname}"
    fmt = output_format.lower()
    print(f"  Writing {fmt} table → {table_path}")
    sdf.write.format(fmt).mode("overwrite").save(table_path)
    print(f"  ✓ {tname} complete ({row_count:,} rows, {len(pdf.columns)} columns)")
