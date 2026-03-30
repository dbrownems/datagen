"""PySpark data generation engine for Delta tables.

Uses mapInPandas for single-pass, multi-column generation with broadcast
value pools. Designed for Fabric Spark at scale.
"""

import sys
import numpy as np
import pandas as pd
from pathlib import Path

from .pool_generator import generate_value_pool, _parse_fixed_values


import logging as _logging

_logger = _logging.getLogger("datagen")
if not _logger.handlers:
    _handler = _logging.StreamHandler(sys.stderr)
    _handler.setFormatter(_logging.Formatter("%(message)s"))
    _logger.addHandler(_handler)
    _logger.setLevel(_logging.INFO)


def _log(msg=""):
    """Log progress — uses stderr + print + flush for Fabric notebook visibility."""
    print(msg, flush=True)
    _logger.info(msg)


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

            # Pre-compute shared geo indices (all geo columns use the same index)
            geo_indices = None
            email_indices = None

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

                # --- Correlated geo column: share indices with other geo cols ---
                elif col.get("_geo_group"):
                    if geo_indices is None:
                        geo_seed = (base_seed + hash("_geo_group") + int(ids[0])) & 0x7FFFFFFF
                        geo_rng = np.random.default_rng(geo_seed)
                        geo_indices = geo_rng.integers(0, cardinality, size=n)
                    values = pool[geo_indices % cardinality]

                # --- Correlated email+name: share indices ---
                elif col.get("_email_group"):
                    if email_indices is None:
                        email_seed = (base_seed + hash("_email_group") + int(ids[0])) & 0x7FFFFFFF
                        email_rng = np.random.default_rng(email_seed)
                        email_indices = email_rng.integers(0, cardinality, size=n)
                    values = pool[email_indices % cardinality]

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
        return

    import time as _time
    _t0 = _time.time()

    # Phase 1 — generate value pools on the driver (silent)
    pools = {}
    weights_map = {}

    # Detect correlated geo columns (city/state/country/postal in same table)
    geo_cols = {}  # col_name → geo_type
    for col in columns:
        col_name = col.name if hasattr(col, "name") else col["name"]
        dist = col.distribution if hasattr(col, "distribution") else (col.get("distribution") or {})
        style = dist.style if hasattr(dist, "style") else (dist.get("style") if isinstance(dist, dict) else None)
        geo_type = dist.geo_type if hasattr(dist, "geo_type") else (dist.get("geo_type") if isinstance(dist, dict) else None)
        if style == "geo" and geo_type:
            geo_cols[col_name] = geo_type

    # If we have 2+ geo columns, generate correlated records
    geo_records = None
    if len(geo_cols) >= 2:
        from .geo_generator import generate_geo_records
        city_col = next((cn for cn, gt in geo_cols.items() if gt == "city"), None)
        if city_col:
            city_card = next(
                (col.cardinality if hasattr(col, "cardinality") else col.get("cardinality", 100))
                for col in columns
                if (col.name if hasattr(col, "name") else col["name"]) == city_col
            )
        else:
            city_card = max(
                (col.cardinality if hasattr(col, "cardinality") else col.get("cardinality", 100))
                for col in columns
                if (col.name if hasattr(col, "name") else col["name"]) in geo_cols
            )
        geo_records = generate_geo_records(city_card, seed=global_seed)
        geo_field_map = {"city": "city", "state": "state", "country": "country", "postal_code": "postal_code"}
        for col_name, geo_type in geo_cols.items():
            field = geo_field_map.get(geo_type, geo_type)
            pools[col_name] = [r[field] for r in geo_records]

    # Detect correlated email+name columns
    from .email_generator import is_email_column, is_name_column, email_to_name
    email_col_name = None
    name_col_name = None
    for col in columns:
        cn = col.name if hasattr(col, "name") else col["name"]
        if is_email_column(cn):
            email_col_name = cn
        elif is_name_column(cn):
            name_col_name = cn

    if email_col_name and name_col_name:
        if email_col_name not in pools:
            email_col = next(c for c in columns
                             if (c.name if hasattr(c, "name") else c["name"]) == email_col_name)
            pools[email_col_name] = generate_value_pool(email_col, global_seed, table_name=table_name)
        pools[name_col_name] = [email_to_name(e) for e in pools[email_col_name]]

    for col in columns:
        col_name = col.name if hasattr(col, "name") else col["name"]
        if col_name in geo_cols and geo_records is not None:
            continue
        if col_name == name_col_name and email_col_name:
            continue
        if col_name == email_col_name and name_col_name and email_col_name in pools:
            continue

        pool = generate_value_pool(col, global_seed, table_name=table_name)
        pools[col_name] = pool

        values_spec = col.values if hasattr(col, "values") else (col.get("values") if isinstance(col, dict) else None)
        w = _compute_weights(values_spec, len(pool))
        if w is not None:
            weights_map[col_name] = w

    _t1 = _time.time()

    # Phase 2 — build schema + broadcast
    schema = _build_schema(columns)
    col_dicts = [_col_to_dict(c) for c in columns]

    for cd in col_dicts:
        is_key = cd.get("is_key", False)
        key_style = cd.get("key_style")
        card = cd.get("cardinality", 0)
        if is_key and key_style and card >= row_count:
            cd["_primary_key_mode"] = True

    if geo_records is not None:
        for cd in col_dicts:
            if cd.get("name") in geo_cols:
                cd["_geo_group"] = True

    if email_col_name and name_col_name:
        for cd in col_dicts:
            if cd.get("name") in (email_col_name, name_col_name):
                cd["_email_group"] = True

    pools_bc = spark.sparkContext.broadcast(pools)
    cols_bc = spark.sparkContext.broadcast(col_dicts)
    seed_bc = spark.sparkContext.broadcast(global_seed)
    weights_bc = spark.sparkContext.broadcast(weights_map)

    _t2 = _time.time()

    # Phase 3 — generate and write
    gen_fn = _make_partition_generator(pools_bc, cols_bc, seed_bc, weights_bc)
    df = spark.range(0, row_count)
    result_df = df.mapInPandas(gen_fn, schema)

    table_path = f"{output_path.rstrip('/')}/{table_name}"
    fmt = output_format.lower()
    job_desc = f"datagen: {table_name} ({row_count:,} rows)"
    spark.sparkContext.setJobGroup(f"datagen_{table_name}", job_desc)
    spark.sparkContext.setJobDescription(job_desc)
    spark.sparkContext.setLocalProperty("callSite.short", job_desc)
    spark.sparkContext.setLocalProperty("callSite.long", job_desc)
    result_df.write.format(fmt).mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(table_path)
    spark.sparkContext.setJobGroup(None, None)
    _t3 = _time.time()

    # Cleanup
    pools_bc.unpersist()
    cols_bc.unpersist()
    seed_bc.unpersist()
    weights_bc.unpersist()

    # Return timing for caller to display
    return {
        "pool_time": _t1 - _t0,
        "broadcast_time": _t2 - _t1,
        "write_time": _t3 - _t2,
        "total_time": _t3 - _t0,
    }


def generate_all_tables(spark, config, output_path=None, output_format="delta", vpax_model=None, overwrite=True):
    """Generate all tables defined in the configuration.

    Args:
        spark: Active SparkSession.
        config: GenerationConfig dataclass, dict, or path to a YAML config file.
        output_path: Override the output_path in the config (optional).
        output_format: "delta" (default) or "parquet".
        vpax_model: Parsed VPAX model dict (optional). When provided,
            date dimension tables are auto-detected and generated with
            derived column values instead of random data.
        overwrite: If True, regenerate all tables. If False, skip tables
            that already exist as Delta/parquet and only generate missing ones.
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

    # Detect schema-enabled lakehouse (Tables/dbo/ exists)
    import os
    if out_path.rstrip("/") == "Tables" and os.path.isdir("Tables/dbo"):
        out_path = "Tables/dbo/"
    elif out_path.rstrip("/") == "Tables" and os.path.isdir("/lakehouse/default/Tables/dbo"):
        out_path = "Tables/dbo/"

    n = len(tables)
    total_rows = sum(
        (t.row_count if hasattr(t, "row_count") else t.get("row_count", 0))
        for t in tables
    )

    from . import __version__
    print(f"Datagen v{__version__} — {model_name}")
    print(f"  Tables: {n}  |  Total rows: {total_rows:,}  |  Output: {out_path}  |  Seed: {seed}")
    print(flush=True)

    import time as _time
    _overall_t0 = _time.time()

    # Try to use tqdm for progress bar (available in Fabric)
    try:
        from tqdm.auto import tqdm
        progress = tqdm(total=n, desc="Tables", unit="table", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
    except ImportError:
        progress = None

    rows_generated = 0
    errors = []
    succeeded_tables = []
    skipped_tables = []

    # Check which tables already exist (for overwrite=False)
    def _table_exists(tname):
        table_path = f"{out_path.rstrip('/')}/{tname}"
        try:
            # Try to read as Delta — works for both relative and absolute paths
            df = spark.read.format("delta").load(table_path)
            df.limit(0).collect()  # force evaluation
            return True
        except Exception:
            return False

    for i, table in enumerate(tables, 1):
        tname = table.name if hasattr(table, "name") else table["name"]
        row_count = table.row_count if hasattr(table, "row_count") else table.get("row_count", 0)
        n_cols = len(table.columns if hasattr(table, "columns") else table.get("columns", []))

        if progress:
            progress.set_postfix_str(f"{tname} ({row_count:,} rows)")

        # Skip existing tables when not overwriting
        if not overwrite and _table_exists(tname):
            succeeded_tables.append(tname)
            skipped_tables.append(tname)
            if progress:
                progress.write(f"  ⊘ {tname} — exists, skipped")
                progress.update(1)
            else:
                print(f"  [{i}/{n}] ⊘ {tname} — exists, skipped", flush=True)
            continue

        if progress:
            progress.set_postfix_str(f"{tname} ({row_count:,} rows)")

        try:
            vpax_table = vpax_tables.get(tname)
            if vpax_table and _is_date_table(vpax_table):
                _generate_date_table_spark(spark, vpax_table, out_path, output_format)
                timing = {"total_time": 0}
            else:
                timing = generate_table(spark, table, out_path, seed, output_format) or {}

            total_time = timing.get("total_time", 0)
            rows_generated += row_count
            succeeded_tables.append(tname)

            if progress:
                progress.write(f"  ✓ {tname} — {row_count:,} rows, {n_cols} cols ({total_time:.1f}s)")
            else:
                print(f"  [{i}/{n}] ✓ {tname} — {row_count:,} rows, {n_cols} cols ({total_time:.1f}s)", flush=True)

        except Exception as e:
            errors.append((tname, str(e)))
            if progress:
                progress.write(f"  ✗ {tname} — {e}")
            else:
                print(f"  [{i}/{n}] ✗ {tname} — {e}", flush=True)

        if progress:
            progress.update(1)

    if progress:
        progress.close()

    _overall_elapsed = _time.time() - _overall_t0
    rows_per_sec = int(rows_generated / _overall_elapsed) if _overall_elapsed > 0 else 0

    print(flush=True)
    generated = n - len(errors) - len(skipped_tables)
    parts = [f"Generated: {generated}/{n} tables"]
    if skipped_tables:
        parts.append(f"Skipped: {len(skipped_tables)}")
    if errors:
        parts.append(f"Errors: {len(errors)}")
    parts.append(f"{rows_generated:,} rows")
    parts.append(f"{_overall_elapsed:.0f}s")
    if rows_generated > 0:
        parts.append(f"{rows_per_sec:,} rows/s")
    print("  |  ".join(parts))
    if errors:
        for tname, err in errors:
            print(f"  ✗ {tname}: {err}")
    print(flush=True)

    return succeeded_tables, out_path


def _is_date_table(vpax_table):
    """Check if a VPAX table is a date dimension."""
    from .date_table import is_date_table
    return is_date_table(vpax_table)


def _generate_date_table_spark(spark, vpax_table, output_path, output_format):
    """Generate a date table from VPAX metadata and write it."""
    from .date_table import generate_date_table

    tname = vpax_table["name"]
    row_count = vpax_table.get("row_count", 365)

    _log(f"  Date table detected — generating {row_count:,} days with derived columns")

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
    job_desc = f"datagen: {tname} (date table, {row_count:,} rows)"
    spark.sparkContext.setJobGroup(f"datagen_{tname}", job_desc)
    spark.sparkContext.setJobDescription(job_desc)
    spark.sparkContext.setLocalProperty("callSite.short", job_desc)
    spark.sparkContext.setLocalProperty("callSite.long", job_desc)
    sdf.write.format(fmt).mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(table_path)
    spark.sparkContext.setJobGroup(None, None)
