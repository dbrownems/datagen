"""Compare generated Delta tables against the generation config.

Queries the generated Delta tables with Spark to get actual row counts
and column cardinality, then compares against the expected values from
the generation config (YAML or in-memory GenerationConfig).

The config is the source of truth — not the VPAX — so tweaked row counts,
cardinalities, and distributions are reflected in the report.
"""

import pandas as pd


def _pct_diff(actual, expected):
    """Percentage difference, handling zeros."""
    if expected == 0:
        return 0.0 if actual == 0 else float("inf")
    return abs(actual - expected) / expected * 100


def _accuracy(actual, expected):
    """Accuracy as 0–100%, where 100% means exact match."""
    if expected == 0:
        return 100.0 if actual == 0 else 0.0
    return max(0.0, 100.0 - _pct_diff(actual, expected))


def _extract_config_stats(config):
    """Extract expected row counts and cardinality from a GenerationConfig.

    Args:
        config: GenerationConfig dataclass, dict, or path to a YAML file.

    Returns:
        (pandas.DataFrame, model_name, tables_list)
    """
    from pathlib import Path

    # Load from file if needed
    if isinstance(config, (str, Path)):
        from .config import load_config
        config = load_config(str(config))

    # Normalize access
    if hasattr(config, "tables"):
        tables = config.tables
        model_name = config.model_name
    else:
        tables = config.get("tables", [])
        model_name = config.get("model_name", "Model")

    rows = []
    tables_out = []

    for table in tables:
        tname = table.name if hasattr(table, "name") else table["name"]
        row_count = table.row_count if hasattr(table, "row_count") else table.get("row_count", 0)
        columns = table.columns if hasattr(table, "columns") else table.get("columns", [])

        rows.append({
            "table": tname,
            "column": "(table)",
            "metric": "row_count",
            "expected": row_count,
        })

        col_list = []
        for col in columns:
            cname = col.name if hasattr(col, "name") else col["name"]
            card = col.cardinality if hasattr(col, "cardinality") else col.get("cardinality", 0)
            rows.append({
                "table": tname,
                "column": cname,
                "metric": "cardinality",
                "expected": card,
            })
            col_list.append({"name": cname})

        tables_out.append({"name": tname, "columns": col_list})

    return pd.DataFrame(rows), model_name, tables_out


def _extract_spark_stats(spark, source_tables, output_path):
    """Get row counts and cardinality from Delta tables using Spark.

    Args:
        spark: Active SparkSession.
        source_tables: List of table dicts from the parsed VPAX.
        output_path: Base path where Delta tables were written.

    Returns:
        pandas.DataFrame with table/column/metric/actual columns.
    """
    rows = []
    base = output_path.rstrip("/")

    for table in source_tables:
        tname = table["name"]
        table_path = f"{base}/{tname}"

        try:
            df = spark.read.format("delta").load(table_path)
        except Exception:
            try:
                df = spark.read.parquet(table_path)
            except Exception:
                print(f"    ⚠ Could not read {table_path}")
                rows.append({
                    "table": tname, "column": "(table)",
                    "metric": "row_count", "actual": 0,
                })
                continue

        row_count = df.count()
        rows.append({
            "table": tname, "column": "(table)",
            "metric": "row_count", "actual": row_count,
        })

        # Column cardinality — single pass with countDistinct for all columns
        col_names = [c["name"] for c in table.get("columns", [])
                     if c["name"] in df.columns]

        if col_names:
            from pyspark.sql.functions import countDistinct
            agg_exprs = [countDistinct(c).alias(c) for c in col_names]
            card_row = df.agg(*agg_exprs).collect()[0]

            for cname in col_names:
                rows.append({
                    "table": tname, "column": cname,
                    "metric": "cardinality", "actual": int(card_row[cname]),
                })

        print(f"    {tname}: {row_count:,} rows, {len(col_names)} columns checked")

    return pd.DataFrame(rows)


def _build_report(source_df, actual_df):
    """Merge source and actual stats, compute accuracy."""
    if source_df.empty:
        return pd.DataFrame()

    merged = source_df.merge(
        actual_df,
        on=["table", "column", "metric"],
        how="left",
    )

    merged["actual"] = merged["actual"].fillna(0).astype(int)
    merged["expected"] = merged["expected"].fillna(0).astype(int)
    merged["diff"] = merged["actual"] - merged["expected"]
    merged["diff_%"] = merged.apply(
        lambda r: round(_pct_diff(r["actual"], r["expected"]), 1), axis=1
    )
    merged["accuracy_%"] = merged.apply(
        lambda r: round(_accuracy(r["actual"], r["expected"]), 1), axis=1
    )

    return merged.sort_values(["table", "column", "metric"]).reset_index(drop=True)


def _print_report(report, model_name=""):
    """Print a formatted comparison report."""
    if report.empty:
        print("No comparison data available.")
        return

    header = f"Comparison Report: {model_name}" if model_name else "Comparison Report"
    print(f"\n{'=' * 80}")
    print(header)
    print(f"{'=' * 80}\n")

    # Summary by table
    tables = report[report["column"] == "(table)"]
    columns = report[report["column"] != "(table)"]

    if not tables.empty:
        print("Table Row Counts:")
        print("-" * 60)
        for _, row in tables.iterrows():
            status = "✓" if row["accuracy_%"] >= 99 else "≈" if row["accuracy_%"] >= 90 else "✗"
            print(f"  {status} {row['table']:30s}  expected={row['expected']:>10,}  "
                  f"actual={row['actual']:>10,}  ({row['accuracy_%']:5.1f}%)")
        print()

    # Cardinality report by table
    card = columns[columns["metric"] == "cardinality"]
    if not card.empty:
        print("Column Cardinality:")
        print("-" * 80)
        current_table = None
        for _, row in card.iterrows():
            if row["table"] != current_table:
                current_table = row["table"]
                print(f"\n  {current_table}:")
            status = "✓" if row["accuracy_%"] >= 99 else "≈" if row["accuracy_%"] >= 90 else "✗"
            print(f"    {status} {row['column']:30s}  expected={row['expected']:>10,}  "
                  f"actual={row['actual']:>10,}  ({row['accuracy_%']:5.1f}%)")
        print()

    # Overall summary
    avg_accuracy = report["accuracy_%"].mean()
    exact_matches = (report["accuracy_%"] >= 99.9).sum()
    close_matches = ((report["accuracy_%"] >= 90) & (report["accuracy_%"] < 99.9)).sum()
    misses = (report["accuracy_%"] < 90).sum()

    print(f"{'=' * 80}")
    print(f"Overall Accuracy: {avg_accuracy:.1f}%")
    print(f"  ✓ Exact matches (≥99.9%):  {exact_matches}")
    print(f"  ≈ Close matches (90-99%):  {close_matches}")
    print(f"  ✗ Misses (<90%):           {misses}")
    print(f"  Total metrics compared:    {len(report)}")
    print(f"{'=' * 80}\n")


def compare_tables(spark, config, output_path="Tables/", print_report=True):
    """Compare generated Delta tables against the generation config.

    Reads the Delta tables with Spark to get actual row counts and
    cardinality, then compares against the expected values from the
    config (row_count and cardinality per column).

    Args:
        spark: Active SparkSession.
        config: GenerationConfig dataclass, dict, or path to a YAML file.
        output_path: Base directory where Delta tables were written.
        print_report: Whether to print the report to stdout.

    Returns:
        pandas.DataFrame with the comparison report.
    """
    source_df, model_name, tables_list = _extract_config_stats(config)

    print(f"  Comparing generated tables against config ({model_name}) ...")
    actual_df = _extract_spark_stats(spark, tables_list, output_path)

    report = _build_report(source_df, actual_df)

    if print_report:
        _print_report(report, model_name)

    return report
