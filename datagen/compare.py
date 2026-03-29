"""Compare generated Delta tables against the source VPAX statistics.

Queries the generated Delta tables directly with Spark to get row counts
and column cardinality, then compares against the source .vpax file.
No semantic model or DAX queries required.
"""

import pandas as pd
from .vpax_parser import parse_vpax


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


def _extract_source_stats(vpax_path):
    """Parse a VPAX file into a flat list of {table, column, stat} dicts."""
    model = parse_vpax(vpax_path)
    rows = []

    for table in model.get("tables", []):
        tname = table["name"]
        row_count = table.get("row_count", 0)

        rows.append({
            "table": tname,
            "column": "(table)",
            "metric": "row_count",
            "expected": row_count,
        })

        for col in table.get("columns", []):
            cname = col["name"]
            rows.append({
                "table": tname,
                "column": cname,
                "metric": "cardinality",
                "expected": col.get("cardinality", 0),
            })

    return pd.DataFrame(rows), model


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


def compare_tables(spark, vpax_path, output_path="Tables/", print_report=True):
    """Compare generated Delta tables against the source VPAX.

    Reads the Delta tables directly with Spark to get actual row counts
    and cardinality, then compares against the .vpax statistics.

    Args:
        spark: Active SparkSession.
        vpax_path: Path to the original .vpax file.
        output_path: Base directory where Delta tables were written.
        print_report: Whether to print the report to stdout.

    Returns:
        pandas.DataFrame with the comparison report.
    """
    source_df, model = _extract_source_stats(vpax_path)
    source_tables = model.get("tables", [])
    name = model.get("model_name", "Model")

    print(f"  Comparing generated tables against {name} ...")
    actual_df = _extract_spark_stats(spark, source_tables, output_path)

    report = _build_report(source_df, actual_df)

    if print_report:
        _print_report(report, name)

    return report


def compare_vpax(source_vpax_path, generated_vpax_path, print_report=True):
    """Compare two VPAX files and report accuracy.

    Args:
        source_vpax_path: Path to the original .vpax file.
        generated_vpax_path: Path to the generated model's .vpax export.
        print_report: Whether to print the report to stdout.

    Returns:
        pandas.DataFrame with the comparison report.
    """
    source_df, model = _extract_source_stats(source_vpax_path)

    gen_model = parse_vpax(generated_vpax_path)
    gen_rows = []
    for table in gen_model.get("tables", []):
        tname = table["name"]
        gen_rows.append({
            "table": tname, "column": "(table)",
            "metric": "row_count", "actual": table.get("row_count", 0),
        })
        for col in table.get("columns", []):
            gen_rows.append({
                "table": tname, "column": col["name"],
                "metric": "cardinality", "actual": col.get("cardinality", 0),
            })
    actual_df = pd.DataFrame(gen_rows)

    report = _build_report(source_df, actual_df)

    if print_report:
        _print_report(report, model.get("model_name", ""))

    return report
