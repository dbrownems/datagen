"""Compare a generated semantic model against the source VPAX statistics.

Two modes:
  - ``compare_vpax()`` — compare two .vpax files (source vs generated)
  - ``compare_model()`` — compare a live Fabric semantic model against
    the source .vpax (uses ``sempy_labs.vertipaq_analyzer``)

Both produce a pandas DataFrame report with per-column accuracy metrics.
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

        # Table-level row
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
            if col.get("total_size"):
                rows.append({
                    "table": tname,
                    "column": cname,
                    "metric": "total_size",
                    "expected": col["total_size"],
                })
            if col.get("dictionary_size"):
                rows.append({
                    "table": tname,
                    "column": cname,
                    "metric": "dictionary_size",
                    "expected": col["dictionary_size"],
                })

    return pd.DataFrame(rows), model


def _extract_vpax_stats(vpax_path):
    """Extract stats from a second VPAX in the same format."""
    model = parse_vpax(vpax_path)
    rows = []

    for table in model.get("tables", []):
        tname = table["name"]
        rows.append({
            "table": tname,
            "column": "(table)",
            "metric": "row_count",
            "actual": table.get("row_count", 0),
        })
        for col in table.get("columns", []):
            cname = col["name"]
            rows.append({
                "table": tname,
                "column": cname,
                "metric": "cardinality",
                "actual": col.get("cardinality", 0),
            })
            if col.get("total_size"):
                rows.append({
                    "table": tname,
                    "column": cname,
                    "metric": "total_size",
                    "actual": col["total_size"],
                })
            if col.get("dictionary_size"):
                rows.append({
                    "table": tname,
                    "column": cname,
                    "metric": "dictionary_size",
                    "actual": col["dictionary_size"],
                })

    return pd.DataFrame(rows)


def _extract_live_stats_dax(dataset, source_tables, workspace=None):
    """Get row counts and cardinality from a live model using DAX.

    Args:
        dataset: Semantic model name.
        source_tables: List of table dicts from the parsed VPAX (with columns).
        workspace: Fabric workspace.

    Returns:
        pandas.DataFrame with table/column/metric/actual columns.
    """
    try:
        import sempy.fabric as fabric
    except ImportError:
        raise ImportError(
            "sempy (semantic-link) is required for compare_model()."
        )

    rows = []
    print("  Querying model for row counts and cardinality ...")

    for table in source_tables:
        tname = table["name"]

        # Row count
        try:
            dax = f'EVALUATE ROW("v", COUNTROWS(\'{tname}\'))'
            result = fabric.evaluate_dax(
                dataset=dataset, dax_string=dax, workspace=workspace,
            )
            row_count = int(result.iloc[0, 0]) if not result.empty else 0
        except Exception:
            row_count = 0

        rows.append({
            "table": tname, "column": "(table)",
            "metric": "row_count", "actual": row_count,
        })

        # Column cardinality
        for col in table.get("columns", []):
            cname = col["name"]
            try:
                dax = f'EVALUATE ROW("v", DISTINCTCOUNT(\'{tname}\'[{cname}]))'
                result = fabric.evaluate_dax(
                    dataset=dataset, dax_string=dax, workspace=workspace,
                )
                card = int(result.iloc[0, 0]) if not result.empty else 0
            except Exception:
                card = 0

            rows.append({
                "table": tname, "column": cname,
                "metric": "cardinality", "actual": card,
            })

        print(f"    {tname}: {row_count:,} rows")

    return pd.DataFrame(rows)


def _find_col(df, *candidates):
    """Find the first matching column name in a DataFrame."""
    cols_lower = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name in df.columns:
            return name
        if name.lower() in cols_lower:
            return cols_lower[name.lower()]
    return None


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
    actual_df = _extract_vpax_stats(generated_vpax_path)

    report = _build_report(source_df, actual_df)

    if print_report:
        _print_report(report, model.get("model_name", ""))

    return report


def compare_model(source_vpax_path, dataset=None, workspace=None, print_report=True):
    """Compare a live semantic model against the source VPAX.

    Queries the deployed model with DAX (COUNTROWS / DISTINCTCOUNT) to
    get actual row counts and cardinality, then compares against the
    source .vpax statistics.

    Args:
        source_vpax_path: Path to the original .vpax file.
        dataset: Semantic model name (defaults to VPAX model name).
        workspace: Fabric workspace name or ID.
        print_report: Whether to print the report to stdout.

    Returns:
        pandas.DataFrame with the comparison report.
    """
    source_df, model = _extract_source_stats(source_vpax_path)
    name = dataset or model.get("model_name", "Model")

    # Use DAX queries — works reliably on Direct Lake without refresh
    source_tables = model.get("tables", [])
    actual_df = _extract_live_stats_dax(name, source_tables, workspace)

    report = _build_report(source_df, actual_df)

    if print_report:
        _print_report(report, name)

    return report
