"""DAX query rewriter — replace filter literal values with generated data.

Parses Power BI-generated DAX queries to find filter/slicer bindings
(TREATAS, IN, direct comparisons) and replaces the literal values with
values that exist in the generated Delta tables.

Preserves:
- Column reference strings (e.g. "'Table'[Column]" used as parameter values)
- Constants in DEFINE MEASURE blocks (e.g. "Current Week", "Yes")
- Query structure and formatting
"""

import json
import re
from collections import defaultdict


# ---------------------------------------------------------------------------
# Pattern extraction — find (table, column, values, span) from DAX text
# ---------------------------------------------------------------------------

# Matches 'TableName'[ColumnName]
_TABLE_COL_RE = r"'([^']+)'\[([^\]]+)\]"

# Column reference pattern — a string literal that looks like 'Table'[Column]
_COL_REF_RE = re.compile(r"""^'[^']+'\[[^\]]+\]$""")


def _is_column_reference(value):
    """Check if a string value is actually a column reference, not a data value."""
    return bool(_COL_REF_RE.match(value))


def extract_filter_bindings(dax_text):
    """Extract all filter binding literals from a DAX query.

    Returns a list of dicts with keys:
        table: str — table name
        column: str — column name
        values: list[str] — literal values
        span: (start, end) — character positions in the original text
        pattern: str — "treatas", "in", or "compare"

    Skips:
        - Values that look like column references ('Table'[Column])
        - Bindings inside DEFINE MEASURE blocks (hardcoded logic constants)
    """
    bindings = []

    # Find the boundary of DEFINE ... EVALUATE to separate measure defs from query
    define_end = 0
    eval_match = re.search(r'\bEVALUATE\b', dax_text, re.IGNORECASE)
    if eval_match:
        define_end = eval_match.start()

    # Also find VAR __DS blocks (PBI filter variable declarations)
    # These come after EVALUATE in PBI-generated queries but are filter bindings
    # Actually in PBI queries, the pattern is:
    #   DEFINE MEASURE ... EVALUATE ... WHERE ... __DS0FilterTable ...
    # The TREATAS/IN patterns appear in VAR definitions after the measures

    # 1. TREATAS({values}, 'Table'[Column])
    for m in re.finditer(
        r"TREATAS\s*\(\s*\{([^}]*)\}\s*,\s*" + _TABLE_COL_RE,
        dax_text,
    ):
        vals_str = m.group(1)
        table = m.group(2)
        column = m.group(3)

        # Extract string values from the value list
        str_vals = re.findall(r'"([^"]*)"', vals_str)

        # Filter out column references
        data_vals = [v for v in str_vals if not _is_column_reference(v)]
        if not data_vals:
            continue

        bindings.append({
            "table": table,
            "column": column,
            "values": data_vals,
            "span": (m.start(1), m.end(1)),
            "pattern": "treatas",
        })

    # 2. 'Table'[Column] IN {values}
    for m in re.finditer(
        _TABLE_COL_RE + r"\s+IN\s*\{([^}]*)\}",
        dax_text,
    ):
        table = m.group(1)
        column = m.group(2)
        vals_str = m.group(3)

        str_vals = re.findall(r'"([^"]*)"', vals_str)
        # Also handle numeric values (unquoted)
        num_vals = re.findall(r'(?<!["\w])(-?\d+(?:\.\d+)?)(?!["\w])', vals_str)

        data_vals = [v for v in str_vals if not _is_column_reference(v)]
        is_numeric = False
        if data_vals:
            values = data_vals
        elif num_vals:
            values = num_vals
            is_numeric = True
        else:
            continue

        bindings.append({
            "table": table,
            "column": column,
            "values": values,
            "span": (m.start(3), m.end(3)),
            "pattern": "in",
            "is_numeric": is_numeric,
        })

    # 3. 'Table'[Column] = "literal" (outside DEFINE MEASURE blocks)
    #    These appear in CALCULATE filters, FILTER predicates, etc.
    for m in re.finditer(
        _TABLE_COL_RE + r"""\s*=\s*"([^"]*)"\s""",
        dax_text,
    ):
        table = m.group(1)
        column = m.group(2)
        value = m.group(3)

        if _is_column_reference(value):
            continue

        bindings.append({
            "table": table,
            "column": column,
            "values": [value],
            "span": (m.start(3), m.end(3)),
            "pattern": "compare",
        })

    return bindings


def get_referenced_columns(dax_text):
    """Extract all (table, column) pairs referenced in filter bindings."""
    bindings = extract_filter_bindings(dax_text)
    cols = set()
    for b in bindings:
        cols.add((b["table"], b["column"]))
    return cols


# ---------------------------------------------------------------------------
# Value lookup — get valid replacement values from generated Delta tables
# ---------------------------------------------------------------------------

def load_column_values(spark, table_name, column_name, output_path="Tables/",
                       max_values=100):
    """Load distinct values for a column from a generated Delta table.

    Args:
        spark: Active SparkSession.
        table_name: Table name (will be sanitized for filesystem).
        column_name: Column name.
        output_path: Base path for Delta tables.
        max_values: Maximum number of distinct values to return.

    Returns:
        List of distinct values (as strings), or None if table/column not found.
    """
    from .spark_generator import _safe_table_name

    safe_name = _safe_table_name(table_name)
    table_path = f"{output_path.rstrip('/')}/{safe_name}"

    try:
        df = spark.read.format("delta").load(table_path)
        if column_name not in df.columns:
            return None
        # Get distinct values
        rows = (
            df.select(column_name)
            .distinct()
            .limit(max_values)
            .collect()
        )
        return [str(row[0]) for row in rows if row[0] is not None]
    except Exception:
        return None


def build_value_map(spark, queries, output_path="Tables/", max_values=100):
    """Build a mapping of (table, column) → [available values] from Delta tables.

    Scans all queries to find referenced columns, then loads distinct values
    from the generated Delta tables.

    Args:
        spark: Active SparkSession.
        queries: List of query dicts (from queries.json).
        output_path: Base path for Delta tables.
        max_values: Max distinct values per column.

    Returns:
        Dict mapping (table_name, column_name) → list of string values.
    """
    # Collect all referenced columns across all queries
    all_cols = set()
    for q in queries:
        text = q.get("[EventText]", "") or q.get("EventText", "")
        if text:
            all_cols.update(get_referenced_columns(text))

    print(f"  Found {len(all_cols)} distinct table/column pairs in queries", flush=True)

    # Load values for each column
    value_map = {}
    n_found = 0
    for table, column in sorted(all_cols):
        vals = load_column_values(spark, table, column, output_path, max_values)
        if vals:
            value_map[(table, column)] = vals
            n_found += 1

    print(f"  Loaded values for {n_found}/{len(all_cols)} columns", flush=True)
    return value_map


# ---------------------------------------------------------------------------
# Rewrite — substitute literal values in DAX queries
# ---------------------------------------------------------------------------

def _format_dax_value(val):
    """Format a value for DAX — quote strings, leave numbers bare."""
    try:
        float(val)
        return str(val)
    except (ValueError, TypeError):
        return f'"{val}"'


def _pick_replacement_values(original_values, available_values, rng):
    """Pick replacement values from available values.

    Tries to maintain the same count. Uses random selection.
    """
    n = len(original_values)
    if not available_values:
        return original_values  # can't replace, keep original

    if n >= len(available_values):
        return list(available_values)

    # Random sample without replacement
    indices = rng.choice(len(available_values), size=n, replace=False)
    return [available_values[i] for i in indices]


def rewrite_query(dax_text, value_map, rng, skip_tables=None):
    """Rewrite a single DAX query, replacing filter literals with generated values.

    Args:
        dax_text: Original DAX query string.
        value_map: Dict mapping (table, column) → [available values].
        rng: numpy random generator for reproducible selection.
        skip_tables: Set of table names to leave untouched (e.g. enter-data tables).

    Returns:
        (rewritten_text, n_replacements) tuple.
    """
    bindings = extract_filter_bindings(dax_text)
    if not bindings:
        return dax_text, 0

    # Process bindings in reverse order (so span offsets remain valid)
    bindings.sort(key=lambda b: b["span"][0], reverse=True)

    result = dax_text
    n_replaced = 0

    for binding in bindings:
        # Skip enter-data tables — their values are original
        if skip_tables and binding["table"] in skip_tables:
            continue

        key = (binding["table"], binding["column"])
        if key not in value_map:
            continue

        available = value_map[key]
        replacements = _pick_replacement_values(binding["values"], available, rng)

        if binding["pattern"] == "treatas":
            new_vals = ", ".join(_format_dax_value(v) for v in replacements)
            start, end = binding["span"]
            result = result[:start] + new_vals + result[end:]
            n_replaced += 1

        elif binding["pattern"] == "in":
            if binding.get("is_numeric"):
                new_vals = ", ".join(str(v) for v in replacements)
            else:
                new_vals = ", ".join(_format_dax_value(v) for v in replacements)
            start, end = binding["span"]
            result = result[:start] + new_vals + result[end:]
            n_replaced += 1

        elif binding["pattern"] == "compare":
            # Single value replacement
            if replacements:
                start, end = binding["span"]
                result = result[:start] + replacements[0] + result[end:]
                n_replaced += 1

    return result, n_replaced


def rewrite_queries(queries, value_map, seed=42, skip_tables=None):
    """Rewrite all queries in a list, replacing filter literals.

    Args:
        queries: List of query dicts (from queries.json).
        value_map: Dict mapping (table, column) → [available values].
        seed: Random seed for reproducible value selection.
        skip_tables: Set of table names to leave untouched (enter-data tables).

    Returns:
        List of rewritten query dicts (original dicts are not modified).
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    rewritten = []
    total_replacements = 0

    for q in queries:
        new_q = dict(q)
        text_key = "[EventText]" if "[EventText]" in q else "EventText"
        text = q.get(text_key, "")

        if text:
            new_text, n = rewrite_query(text, value_map, rng, skip_tables=skip_tables)
            new_q[text_key] = new_text
            total_replacements += n

        rewritten.append(new_q)

    print(f"  Rewrote {total_replacements} filter binding(s) across {len(queries)} queries",
          flush=True)
    return rewritten


# ---------------------------------------------------------------------------
# Query-informed value extraction
# ---------------------------------------------------------------------------

def extract_query_values(config, queries_path, max_cardinality=20):
    """Extract original filter values from captured queries and apply them
    to low-cardinality columns in the generation config.

    For columns with cardinality ≤ max_cardinality that appear in query
    filter bindings, replace the generated values with the original values
    from the queries. This ensures filter predicates match real data.

    Args:
        config: GenerationConfig dataclass.
        queries_path: Path to queries.json file.
        max_cardinality: Only apply to columns with this many or fewer distinct values.

    Returns:
        Number of columns updated.
    """
    # Load queries
    with open(queries_path, "r", encoding="utf-8") as f:
        queries = json.load(f)

    # Collect all filter values per (table, column) from queries
    query_values = defaultdict(set)
    for q in queries:
        text = q.get("[EventText]", "") or q.get("EventText", "")
        if not text:
            continue
        for b in extract_filter_bindings(text):
            key = (b["table"], b["column"])
            for v in b["values"]:
                if not _is_column_reference(v):
                    query_values[key].add(v)

    # Apply to config columns with low cardinality
    tables = config.tables if hasattr(config, "tables") else config.get("tables", [])
    n_updated = 0

    for tc in tables:
        tname = tc.name if hasattr(tc, "name") else tc["name"]
        cols = tc.columns if hasattr(tc, "columns") else tc.get("columns", [])

        for col in cols:
            cname = col.name if hasattr(col, "name") else col["name"]
            card = col.cardinality if hasattr(col, "cardinality") else col.get("cardinality", 0)
            dtype = col.data_type if hasattr(col, "data_type") else col.get("data_type", "string")

            # Skip if already has fixed values or cardinality too high
            existing_values = col.values if hasattr(col, "values") else col.get("values")
            if existing_values:
                continue
            if card > max_cardinality:
                continue
            if dtype != "string":
                continue

            key = (tname, cname)
            if key not in query_values:
                continue

            vals = sorted(query_values[key])
            if not vals:
                continue

            # Apply the query values as fixed values
            if hasattr(col, "values"):
                col.values = vals
                col.cardinality = len(vals)
            else:
                col["values"] = vals
                col["cardinality"] = len(vals)

            n_updated += 1

    return n_updated


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def rewrite_queries_file(spark, input_path, output_path=None,
                         tables_path="Tables/", seed=42, skip_tables=None):
    """Rewrite a queries.json file, replacing filter values with generated data.

    Args:
        spark: Active SparkSession.
        input_path: Path to the input queries.json file.
        output_path: Path for the output file (default: overwrites input).
        tables_path: Base path for generated Delta tables.
        seed: Random seed.
        skip_tables: Set of table names to leave untouched (enter-data tables).

    Returns:
        Number of replacements made.
    """
    import numpy as np

    if output_path is None:
        output_path = input_path

    print(f"Loading queries from {input_path} ...", flush=True)
    with open(input_path, "r", encoding="utf-8") as f:
        queries = json.load(f)
    print(f"  {len(queries)} queries loaded", flush=True)

    print("Building value map from Delta tables ...", flush=True)
    value_map = build_value_map(spark, queries, tables_path)

    if skip_tables:
        print(f"  Skipping {len(skip_tables)} enter-data table(s)", flush=True)

    print("Rewriting queries ...", flush=True)
    rewritten = rewrite_queries(queries, value_map, seed, skip_tables=skip_tables)

    print(f"Saving to {output_path} ...", flush=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rewritten, f, indent=2, ensure_ascii=False)

    print(f"✓ Done", flush=True)
    return sum(1 for q in rewritten if q != queries)
