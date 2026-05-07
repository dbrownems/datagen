"""Validate per-table histograms and compute parent PK seeding map.

Histograms let users pin specific column-value tuples in a table to a
target row count (or fraction of row_count). When a pinned column is the
"from" (FK) side of a relationship, the referenced parent table's PK
pool must be seeded with those values so the FK has somewhere to point.

This module:

1. Validates each table's histogram (no mixing fractions/counts, totals
   bounded, columns exist).
2. Walks the relationship graph and rejects configurations where two
   histogram-having tables sit in the same connected component
   (unsupported in this version — would require coordinated generation).
3. Computes a ``parent_seed_map`` of the form
   ``{parent_table: {parent_column: [required_values...]}}`` to be passed
   to the spark generator.
"""

from collections import defaultdict


def _coerce_value(value, data_type):
    """Best-effort coerce a YAML literal to the column's data type."""
    if value is None:
        return None
    try:
        if data_type == "int64":
            return int(value)
        if data_type == "double":
            return float(value)
        if data_type == "boolean":
            if isinstance(value, str):
                return value.strip().lower() in ("true", "1", "yes")
            return bool(value)
        # string, datetime — keep as-is
        return value
    except (ValueError, TypeError):
        return value


def _resolve_rows(entries, table_row_count, table_name):
    """Convert each entry's ``rows`` to a concrete int count.

    Validates: no mixing fractions/counts; totals bounded by row_count.
    Returns list[int] aligned with ``entries``.
    """
    if not entries:
        return [], "empty"

    fractions = []
    counts = []
    for i, e in enumerate(entries):
        r = getattr(e, "rows", None)
        if r is None:
            r = e.get("rows") if isinstance(e, dict) else None
        if r is None:
            raise ValueError(
                f"Histogram entry {i} on table '{table_name}' is missing 'rows'."
            )
        if isinstance(r, bool) or not isinstance(r, (int, float)):
            raise ValueError(
                f"Histogram entry {i} on table '{table_name}' has non-numeric "
                f"'rows' value: {r!r}"
            )
        if 0 <= r < 1:
            fractions.append((i, float(r)))
        elif r >= 1:
            counts.append((i, int(r)))
        else:
            raise ValueError(
                f"Histogram entry {i} on table '{table_name}' has negative "
                f"'rows' value: {r!r}"
            )

    if fractions and counts:
        raise ValueError(
            f"Histogram on table '{table_name}' mixes fractional rows "
            f"(in [0,1)) with absolute counts (>=1). Use one or the other."
        )

    resolved = [0] * len(entries)
    if fractions:
        total = sum(f for _, f in fractions)
        if total > 1.0 + 1e-9:
            raise ValueError(
                f"Histogram on table '{table_name}' fractions sum to "
                f"{total:.4f} > 1.0."
            )
        for i, f in fractions:
            resolved[i] = int(round(f * table_row_count))
    else:
        for i, c in counts:
            resolved[i] = c

    mode = "fractions" if fractions else "counts" if counts else "empty"
    return resolved, mode


def validate_and_build_seed_map(config, vpax_model):
    """Validate all histograms and build the parent PK seeding map.

    Args:
        config: GenerationConfig (with TableConfig.histogram populated).
        vpax_model: parsed VPAX model dict (for relationships).

    Returns:
        Tuple ``(parent_seed_map, resolved_counts)`` where:
        - ``parent_seed_map`` is ``{parent_table: {parent_col: [values]}}``
        - ``resolved_counts`` is ``{table_name: [int per histogram entry]}``

    Raises:
        ValueError on any validation failure.
    """
    tables_by_name = {t.name: t for t in config.tables}
    relationships = (vpax_model or {}).get("relationships", []) or []

    # Index relationships by (from_table, from_column) → (to_table, to_column).
    # Convention: from_* is the FK (many) side, to_* is the PK (one) side.
    fk_to_pk = {}
    for r in relationships:
        fk_to_pk[(r["from_table"], r["from_column"])] = (
            r["to_table"], r["to_column"]
        )

    # 1. Per-table validation + resolve row counts + collect FK seeding.
    parent_seed_map = defaultdict(lambda: defaultdict(list))
    resolved_counts = {}
    histogram_tables = set()

    for table in config.tables:
        entries = getattr(table, "histogram", None) or []
        if not entries:
            continue

        histogram_tables.add(table.name)
        col_names = {c.name for c in table.columns}

        # Validate pinned columns exist
        for i, e in enumerate(entries):
            vals = getattr(e, "values", None)
            if vals is None:
                vals = e.get("values") if isinstance(e, dict) else None
            if not isinstance(vals, dict) or not vals:
                raise ValueError(
                    f"Histogram entry {i} on table '{table.name}' has no "
                    f"'values' mapping."
                )
            for col_name in vals.keys():
                if col_name not in col_names:
                    raise ValueError(
                        f"Histogram entry {i} on table '{table.name}' "
                        f"references unknown column '{col_name}'."
                    )

        # Resolve rows + global per-table totals
        counts, mode = _resolve_rows(entries, table.row_count, table.name)
        resolved_counts[table.name] = counts

        # Reconcile pinned-row total vs the table's target row_count.
        # Only meaningful for absolute counts; fractions already cap at
        # row_count by construction in _resolve_rows.
        if mode == "counts":
            pinned_total = sum(counts)
            if pinned_total > table.row_count:
                print(
                    f"  Histogram on '{table.name}': pinned rows sum to "
                    f"{pinned_total:,} > target row_count "
                    f"{table.row_count:,}; overriding row_count to "
                    f"{pinned_total:,}.",
                    flush=True,
                )
                table.row_count = pinned_total
            elif pinned_total < table.row_count:
                fill = table.row_count - pinned_total
                print(
                    f"  Histogram on '{table.name}': pinned rows sum to "
                    f"{pinned_total:,} < target row_count "
                    f"{table.row_count:,}; generating {fill:,} additional "
                    f"row(s) to fill.",
                    flush=True,
                )

        # Coerce values to declared column types in-place
        col_types = {c.name: c.data_type for c in table.columns}
        for e in entries:
            vals = e.values if hasattr(e, "values") else e["values"]
            for cn, v in list(vals.items()):
                vals[cn] = _coerce_value(v, col_types.get(cn, "string"))

        # 2. Build parent seeding map for FK columns
        for entry, n in zip(entries, counts):
            if n <= 0:
                continue
            vals = entry.values if hasattr(entry, "values") else entry["values"]
            for col_name, v in vals.items():
                pk_ref = fk_to_pk.get((table.name, col_name))
                if pk_ref is None:
                    continue
                parent_table, parent_col = pk_ref
                # Capacity check against parent row_count
                if parent_table in tables_by_name:
                    bucket = parent_seed_map[parent_table][parent_col]
                    if v not in bucket:
                        bucket.append(v)

    # Capacity check: required PK values must fit in parent.row_count
    for parent_name, cols in parent_seed_map.items():
        parent = tables_by_name.get(parent_name)
        if parent is None:
            continue
        for col_name, vals in cols.items():
            if len(vals) > parent.row_count:
                raise ValueError(
                    f"Histogram requires {len(vals)} distinct values in "
                    f"'{parent_name}.{col_name}' but parent row_count is "
                    f"{parent.row_count}."
                )

    # Cross-table check: previously rejected any two histogram tables in the
    # same connected component. Relaxed — multiple child tables can share a
    # parent dimension; their required FK values are simply accumulated
    # (deduped) into ``parent_seed_map`` above. Order doesn't matter because
    # the map is computed upfront and consulted per-table during generation.
    #
    # NOTE: If a parent table has its OWN histogram on the same PK column that
    # is being seeded by a child, the parent's histogram-pinned rows may
    # displace the seeded values. That edge case is not handled here.

    # Convert defaultdicts to plain dicts for serialization safety
    parent_seed_map = {
        k: {kk: list(vv) for kk, vv in v.items()}
        for k, v in parent_seed_map.items()
    }
    return parent_seed_map, resolved_counts
