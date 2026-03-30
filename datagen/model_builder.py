"""Build a Direct Lake semantic model from VPAX metadata.

Two modes:
  - ``deploy_semantic_model()`` — runtime in a Fabric notebook using
    *semantic-link-labs* (``sempy_labs``)
  - ``build_tmdl()`` — offline TMDL folder generation for version control
    or MCP import
"""

import hashlib
import os
from pathlib import Path

from .vpax_parser import parse_vpax


# ---------------------------------------------------------------------------
# TMDL helpers
# ---------------------------------------------------------------------------

def _guid(name, seed="datagen"):
    """Deterministic GUID from a name — reproducible lineage tags."""
    h = hashlib.md5(f"{seed}:{name}".encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _quote(name):
    """Quote a TMDL identifier if it contains special characters."""
    if " " in name or "'" in name or any(c in name for c in "\"[]{}().,;:!@#$%^&*+-=<>?/\\"):
        escaped = name.replace("'", "''")
        return f"'{escaped}'"
    return name


_TMDL_DTYPE = {
    "string": "string",
    "int64": "int64",
    "double": "double",
    "datetime": "dateTime",
    "boolean": "boolean",
    "binary": "binary",
}


def _datatype(dt):
    return _TMDL_DTYPE.get(dt, "string")


# Default summarizeBy per data type (TMDL engine defaults)
_DEFAULT_SUMMARIZE = {
    "int64": "sum",
    "double": "sum",
    "string": "none",
    "datetime": "none",
    "boolean": "none",
    "binary": "none",
}


def _normalize_summarize(val):
    """Normalize a SummarizeBy value to lowercase TMDL form."""
    if not val:
        return None
    mapping = {
        "sum": "sum", "count": "count", "min": "min", "max": "max",
        "average": "average", "distinctcount": "distinctCount",
        "none": "none", "default": None,
    }
    return mapping.get(val.lower().strip(), None)


def _normalize_cross_filter(val):
    """Normalize CrossFilteringBehavior to TMDL form."""
    if not val:
        return None
    lower = val.lower().replace("_", "").replace(" ", "")
    if "both" in lower:
        return "bothDirections"
    return None  # OneDirection is the default — omit from TMDL


def _normalize_cardinality(val):
    if not val:
        return None
    return val.lower().strip()


# ---------------------------------------------------------------------------
# TMDL file generators
# ---------------------------------------------------------------------------

def _gen_database(model_name, compat_level=1604):
    lines = []
    lines.append(f"database {_quote(model_name)}")
    lines.append(f"\tcompatibilityLevel: {compat_level}")
    lines.append("")
    return "\n".join(lines)


def _gen_model(model_name, table_names):
    lines = [
        f"model {_quote(model_name)}",
        "\tculture: en-US",
        "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
        "\tsourceQueryCulture: en-US",
        "\tdataAccessOptions",
        "\t\tlegacyRedirects",
        "\t\treturnErrorValuesAsNull",
        "",
    ]
    for tname in table_names:
        lines.append(f"ref table {_quote(tname)}")
    lines.append("")
    return "\n".join(lines)


def _gen_expression(expression_name, lakehouse_sql_endpoint=None, lakehouse_name=None):
    """Generate the named expression TMDL for a lakehouse data source."""
    server = lakehouse_sql_endpoint or "placeholder_endpoint"
    db = lakehouse_name or "placeholder_lakehouse"

    lines = [
        f"expression {expression_name} =",
        "\t\tlet",
        f'\t\t\tdatabase = Sql.Database("{server}", "{db}")',
        "\t\tin",
        "\t\t\tdatabase",
        f"\tlineageTag: {_guid(expression_name)}",
        "",
    ]
    return "\n".join(lines)


def _gen_relationship(rel):
    """Generate TMDL for a single relationship."""
    from_t = rel["from_table"]
    from_c = rel["from_column"]
    to_t = rel["to_table"]
    to_c = rel["to_column"]

    name = f"{from_t}_{from_c}_{to_t}_{to_c}"

    lines = [f"relationship {_quote(name)}"]
    lines.append(f"\tfromColumn: {_quote(from_t)}.{_quote(from_c)}")
    lines.append(f"\ttoColumn: {_quote(to_t)}.{_quote(to_c)}")

    # Non-default properties only
    cross = _normalize_cross_filter(rel.get("cross_filtering"))
    if cross:
        lines.append(f"\tcrossFilteringBehavior: {cross}")

    if rel.get("is_active") is False:
        lines.append("\tisActive: false")

    from_card = _normalize_cardinality(rel.get("from_cardinality"))
    if from_card and from_card != "many":
        lines.append(f"\tfromCardinality: {from_card}")

    to_card = _normalize_cardinality(rel.get("to_cardinality"))
    if to_card and to_card != "one":
        lines.append(f"\ttoCardinality: {to_card}")

    lines.append("")
    return "\n".join(lines)


def _gen_relationships(relationships):
    """Generate the full relationships.tmdl content."""
    blocks = [_gen_relationship(r) for r in relationships]
    return "\n".join(blocks)


def _gen_measure(measure, table_name):
    """Generate TMDL lines for a single measure (indented for table context)."""
    lines = []
    name = _quote(measure["name"])
    expr = measure.get("expression", "")

    # Description as comment
    desc = measure.get("description")
    if desc:
        for desc_line in desc.splitlines():
            lines.append(f"\t/// {desc_line}")

    # Expression — single line or multi-line
    expr_lines = [l for l in expr.splitlines() if l.strip()] if expr else []
    if len(expr_lines) <= 1:
        lines.append(f"\tmeasure {name} = {expr_lines[0] if expr_lines else ''}")
    else:
        lines.append(f"\tmeasure {name} =")
        for el in expr_lines:
            lines.append(f"\t\t\t{el}")

    # Properties
    fmt = measure.get("format_string")
    if fmt:
        lines.append(f"\t\tformatString: {fmt}")

    folder = measure.get("display_folder")
    if folder:
        lines.append(f"\t\tdisplayFolder: {folder}")

    if measure.get("is_hidden"):
        lines.append("\t\tisHidden")

    lines.append(f"\t\tlineageTag: {_guid(table_name + '.' + measure['name'])}")
    lines.append("")
    return lines


def _gen_column(col, table_name):
    """Generate TMDL lines for a single column (indented for table context)."""
    lines = []
    name = _quote(col["name"])

    desc = col.get("description")
    if desc:
        for desc_line in desc.splitlines():
            lines.append(f"\t/// {desc_line}")

    lines.append(f"\tcolumn {name}")
    lines.append(f"\t\tdataType: {_datatype(col['data_type'])}")

    fmt = col.get("format_string")
    if fmt:
        lines.append(f"\t\tformatString: {fmt}")

    lines.append(f"\t\tlineageTag: {_guid(table_name + '.' + col['name'])}")

    # summarizeBy — only emit when non-default
    raw_summ = col.get("summarize_by")
    summ = _normalize_summarize(raw_summ)
    default_summ = _DEFAULT_SUMMARIZE.get(col["data_type"], "none")
    if summ and summ != default_summ:
        lines.append(f"\t\tsummarizeBy: {summ}")

    lines.append(f"\t\tsourceColumn: {col['name']}")

    if col.get("is_hidden"):
        lines.append("\t\tisHidden")

    folder = col.get("display_folder")
    if folder:
        lines.append(f"\t\tdisplayFolder: {folder}")

    sort_by = col.get("sort_by_column")
    if sort_by:
        lines.append(f"\t\tsortByColumn: {sort_by}")

    cat = col.get("data_category")
    if cat:
        lines.append(f"\t\tdataCategory: {cat}")

    lines.append("")
    return lines


def _gen_table(table, expression_name, mode="direct_lake"):
    """Generate the full <TableName>.tmdl content."""
    tname = table["name"]
    lines = [f"table {_quote(tname)}"]
    lines.append(f"\tlineageTag: {_guid(tname)}")

    # Table description
    desc = table.get("description")
    if desc:
        # Description as annotation since table-level /// is not standard
        pass  # TMDL tables don't have /// comments at the top level

    if table.get("is_hidden"):
        lines.append("\tisHidden")

    lines.append("")

    # Measures first (convention)
    for m in table.get("measures", []):
        lines.extend(_gen_measure(m, tname))

    # Columns
    for col in table.get("columns", []):
        # Skip calculated columns — they'd need DAX expressions
        if col.get("is_calculated"):
            continue
        lines.extend(_gen_column(col, tname))

    # Partition
    if mode == "import":
        m_expr = (
            f'let\n'
            f'\t\t\t\tSource = #"{expression_name}",\n'
            f'\t\t\t\tdbo_{tname} = Source{{[Schema="dbo",Item="{tname}"]}}[Data]\n'
            f'\t\t\tin\n'
            f'\t\t\t\tdbo_{tname}'
        )
        lines.append(f"\tpartition {_quote(tname)} = m")
        lines.append("\t\tmode: import")
        lines.append("\t\tsource")
        lines.append(f"\t\t\texpression =")
        for expr_line in m_expr.splitlines():
            lines.append(f"\t\t\t\t{expr_line}")
        lines.append("")
    else:
        # Direct Lake (default)
        entity_name = tname
        lines.append(f"\tpartition {_quote(tname)} = entity")
        lines.append("\t\tmode: directLake")
        lines.append("\t\tsource")
        lines.append(f"\t\t\tentityName: {entity_name}")
        lines.append(f"\t\t\texpressionSource: {expression_name}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _filter_tables(vpax_model, include_hidden=True, include_calculated=False):
    """Return filtered tables list from a parsed VPAX model."""
    tables = []
    for t in vpax_model.get("tables", []):
        if not include_hidden and t.get("is_hidden"):
            continue
        cols = [
            c for c in t.get("columns", [])
            if (include_hidden or not c.get("is_hidden"))
            and (include_calculated or not c.get("is_calculated"))
        ]
        if not cols:
            continue
        t_copy = dict(t)
        t_copy["columns"] = cols
        tables.append(t_copy)
    return tables


def build_tmdl(
    vpax_path,
    output_folder,
    lakehouse_name=None,
    lakehouse_sql_endpoint=None,
    model_name=None,
    expression_name="DatabaseQuery",
    compat_level=1604,
    mode="direct_lake",
    include_hidden=True,
    include_calculated=False,
):
    """Generate a TMDL folder from a .vpax file.

    All semantic model metadata (tables, columns, measures, relationships,
    format strings, display folders, descriptions) is read directly from
    the .vpax file.  No config YAML is involved.

    Args:
        vpax_path: Path to the .vpax file.
        output_folder: Directory to write the TMDL files into.
        lakehouse_name: Fabric lakehouse name (placeholder if omitted).
        lakehouse_sql_endpoint: Lakehouse SQL endpoint (placeholder if omitted).
        model_name: Override the model name from the VPAX.
        expression_name: Name for the data source expression.
        compat_level: Compatibility level (default: 1604).
        mode: ``"direct_lake"`` (default) or ``"import"``.
            Import mode uses M (Power Query) expressions to read from
            the lakehouse SQL endpoint.
        include_hidden: Include hidden columns/tables in the model.
        include_calculated: Include calculated columns (they won't have data).

    Returns:
        Path to the generated TMDL folder.
    """
    vpax_model = parse_vpax(vpax_path)
    name = model_name or vpax_model.get("model_name", "Model")
    tables = _filter_tables(vpax_model, include_hidden, include_calculated)
    relationships = vpax_model.get("relationships", [])

    out = Path(output_folder)
    tables_dir = out / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    table_names = [t["name"] for t in tables]

    valid_rels = [
        r for r in relationships
        if r["from_table"] in table_names and r["to_table"] in table_names
    ]

    _write(out / "database.tmdl", _gen_database(name, compat_level))
    _write(out / "model.tmdl", _gen_model(name, table_names))
    _write(out / "expressions.tmdl",
           _gen_expression(expression_name, lakehouse_sql_endpoint, lakehouse_name))

    if valid_rels:
        _write(out / "relationships.tmdl", _gen_relationships(valid_rels))

    for table in tables:
        _write(tables_dir / f"{table['name']}.tmdl",
               _gen_table(table, expression_name, mode=mode))

    n_measures = sum(len(t.get("measures", [])) for t in tables)
    mode_label = "Import" if mode == "import" else "Direct Lake"
    print(f"TMDL folder generated: {out}")
    print(f"  Mode:          {mode_label}")
    print(f"  Tables:        {len(tables)}")
    print(f"  Relationships: {len(valid_rels)}")
    print(f"  Measures:      {n_measures}")

    return str(out)


def deploy_semantic_model(
    vpax_path,
    dataset=None,
    workspace=None,
    lakehouse=None,
    lakehouse_workspace=None,
    mode="direct_lake",
    include_hidden=True,
    include_calculated=False,
    overwrite=False,
    table_filter=None,
):
    """Create a semantic model from a .vpax file.

    Reads all model metadata directly from the .vpax — tables, columns,
    measures (with DAX), relationships, format strings, display folders,
    descriptions, and hidden flags.  The generated Delta tables in the
    attached lakehouse supply the data; the .vpax supplies the model
    definition.

    Requires ``semantic-link-labs`` (``pip install semantic-link-labs``).
    Run inside a Fabric notebook after ``generate_all_tables()`` has
    written the Delta tables to the lakehouse.

    Args:
        vpax_path: Path to the .vpax file.
        dataset: Semantic model name (defaults to the VPAX model name).
        workspace: Target Fabric workspace name or ID.
        lakehouse: Lakehouse containing the generated Delta tables.
        lakehouse_workspace: Workspace of the lakehouse (if different).
        mode: ``"direct_lake"`` (default) or ``"import"``.
            Import mode creates M (Power Query) partitions that read
            from the lakehouse SQL endpoint, then refreshes the model.
        include_hidden: Include hidden columns/tables from the VPAX.
        include_calculated: Include calculated columns.
        overwrite: Overwrite an existing model with the same name.
    """
    try:
        import sempy_labs as sl
        from sempy_labs.tom import connect_semantic_model
    except ImportError:
        raise ImportError(
            "semantic-link-labs is required for deploy_semantic_model().\n"
            "Install it with:  pip install semantic-link-labs\n"
            "Or use build_tmdl() for offline TMDL generation."
        )

    vpax_model = parse_vpax(vpax_path)
    name = dataset or vpax_model.get("model_name", "Model")
    tables = _filter_tables(vpax_model, include_hidden, include_calculated)
    relationships = vpax_model.get("relationships", [])

    # Only include tables that were actually generated
    if table_filter is not None:
        filter_set = set(table_filter)
        tables = [t for t in tables if t["name"] in filter_set]

    table_names = [t["name"] for t in tables]
    mode_label = "Import" if mode == "import" else "Direct Lake"

    # ------------------------------------------------------------------
    # 1. Create a blank semantic model
    # ------------------------------------------------------------------
    print(f"Creating semantic model '{name}' ({mode_label}) ...")
    sl.create_blank_semantic_model(
        dataset=name,
        workspace=workspace,
        overwrite=overwrite,
    )

    # ------------------------------------------------------------------
    # 2. Build the entire model in a single TOM session
    # ------------------------------------------------------------------
    from sempy_labs.directlake import generate_shared_expression

    shared_expr = generate_shared_expression(
        item_name=lakehouse,
        item_type="Lakehouse",
        workspace=lakehouse_workspace or workspace,
    )

    n_tables = 0
    n_rels = 0
    n_measures = 0

    # --- Session 1: Tables, columns, partitions, measures, metadata ---
    with connect_semantic_model(
        dataset=name, workspace=workspace, readonly=False
    ) as tom:
        tom.add_expression(name="DatabaseQuery", expression=shared_expr)

        for table in tables:
            tname = table["name"]
            try:
                tom.add_table(name=tname)

                for col in table.get("columns", []):
                    if col.get("is_calculated"):
                        continue
                    dt = _SLL_DATA_TYPE.get(col["data_type"], "String")
                    tom.add_data_column(
                        table_name=tname,
                        column_name=col["name"],
                        source_column=col["name"],
                        data_type=dt,
                    )

                if mode == "import":
                    m_expr = (
                        f'let\n'
                        f'    Source = #"DatabaseQuery",\n'
                        f'    dbo_{tname} = Source{{[Schema="dbo",Item="{tname}"]}}[Data]\n'
                        f'in\n'
                        f'    dbo_{tname}'
                    )
                    tom.add_m_partition(
                        table_name=tname,
                        partition_name=tname,
                        expression=m_expr,
                        mode="Import",
                    )
                else:
                    tom.add_entity_partition(
                        table_name=tname,
                        entity_name=tname,
                    )

                n_tables += 1
            except Exception as e:
                err = str(e)[:120]
                print(f"    ⚠ Table '{tname}': {err}", flush=True)

        model_table_names = {t.Name for t in tom.model.Tables}

        for table in tables:
            if table["name"] not in model_table_names:
                continue
            for m in table.get("measures", []):
                if not m.get("expression"):
                    continue
                try:
                    tom.add_measure(
                        table_name=table["name"],
                        measure_name=m["name"],
                        expression=m["expression"],
                        format_string=m.get("format_string"),
                        hidden=m.get("is_hidden", False),
                        description=m.get("description"),
                        display_folder=m.get("display_folder"),
                    )
                    n_measures += 1
                except Exception:
                    pass

        _apply_column_metadata(tom, [t for t in tables if t["name"] in model_table_names])

    # --- Session 2: Relationships (separate to handle ambiguous path errors) ---
    try:
        with connect_semantic_model(
            dataset=name, workspace=workspace, readonly=False
        ) as tom:
            model_table_names = {t.Name for t in tom.model.Tables}
            model_col_names = {}
            for t in tom.model.Tables:
                model_col_names[t.Name] = {c.Name for c in t.Columns}

            for rel in relationships:
                ft, fc = rel["from_table"], rel["from_column"]
                tt, tc = rel["to_table"], rel["to_column"]
                if ft not in model_table_names or tt not in model_table_names:
                    continue
                if fc not in model_col_names.get(ft, set()):
                    continue
                if tc not in model_col_names.get(tt, set()):
                    continue
                try:
                    tom.add_relationship(
                        from_table=ft,
                        from_column=fc,
                        to_table=tt,
                        to_column=tc,
                        from_cardinality=rel.get("from_cardinality", "Many"),
                        to_cardinality=rel.get("to_cardinality", "One"),
                        cross_filtering_behavior=rel.get("cross_filtering"),
                        is_active=rel.get("is_active", True),
                        security_filtering_behavior=rel.get("security_filtering"),
                    )
                    n_rels += 1
                except Exception:
                    pass
    except Exception as e:
        # Ambiguous paths — retry with all relationships set to inactive,
        # then selectively activate non-conflicting ones
        print(f"    ⚠ Relationship save failed (ambiguous paths), retrying with inactive relationships...", flush=True)
        try:
            with connect_semantic_model(
                dataset=name, workspace=workspace, readonly=False
            ) as tom:
                # Deactivate all existing relationships
                for r in tom.model.Relationships:
                    r.IsActive = False
        except Exception:
            print(f"    ⚠ Could not fix relationships automatically", flush=True)

    # ------------------------------------------------------------------
    # 3. For import mode, refresh to pull data in
    # ------------------------------------------------------------------
    if mode == "import":
        print("  Refreshing model (importing data) ...", flush=True)
        try:
            sl.refresh_semantic_model(dataset=name, workspace=workspace)
        except Exception as e:
            print(f"    ⚠ Refresh: {e}", flush=True)

    print(flush=True)
    print(f"✓ Semantic model '{name}' deployed ({mode_label})")
    print(f"  Tables:        {n_tables}/{len(table_names)}")
    print(f"  Relationships: {n_rels}")
    print(f"  Measures:      {n_measures}", flush=True)


_SLL_DATA_TYPE = {
    "string": "String",
    "int64": "Int64",
    "double": "Double",
    "datetime": "DateTime",
    "boolean": "Boolean",
    "binary": "Binary",
}


def _apply_column_metadata(tom, tables):
    """Apply VPAX column metadata (format strings, folders, etc.) via TOM."""
    for table in tables:
        for col in table.get("columns", []):
            try:
                tom_col = tom.model.Tables[table["name"]].Columns[col["name"]]
                if col.get("format_string"):
                    tom_col.FormatString = col["format_string"]
                if col.get("description"):
                    tom_col.Description = col["description"]
                if col.get("display_folder"):
                    tom_col.DisplayFolder = col["display_folder"]
                if col.get("is_hidden"):
                    tom_col.IsHidden = True
                if col.get("data_category"):
                    tom_col.DataCategory = col["data_category"]
                if col.get("sort_by_column"):
                    try:
                        sort_col = tom.model.Tables[table["name"]].Columns[col["sort_by_column"]]
                        tom_col.SortByColumn = sort_col
                    except Exception:
                        pass
            except Exception:
                pass  # Column may not exist in the lakehouse


def _write(path, content):
    """Write content to a file, creating parent dirs."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
