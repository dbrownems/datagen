"""Build a Direct Lake semantic model from the Model.bim in a VPAX file.

Extracts the full model definition (model.bim) from the VPAX, modifies
table partitions to point at the generated OneLake Delta tables, and
deploys via the Fabric REST API.  This preserves the entire original
model — all relationships, measures, hierarchies, roles, column metadata,
format strings, display folders, etc.
"""

import base64
import json
import zipfile


def _safe_folder_name(name):
    """Sanitize a table name for use as a Delta table folder name."""
    for ch in "/\\:*?\"<>|":
        name = name.replace(ch, "_")
    return name


def _extract_bim(vpax_path):
    """Extract Model.bim JSON from a VPAX file."""
    import io

    try:
        vpax_bytes = open(vpax_path, "rb").read()
    except OSError:
        # FUSE mount may be disconnected — try notebookutils
        try:
            import notebookutils
            vpax_bytes = notebookutils.fs.read(vpax_path)
            if isinstance(vpax_bytes, str):
                vpax_bytes = vpax_bytes.encode("latin-1")
        except Exception:
            raise OSError(
                f"Cannot read {vpax_path}. The lakehouse FUSE mount may be disconnected. "
                f"Try restarting the notebook session."
            )

    with zipfile.ZipFile(io.BytesIO(vpax_bytes), "r") as zf:
        names = zf.namelist()
        bim_name = next((n for n in names if n.lower() == "model.bim"), None)
        if bim_name:
            return json.loads(zf.read(bim_name))
        raise ValueError(f"No Model.bim found in {vpax_path}")


def _get_lakehouse_info(lakehouse=None, workspace=None):
    """Get lakehouse details (IDs, OneLake host) from notebookutils."""
    import notebookutils

    if not lakehouse:
        lakehouse = notebookutils.runtime.context.get("defaultLakehouseName")
        if not lakehouse:
            raise RuntimeError(
                "No lakehouse specified and no default lakehouse attached. "
                "Attach a lakehouse to the notebook or pass lakehouse='name'."
            )

    info = notebookutils.lakehouse.get(lakehouse)

    props = info.get("properties", {})
    ws_id = info["workspaceId"]
    lh_id = info["id"]
    lh_name = info.get("displayName", lakehouse or "lakehouse")

    # Extract OneLake host (handles msit vs prod). Order:
    #   1. oneLakeTablesPath property on the lakehouse
    #   2. Fabric workspace API → oneLakeEndpoints.dfsEndpoint
    #   3. Default to public prod endpoint
    onelake_host = None
    tables_url = props.get("oneLakeTablesPath", "")
    if tables_url:
        from urllib.parse import urlparse
        onelake_host = urlparse(tables_url).hostname
    if not onelake_host:
        try:
            ws = notebookutils.workspace.get(ws_id) if hasattr(notebookutils, "workspace") else None
            if ws:
                ep = (ws.get("oneLakeEndpoints") or {}).get("dfsEndpoint", "")
                if ep:
                    from urllib.parse import urlparse
                    onelake_host = urlparse(ep).hostname
        except Exception:
            pass
    if not onelake_host:
        try:
            import sempy.fabric as _fabric
            client = _fabric.FabricRestClient()
            r = client.get(f"v1/workspaces/{ws_id}")
            if r.status_code == 200:
                ep = (r.json().get("oneLakeEndpoints") or {}).get("dfsEndpoint", "")
                if ep:
                    from urllib.parse import urlparse
                    onelake_host = urlparse(ep).hostname
        except Exception:
            pass
    if not onelake_host:
        onelake_host = "onelake.dfs.fabric.microsoft.com"

    # Detect schema-enabled lakehouses by probing OneLake DFS for Tables/<schema>/.
    # notebookutils.lakehouse.get() may return defaultSchema="dbo" for schema-less
    # lakehouses (false positive), or omit it entirely for schema-enabled
    # lakehouses (false negative). So always probe directly: try the property,
    # then fall back to "dbo" (the conventional default).
    candidate_schemas = []
    prop_schema = props.get("defaultSchema") or None
    if prop_schema:
        candidate_schemas.append(prop_schema)
    if "dbo" not in candidate_schemas:
        candidate_schemas.append("dbo")

    default_schema = None
    try:
        import requests as _rq
        tok = notebookutils.credentials.getToken("storage")
        for cand in candidate_schemas:
            url = (
                f"https://{onelake_host}/{ws_id}"
                f"?resource=filesystem&recursive=false"
                f"&directory={lh_id}/Tables/{cand}&maxResults=1"
            )
            r = _rq.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=15)
            if r.status_code in (200, 206):
                default_schema = cand
                break
    except Exception:
        # If the probe fails entirely, trust the property if present
        default_schema = prop_schema

    return {
        "ws_id": ws_id,
        "lh_id": lh_id,
        "lh_name": lh_name,
        "onelake_host": onelake_host,
        "default_schema": default_schema,
    }


def _add_missing_bim_columns(config, bim):
    """Add columns from Model.bim that are missing from the generation config."""
    from .config import ColumnConfig, DistributionConfig

    _SKIP_TYPES = {"rowNumber"}
    _BIM_TO_CONFIG_TYPE = {
        "string": "string", "int64": "int64", "double": "double",
        "boolean": "boolean", "dateTime": "datetime", "decimal": "double",
    }

    bim_model = bim.get("model", bim)

    tables_list = config.tables if hasattr(config, "tables") else config.get("tables", [])
    config_cols = {}
    for tc in tables_list:
        tname = tc.name if hasattr(tc, "name") else tc["name"]
        cols = tc.columns if hasattr(tc, "columns") else tc.get("columns", [])
        config_cols[tname] = {
            (c.name if hasattr(c, "name") else c["name"]) for c in cols
        }

    n_added = 0
    for bim_table in bim_model.get("tables", []):
        tname = bim_table["name"]
        if tname not in config_cols:
            continue

        existing = config_cols[tname]
        config_table = next(
            (tc for tc in tables_list
             if (tc.name if hasattr(tc, "name") else tc["name"]) == tname),
            None,
        )
        if config_table is None:
            continue

        for bim_col in bim_table.get("columns", []):
            if bim_col.get("type") in _SKIP_TYPES:
                continue
            col_name = bim_col["name"]
            if col_name in existing:
                continue

            data_type = _BIM_TO_CONFIG_TYPE.get(
                bim_col.get("dataType", "string"), "string"
            )
            row_count = config_table.row_count if hasattr(config_table, "row_count") else config_table.get("row_count", 100)
            new_col = ColumnConfig(
                name=col_name,
                data_type=data_type,
                cardinality=min(10, row_count),
                distribution=DistributionConfig(
                    avg_length=12 if data_type == "string" else None,
                    style="docker" if data_type == "string" else None,
                ),
            )
            if hasattr(config_table, "columns"):
                config_table.columns.append(new_col)
            else:
                config_table["columns"].append(new_col)
            existing.add(col_name)
            n_added += 1

    return n_added


def _is_enter_data_table(bim_table):
    """Check if a BIM table is an 'enter data' table (pasted/inline data)."""
    for part in bim_table.get("partitions", []):
        source = part.get("source", {})
        expr = source.get("expression", "")
        if isinstance(expr, list):
            expr = "\n".join(expr)
        if "Table.FromRows" in expr and "Json.Document" in expr:
            return True
    return False


def _is_measure_only_table(bim_table):
    """Check if a BIM table has measures but no data columns."""
    has_measures = bool(bim_table.get("measures"))
    has_data_cols = any(
        c.get("type", "data") not in ("calculated", "calculatedTableColumn", "rowNumber")
        for c in bim_table.get("columns", [])
    )
    return has_measures and not has_data_cols


def _is_calculated_table(bim_table):
    """Check if a BIM table is a calculated table (DAX-defined partition)."""
    for part in bim_table.get("partitions", []):
        if part.get("source", {}).get("type") == "calculated":
            return True
    return False


def get_tables_to_skip(vpax_path, mode="import"):
    """Return set of table names that should NOT get Delta tables generated.

    For import mode: skip measure-only tables, enter-data tables,
    and calculated tables (they keep their DAX definitions).
    For direct_lake mode: returns empty set (generate everything).
    """
    if mode == "direct_lake":
        return set()

    bim = _extract_bim(vpax_path)
    model = bim.get("model", bim)
    skip = set()
    for table in model.get("tables", []):
        tname = table["name"]
        if _is_enter_data_table(table):
            skip.add(tname)
        elif _is_measure_only_table(table):
            skip.add(tname)
        elif _is_calculated_table(table):
            skip.add(tname)
    return skip


def get_enter_data_tables(vpax_path):
    """Return set of table names that are enter-data tables.

    These tables have inline data in their M expressions and should
    not have their filter values replaced in DAX queries.
    """
    bim = _extract_bim(vpax_path)
    model = bim.get("model", bim)
    return {
        table["name"] for table in model.get("tables", [])
        if _is_enter_data_table(table)
    }


# Reverse of the BIM→config map used elsewhere. Used when we need to push
# config-side data_type overrides (e.g. from auto PK-wins alignment) back
# into the BIM so Direct Lake's relationship type check passes.
_CONFIG_TO_BIM_TYPE = {
    "string": "string", "int64": "int64", "double": "double",
    "boolean": "boolean", "datetime": "dateTime", "decimal": "decimal",
}


def _apply_config_column_types_to_bim(bim, config, *, include_calculated=False, log=print):
    """Override BIM column `dataType` values to match the (possibly mutated)
    GenerationConfig. Returns the list of (table, column, old, new) changes.

    The auto PK-wins fixer mutates `config.tables[].columns[].data_type` so
    Delta tables are written with the chosen types. This function is the
    matching half: it patches the BIM that ships to the semantic model so
    the BIM-side column types agree with the Delta types AND with each
    other across relationships.

    Calculated columns (BIM ``type=="calculated"``) are skipped by default —
    their type is normally derived from the DAX expression. Pass
    ``include_calculated=True`` when the calc column is about to be converted
    to a data column (Direct Lake) and the DAX type is no longer authoritative.
    """
    bim_model = bim.get("model", bim)
    tables_list = config.tables if hasattr(config, "tables") else config.get("tables", [])
    cfg_types = {}
    for tc in tables_list:
        tname = tc.name if hasattr(tc, "name") else tc["name"]
        cols = tc.columns if hasattr(tc, "columns") else tc.get("columns", [])
        cfg_types[tname] = {}
        for c in cols:
            cname = c.name if hasattr(c, "name") else c["name"]
            cdt = c.data_type if hasattr(c, "data_type") else c.get("data_type", "")
            cfg_types[tname][cname] = (cdt or "").lower()

    skip_types = {"rowNumber"}
    if not include_calculated:
        skip_types |= {"calculated", "calculatedTableColumn"}

    changes = []
    for bim_table in bim_model.get("tables", []):
        tname = bim_table.get("name")
        cfg_t = cfg_types.get(tname)
        if not cfg_t:
            continue
        for bim_col in bim_table.get("columns", []):
            cname = bim_col.get("name")
            if not cname:
                continue
            if bim_col.get("type") in skip_types:
                continue
            cfg_dt = cfg_t.get(cname)
            if not cfg_dt:
                continue
            new_dt = _CONFIG_TO_BIM_TYPE.get(cfg_dt)
            if not new_dt:
                continue
            old_dt = bim_col.get("dataType")
            if old_dt and old_dt != new_dt:
                bim_col["dataType"] = new_dt
                # Force re-inference of source provider type to avoid stale
                # ProviderType vs DataType mismatches that the engine rejects.
                if "sourceProviderType" in bim_col:
                    bim_col.pop("sourceProviderType", None)
                changes.append((tname, cname, old_dt, new_dt))

    if changes:
        log(f"  Patched {len(changes)} BIM column dataType(s) to match config "
            f"(needed by Direct Lake relationship type check):")
        for tname, cname, old, new in changes:
            log(f"      {tname}[{cname}]: {old}  →  {new}")
    return changes


    """Add columns from Model.bim that are missing from the generation config.

    The VPAX DaxModel.json stats may not include all columns (e.g. some
    calculated columns). The BIM is authoritative — ensure every column
    in the BIM has a corresponding column in the generation config so it
    gets generated in the Delta table.
    """
    from .config import ColumnConfig, DistributionConfig

    _CALC_TYPES = {"calculated", "calculatedTableColumn"}
    _SKIP_TYPES = {"rowNumber"}
    _BIM_TO_CONFIG_TYPE = {
        "string": "string", "int64": "int64", "double": "double",
        "boolean": "boolean", "dateTime": "datetime", "decimal": "double",
    }

    bim_model = bim.get("model", bim)

    # Build lookup: table_name → set of column names in config
    config_cols = {}
    tables_list = config.tables if hasattr(config, "tables") else config.get("tables", [])
    for tc in tables_list:
        tname = tc.name if hasattr(tc, "name") else tc["name"]
        cols = tc.columns if hasattr(tc, "columns") else tc.get("columns", [])
        config_cols[tname] = {
            (c.name if hasattr(c, "name") else c["name"])
            for c in cols
        }

    n_added = 0
    for bim_table in bim_model.get("tables", []):
        tname = bim_table["name"]
        if tname not in config_cols:
            continue

        existing = config_cols[tname]
        # Find the config table object to append to
        config_table = None
        for tc in tables_list:
            if (tc.name if hasattr(tc, "name") else tc["name"]) == tname:
                config_table = tc
                break
        if config_table is None:
            continue

        for bim_col in bim_table.get("columns", []):
            col_type = bim_col.get("type")
            if col_type in _SKIP_TYPES:
                continue
            col_name = bim_col["name"]
            if col_name in existing:
                continue

            # This column is in the BIM but not in the config — add it
            data_type = _BIM_TO_CONFIG_TYPE.get(
                bim_col.get("dataType", "string"), "string"
            )
            new_col = ColumnConfig(
                name=col_name,
                data_type=data_type,
                cardinality=min(10, config_table.row_count if hasattr(config_table, "row_count") else config_table.get("row_count", 100)),
                distribution=DistributionConfig(
                    avg_length=12 if data_type == "string" else None,
                    style="docker" if data_type == "string" else None,
                ),
            )
            if hasattr(config_table, "columns"):
                config_table.columns.append(new_col)
            else:
                config_table["columns"].append(new_col)
            existing.add(col_name)
            n_added += 1

    return n_added


def _modify_bim_for_direct_lake(bim, lh_info, table_filter=None):
    """Modify a model.bim to use Direct Lake on OneLake.

    Uses the .NET TOM (Tabular Object Model) library to properly deserialize,
    modify, and reserialize the BIM.
    """
    return _modify_bim_via_tom(bim, lh_info, table_filter)


def _modify_bim_for_import(bim, lh_info, table_filter=None, use_sql_endpoint=False):
    """Modify a model.bim for Import mode reading from OneLake Delta tables.

    Uses a single shared data source pointing at the Tables folder, with
    each table referenced by folder name. This ensures Power BI sees one
    data source for all queries.

    M query pattern:
        let
            Source = AzureStorage.DataLake("https://{host}/{ws}/{lh}/Tables/dbo/", ...),
            Table = Source{[Name="TableName"]}[Content],
            Data = DeltaLake.Table(Table)
        in
            Data
    """
    model = bim.get("model", bim)
    onelake_host = lh_info["onelake_host"]
    ws_id = lh_info["ws_id"]
    lh_id = lh_info["lh_id"]
    schema = lh_info.get("default_schema")

    # Build the shared source URL — include schema folder only if the lakehouse
    # has schemas enabled. Schema-less lakehouses store tables directly under Tables/.
    if schema:
        source_url = f"https://{onelake_host}/{ws_id}/{lh_id}/Tables/{schema}/"
    else:
        source_url = f"https://{onelake_host}/{ws_id}/{lh_id}/Tables/"

    filter_set = set(table_filter) if table_filter is not None else None

    n_rewritten = 0
    n_calc_kept = 0
    for table in model.get("tables", []):
        tname = table["name"]
        has_delta = filter_set is None or tname in filter_set

        if not has_delta:
            continue

        # Don't rewrite calculated tables — they have DAX partition expressions
        # that must be preserved in import mode
        is_calc_table = any(
            p.get("source", {}).get("type") == "calculated"
            for p in table.get("partitions", [])
        )
        if is_calc_table:
            n_calc_kept += 1
            continue

        safe_name = _safe_folder_name(tname)

        m_expr = (
            "let\n"
            f'    Source = AzureStorage.DataLake("{source_url}", [HierarchicalNavigation=true]),\n'
            f'    Table = Source{{[Name="{safe_name}"]}}[Content],\n'
            "    Data = DeltaLake.Table(Table)\n"
            "in\n"
            "    Data"
        )

        table["partitions"] = [
            {
                "name": tname,
                "source": {
                    "type": "m",
                    "expression": m_expr.split("\n"),
                },
            }
        ]
        n_rewritten += 1

    print(f"    Source: {source_url}", flush=True)
    print(f"    Rewrote {n_rewritten} partition(s), kept {n_calc_kept} calculated table(s)", flush=True)
    return bim


def _strip_unknown_bim_properties(bim, table_filter=None, convert_calc=True):
    """Clean BIM JSON for TOM deserialization and Fabric import.

    When convert_calc=True (Direct Lake): converts calculated columns to data
    columns since Direct Lake doesn't support them.
    When convert_calc=False (Import): leaves calculated columns untouched.

    Strips unknown column properties in either mode.
    If table_filter is provided, only modifies tables in the filter set.
    """
    _KNOWN_COL_PROPS = {
        "name", "type", "dataType", "sourceColumn", "description", "isHidden",
        "displayFolder", "formatString", "dataCategory", "sortByColumn",
        "summarizeBy", "annotations", "extendedProperties", "lineageTag",
        "sourceLineageTag", "isKey", "isNullable", "isUnique", "isDefaultLabel",
        "isDefaultImage", "alignment", "tableDetailPosition",
        "isAvailableInMDX", "groupByColumns", "alternateOf", "encodingHint",
    }
    # Calc columns have additional valid props
    _CALC_COL_PROPS = _KNOWN_COL_PROPS | {"expression", "type"}
    _ROWNUM_PROPS = {"name", "type", "dataType", "isHidden", "annotations",
                     "isUnique", "isNullable", "lineageTag"}
    _CALC_TYPES = {"calculated", "calculatedTableColumn"}
    filter_set = set(table_filter) if table_filter is not None else None

    model = bim.get("model", bim)
    n_converted = 0
    for table in model.get("tables", []):
        tname = table.get("name", "")

        # Skip tables not in filter (e.g. enter-data tables in import mode)
        if filter_set is not None and tname not in filter_set:
            continue

        for col in table.get("columns", []):
            col_type = col.get("type")

            if col_type in _CALC_TYPES:
                if convert_calc:
                    # Convert to data column — keep only valid data column props
                    clean = {k: v for k, v in col.items() if k in _KNOWN_COL_PROPS}
                    clean.pop("type", None)
                    clean["sourceColumn"] = col["name"]
                    col.clear()
                    col.update(clean)
                    n_converted += 1
                else:
                    # Import mode: keep calc columns, just strip unknown props
                    unknown = [k for k in col if k not in _CALC_COL_PROPS]
                    for k in unknown:
                        col.pop(k)

            elif col_type == "rowNumber":
                unknown = [k for k in col if k not in _ROWNUM_PROPS]
                for k in unknown:
                    col.pop(k)

            else:
                unknown = [k for k in col if k not in _KNOWN_COL_PROPS]
                for k in unknown:
                    col.pop(k)
                if "sourceColumn" not in col and col.get("name"):
                    col["sourceColumn"] = col["name"]

    if n_converted:
        print(f"    Converted {n_converted} calculated column(s) to data", flush=True)

    return bim


def _modify_bim_via_tom(bim, lh_info, table_filter=None):
    """Modify model.bim using .NET TOM (Tabular Object Model).

    Uses TOM for structural changes (partitions, expressions, table filtering)
    then serializes back to JSON. Calculated column conversion is handled by
    JSON post-processing in _strip_unknown_bim_properties.

    Returns (bim_dict, expr_name).
    """
    # Bootstrap .NET runtime via sempy (handles Fabric CLR setup)
    import sempy.fabric as fabric
    fabric.create_tom_server()

    import clr
    clr.AddReference("Microsoft.AnalysisServices.Tabular")
    from Microsoft.AnalysisServices.Tabular import (
        JsonSerializer, ColumnType, Partition, ModeType,
        EntityPartitionSource, NamedExpression,
        ExpressionKind, PowerBIDataSourceVersion,
    )

    _strip_unknown_bim_properties(bim)
    bim_json = json.dumps(bim, indent=2)
    db = JsonSerializer.DeserializeDatabase(bim_json)
    model = db.Model

    # Set compatibility level first — DirectLake partitions require >= 1604
    db.CompatibilityLevel = max(db.CompatibilityLevel, 1604)

    # Build the OneLake expression
    onelake_url = f"https://{lh_info['onelake_host']}/{lh_info['ws_id']}/{lh_info['lh_id']}"
    expr_name = f"DirectLake - {lh_info['lh_name']}"
    schema = lh_info.get("default_schema")

    # Replace all expressions with our single OneLake source
    model.Expressions.Clear()
    expr = NamedExpression()
    expr.Name = expr_name
    expr.Kind = ExpressionKind.M
    expr.Expression = (
        "let\n"
        f'    Source = AzureStorage.DataLake("{onelake_url}", [HierarchicalNavigation=true])\n'
        "in\n"
        "    Source"
    )
    model.Expressions.Add(expr)

    # Build filter set
    filter_set = set(table_filter) if table_filter is not None else None

    # Modify each table for Direct Lake
    # Tables with Delta backing get entity partitions; others keep empty partitions
    n_with_delta = 0
    n_without = 0
    for table in model.Tables:
        tname = table.Name
        safe_name = _safe_folder_name(tname)
        has_delta = filter_set is None or tname in filter_set

        table.Partitions.Clear()

        if not has_delta:
            n_without += 1
            continue

        # Add Direct Lake entity partition
        part = Partition()
        part.Name = tname
        part.Mode = ModeType.DirectLake
        source = EntityPartitionSource()
        source.EntityName = safe_name
        if schema:
            source.SchemaName = schema
        source.ExpressionSource = model.Expressions[expr_name]
        part.Source = source
        table.Partitions.Add(part)
        n_with_delta += 1

        # Fix sourceColumn on data columns. Setting SourceLineageTag = col.Name
        # is critical for Direct Lake on OneLake: it tells the engine to bind
        # the model column directly to the Delta column by name. Without it,
        # the engine falls back to SQL-endpoint metadata, which is brittle for
        # newly-recreated tables.
        for col in table.Columns:
            if col.Type == ColumnType.Data:
                col.SourceColumn = col.Name
                col.SourceProviderType = None
                col.SourceLineageTag = col.Name

    # Remove query groups
    if hasattr(model, "QueryGroups") and model.QueryGroups is not None:
        model.QueryGroups.Clear()

    # Set Direct Lake compatible options
    model.DefaultPowerBIDataSourceVersion = PowerBIDataSourceVersion.PowerBI_V3

    # Remove relationships with incompatible data types (Direct Lake is strict)
    incompatible_rels = []
    for rel in model.Relationships:
        try:
            from_dt = rel.FromColumn.DataType
            to_dt = rel.ToColumn.DataType
            if from_dt != to_dt:
                incompatible_rels.append((
                    rel.Name,
                    rel.FromTable.Name, rel.FromColumn.Name, str(from_dt),
                    rel.ToTable.Name, rel.ToColumn.Name, str(to_dt),
                ))
        except Exception:
            pass
    if incompatible_rels:
        print(f"    ⚠ Dropping {len(incompatible_rels)} relationship(s) with mismatched data types "
              f"(Direct Lake requires identical FromColumn/ToColumn types):", flush=True)
        for rname, ft, fc, fdt, tt, tc, tdt in incompatible_rels:
            print(f"        {ft}[{fc}] ({fdt})  →  {tt}[{tc}] ({tdt})", flush=True)
        print(f"      Fix: align the `data_type` of these columns in the YAML config "
              f"(both sides of each relationship must match), then re-run.", flush=True)
        for rname, *_ in incompatible_rels:
            model.Relationships.Remove(rname)

    n_tables = model.Tables.Count
    n_rels = model.Relationships.Count
    n_measures = sum(t.Measures.Count for t in model.Tables)
    print(f"    TOM: {n_with_delta} tables with Delta, {n_without} without, {n_rels} relationships, {n_measures} measures", flush=True)

    # Serialize back to JSON and post-process
    result_json = JsonSerializer.SerializeDatabase(db)
    result_bim = json.loads(result_json)
    _strip_unknown_bim_properties(result_bim)
    _mark_as_direct_lake_on_onelake(result_bim)

    return result_bim, expr_name


def _mark_as_direct_lake_on_onelake(bim):
    """Annotate the BIM as a true Direct Lake on OneLake model.

    Without these markers the Power BI engine treats the model as a regular
    Direct Lake (SQL-endpoint-fronted) and falls back to SQL metadata for
    column resolution — which is brittle for tables that were recently
    recreated (the SQL endpoint sync may lag behind the Delta tables).

    Specifically:
      * Sets ``PBI_ProTooling = ["DirectLakeOnOneLakeInWeb"]`` on the model.
      * Removes ``__TEdtr`` (Tabular Editor leftover) from model annotations.
      * Strips per-table ``PBI_NavigationStepName`` and ``PBI_ResultType``
        annotations carried over from the source vpax (Power Query Navigation
        steps that have no meaning under Direct Lake).
    """
    model = bim.get("model", bim)

    # --- Model annotations ---
    annotations = model.setdefault("annotations", [])

    target_value = '["DirectLakeOnOneLakeInWeb"]'
    found = False
    for ann in annotations:
        if ann.get("name") == "PBI_ProTooling":
            ann["value"] = target_value
            found = True
            break
    if not found:
        annotations.append({"name": "PBI_ProTooling", "value": target_value})

    # Drop tooling leftovers that confuse the DLOOL profile
    _DROP_MODEL_ANNS = {"__TEdtr"}
    model["annotations"] = [
        a for a in annotations if a.get("name") not in _DROP_MODEL_ANNS
    ]

    # --- Per-table annotations ---
    _DROP_TABLE_ANNS = {"PBI_NavigationStepName", "PBI_ResultType"}
    for table in model.get("tables", []):
        anns = table.get("annotations")
        if not anns:
            continue
        cleaned = [a for a in anns if a.get("name") not in _DROP_TABLE_ANNS]
        if cleaned:
            table["annotations"] = cleaned
        else:
            table.pop("annotations", None)


_USERNAME_RE = None


def _get_username_re():
    """Lazy-compile regex matching USERNAME() / USERPRINCIPALNAME() (case-insensitive,
    only as whole tokens — won't match MYUSERNAME, etc.).
    """
    global _USERNAME_RE
    if _USERNAME_RE is None:
        import re
        # (?<![A-Za-z0-9_]) ensures preceding char isn't an identifier char
        _USERNAME_RE = re.compile(
            r"(?<![A-Za-z0-9_])(USERNAME|USERPRINCIPALNAME)\s*\(\s*\)",
            re.IGNORECASE,
        )
    return _USERNAME_RE


def _rewrite_dax(expr):
    """Rewrite USERNAME()/USERPRINCIPALNAME() to CUSTOMDATA() in a DAX expression
    (string or list-of-strings). Returns (new_expr, count_of_replacements)."""
    if expr is None:
        return expr, 0
    rx = _get_username_re()
    if isinstance(expr, list):
        joined = "\n".join(expr)
        new, n = rx.subn("CUSTOMDATA()", joined)
        if n:
            return new.split("\n"), n
        return expr, 0
    new, n = rx.subn("CUSTOMDATA()", expr)
    return new, n


def _rewrite_username_to_customdata(model):
    """Replace USERNAME()/USERPRINCIPALNAME() with CUSTOMDATA() in measures, KPIs,
    calculated columns, calculated tables, and RLS row-level filter expressions.
    Returns the number of expressions modified.
    """
    n = 0
    for table in model.get("tables", []):
        for measure in table.get("measures", []):
            new_expr, c = _rewrite_dax(measure.get("expression"))
            if c:
                measure["expression"] = new_expr
                n += 1
            kpi = measure.get("kpi")
            if isinstance(kpi, dict):
                for key in ("targetExpression", "statusExpression", "trendExpression"):
                    new_expr, c = _rewrite_dax(kpi.get(key))
                    if c:
                        kpi[key] = new_expr
                        n += 1
        for col in table.get("columns", []):
            if col.get("type") in ("calculated", "calculatedTableColumn"):
                new_expr, c = _rewrite_dax(col.get("expression"))
                if c:
                    col["expression"] = new_expr
                    n += 1
        for part in table.get("partitions", []):
            src = part.get("source", {})
            if src.get("type") == "calculated":
                new_expr, c = _rewrite_dax(src.get("expression"))
                if c:
                    src["expression"] = new_expr
                    n += 1
    for role in model.get("roles", []):
        for tp in role.get("tablePermissions", []):
            new_expr, c = _rewrite_dax(tp.get("filterExpression"))
            if c:
                tp["filterExpression"] = new_expr
                n += 1
    return n


def deploy_semantic_model(
    vpax_path,
    dataset=None,
    workspace=None,
    lakehouse=None,
    lakehouse_workspace=None,
    mode="import",
    overwrite=False,
    table_filter=None,
    replace_username_with_customdata=False,
    config=None,
):
    """Deploy a semantic model from a VPAX file's Model.bim.

    Extracts the full model definition from the VPAX, modifies partitions
    to point at OneLake Delta tables, and deploys via the Fabric REST API.
    Preserves the entire original model — relationships, measures, hierarchies,
    roles, column metadata, format strings, display folders, etc.

    Args:
        vpax_path: Path to the .vpax file.
        dataset: Semantic model name (defaults to the VPAX model name).
        workspace: Target Fabric workspace name or ID.
        lakehouse: Lakehouse containing the generated Delta tables.
        lakehouse_workspace: Workspace of the lakehouse (if different).
        mode: ``"import"`` (default) or ``"direct_lake"``.
        overwrite: Overwrite an existing model with the same name.
        table_filter: Optional list of table names to include.
        replace_username_with_customdata: If True, rewrite USERNAME() and
            USERPRINCIPALNAME() calls in measure expressions and RLS row
            filters to CUSTOMDATA(). Useful for load testing where the
            tester impersonates many users via the CustomData connection
            property without needing real EffectiveUserName.
    """
    import sempy_labs as sl

    # Extract the full model.bim from the VPAX
    print("  Extracting Model.bim from VPAX ...", flush=True)
    bim = _extract_bim(vpax_path)

    name = dataset
    if not name:
        # Try to get a friendly name from the VPAX filename
        import os
        vpax_basename = os.path.splitext(os.path.basename(vpax_path))[0]
        # Strip common suffixes like " VPAX", ".vpax"
        for suffix in [" VPAX", " vpax", "_VPAX", "_vpax"]:
            if vpax_basename.endswith(suffix):
                vpax_basename = vpax_basename[:-len(suffix)]
        name = vpax_basename.strip() or "Model"

    # Get lakehouse details
    print("  Resolving lakehouse connection ...", flush=True)
    lh_info = _get_lakehouse_info(lakehouse, lakehouse_workspace or workspace)

    # Modify the BIM based on mode
    if mode == "import":
        print("  Converting model to Import from OneLake ...", flush=True)
        _strip_unknown_bim_properties(bim, table_filter=table_filter, convert_calc=False)
        bim = _modify_bim_for_import(bim, lh_info, table_filter)
    else:
        print("  Converting model to Direct Lake on OneLake ...", flush=True)
        # Patch BIM column dataTypes from the (possibly auto-aligned) config
        # BEFORE the conversion, so the relationship type check inside
        # _modify_bim_for_direct_lake sees the corrected types.
        # In DL, calculated columns get converted to data — also override their
        # BIM dataType from config (their DAX-derived type is no longer
        # authoritative once they're stored as Delta data).
        if config is not None:
            _apply_config_column_types_to_bim(bim, config, include_calculated=True)
        bim, expr_name = _modify_bim_for_direct_lake(bim, lh_info, table_filter)

    # Update model name
    bim["name"] = name
    if "model" in bim and isinstance(bim["model"], dict):
        bim["model"]["name"] = name
    model = bim.get("model", bim)

    if replace_username_with_customdata:
        n_repl = _rewrite_username_to_customdata(model)
        print(f"  Rewrote USERNAME()/USERPRINCIPALNAME() → CUSTOMDATA() in "
              f"{n_repl} expression(s)", flush=True)

    n_tables = len(model.get("tables", []))
    n_rels = len(model.get("relationships", []))
    n_measures = sum(len(t.get("measures", [])) for t in model.get("tables", []))

    # Save BIM for troubleshooting
    bim_path = "/lakehouse/default/Files/datagen/model.bim"
    try:
        import os
        os.makedirs(os.path.dirname(bim_path), exist_ok=True)
        with open(bim_path, "w", encoding="utf-8") as f:
            json.dump(bim, f, indent=2)
        print(f"  Saved model.bim to Files/datagen/model.bim", flush=True)
    except Exception as e:
        print(f"  ⚠ Could not save model.bim: {e}", flush=True)

    # Deploy via Fabric REST API
    print(f"  Deploying '{name}' ({n_tables} tables, {n_rels} relationships, {n_measures} measures) ...", flush=True)

    actual_name = _deploy_bim(bim, name, workspace, overwrite)

    # Import mode reads from OneLake via AzureStorage.DataLake. Bind the model
    # to a workspace-identity-credentialled cloud connection so refresh can
    # authenticate without per-user OAuth.
    if mode == "import":
        try:
            _bind_import_model_to_workspace_identity(actual_name, lh_info, workspace)
        except Exception as e:
            print(f"  ⚠ Could not auto-bind workspace-identity connection: {e}", flush=True)
            print(f"    Configure manually: Semantic model settings → Data source", flush=True)
            print(f"    credentials → sign in with the workspace identity.", flush=True)

    # Refresh using the actual display name from the API
    print("  Refreshing model ...", flush=True)
    refresh_ok = False
    try:
        sl.refresh_semantic_model(dataset=actual_name, workspace=workspace)
        print("    ✓ Refresh complete", flush=True)
        refresh_ok = True
    except Exception as e:
        err_msg = str(e)[:300]
        print(f"    ✗ Refresh failed: {err_msg}", flush=True)
        # Get detailed error from refresh history
        import time as _time2
        _time2.sleep(3)  # let refresh history populate
        try:
            _print_refresh_errors(actual_name, workspace)
        except Exception as re:
            print(f"    ⚠ Could not fetch refresh details: {re}", flush=True)

    mode_label = "Direct Lake" if mode == "direct_lake" else "Import"
    print(flush=True)
    if refresh_ok:
        print(f"✓ Semantic model '{actual_name}' deployed ({mode_label} on OneLake)")
    else:
        print(f"⚠ Semantic model '{actual_name}' deployed but refresh failed")
    print(f"  Mode:          {mode_label}")
    print(f"  Tables:        {n_tables}")
    print(f"  Relationships: {n_rels}")
    print(f"  Measures:      {n_measures}", flush=True)


def _bind_import_model_to_workspace_identity(dataset_name, lh_info, workspace=None):
    """Bind the deployed import-mode model to a workspace-identity ADLS connection.

    Creates (or reuses) a Fabric cloud connection of type AzureDataLakeStorage with
    WorkspaceIdentity credentials pointing at the source lakehouse's OneLake path,
    then takes over the dataset and binds it to that connection so refresh can
    authenticate without per-user OAuth.

    Prints a notice that the user must (1) enable workspace identity on the
    consuming workspace and (2) grant that identity access to the source lakehouse
    (ReadAll on the lakehouse, or Contributor on the lakehouse's workspace).
    """
    import sempy.fabric as fabric
    from sempy_labs._helper_functions import resolve_workspace_name_and_id
    import notebookutils
    import requests

    (ws_name, ws_id) = resolve_workspace_name_and_id(workspace)

    # The Connections API requires a Power BI token with broad scopes.
    # FabricRestClient() returns a narrowly-scoped token that 403s on /v1/connections,
    # so use the notebookutils-issued "pbi" token (the user's interactive scopes).
    pbi_token = notebookutils.credentials.getToken("pbi")
    fabric_base = "https://api.fabric.microsoft.com"
    pbi_base = "https://api.powerbi.com"
    headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}

    # Resolve dataset id (FabricRestClient is fine for read-only items list)
    fab = fabric.FabricRestClient()
    items = fab.get(f"/v1/workspaces/{ws_id}/semanticModels").json().get("value", [])
    model = next((i for i in items if i["displayName"] == dataset_name), None)
    if not model:
        raise RuntimeError(f"Semantic model '{dataset_name}' not found after deploy")
    model_id = model["id"]

    # Server + path: AzureDataLakeStorage connections want server=https://<host>
    # and path=/<wsId>/<lhId>/Tables (the model's M expressions point at this prefix).
    server = f"https://{lh_info['onelake_host']}"
    path = f"/{lh_info['ws_id']}/{lh_info['lh_id']}/Tables"
    conn_name = f"{ws_name}-{lh_info['lh_name']}-ADLS"

    # If the model is already bound to a gateway/SCC (e.g. when overwriting an
    # existing model), reuse it — don't create a new SCC. The user is responsible
    # for ensuring its credentials are valid for the lakehouse.
    existing_conn_id = None
    existing_conn_name = None
    try:
        r = requests.get(
            f"{pbi_base}/v1.0/myorg/groups/{ws_id}/datasets/{model_id}/datasources",
            headers=headers, timeout=60,
        )
        if r.status_code == 200:
            for ds in r.json().get("value", []):
                gid = ds.get("gatewayId")
                if gid and gid != "00000000-0000-0000-0000-000000000000":
                    existing_conn_id = gid
                    break
    except Exception:
        pass

    if existing_conn_id:
        # Look up the SCC's display name (best effort)
        try:
            r = requests.get(
                f"{fabric_base}/v1/connections/{existing_conn_id}",
                headers=headers, timeout=60,
            )
            if r.status_code == 200:
                existing_conn_name = r.json().get("displayName")
        except Exception:
            pass

        label = f"'{existing_conn_name}'" if existing_conn_name else f"({existing_conn_id})"
        print(
            f"  ✓ Model already bound to existing cloud connection {label} — reusing.",
            flush=True,
        )
        print(
            "  ⚠ ACTION REQUIRED for refresh to succeed:\n"
            f"    • Ensure that the cloud connection {label} is configured with\n"
            f"      credentials that can access the lakehouse '{lh_info['lh_name']}'\n"
            f"      at {server}{path}.",
            flush=True,
        )
        return

    # No existing binding — find an SCC by our naming convention or create a new one
    conn_id = None
    try:
        r = requests.get(f"{fabric_base}/v1/connections", headers=headers, timeout=60)
        if r.status_code == 200:
            match = next(
                (c for c in r.json().get("value", []) if c.get("displayName") == conn_name),
                None,
            )
            if match:
                conn_id = match["id"]
    except Exception:
        pass

    created_new = False
    if not conn_id:
        body = {
            "connectivityType": "ShareableCloud",
            "displayName": conn_name,
            "connectionDetails": {
                "type": "AzureDataLakeStorage",
                "creationMethod": "AzureDataLakeStorage",
                "parameters": [
                    {"name": "server", "dataType": "Text", "value": server},
                    {"name": "path", "dataType": "Text", "value": path},
                ],
            },
            "privacyLevel": "Organizational",
            "credentialDetails": {
                "singleSignOnType": "None",
                "connectionEncryption": "NotEncrypted",
                "skipTestConnection": False,
                "credentials": {"credentialType": "WorkspaceIdentity"},
            },
        }
        r = requests.post(
            f"{fabric_base}/v1/connections", headers=headers, json=body, timeout=60,
        )
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Create connection failed ({r.status_code}): {r.text[:400]}")
        conn_id = r.json()["id"]
        created_new = True

    # Take over (required before BindToGateway) and bind
    r = requests.post(
        f"{pbi_base}/v1.0/myorg/groups/{ws_id}/datasets/{model_id}/Default.TakeOver",
        headers=headers, timeout=60,
    )
    if r.status_code not in (200, 202):
        raise RuntimeError(f"TakeOver failed ({r.status_code}): {r.text[:300]}")
    r = requests.post(
        f"{pbi_base}/v1.0/myorg/groups/{ws_id}/datasets/{model_id}/Default.BindToGateway",
        headers=headers,
        json={"gatewayObjectId": conn_id, "datasourceObjectIds": [conn_id]},
        timeout=60,
    )
    if r.status_code not in (200, 202):
        raise RuntimeError(f"BindToGateway failed ({r.status_code}): {r.text[:300]}")

    label = f"'{conn_name}'"
    if created_new:
        print(
            f"  ✓ Created cloud connection {label} configured with Workspace "
            f"Identity, and bound the model to it.", flush=True,
        )
    else:
        print(
            f"  ✓ Bound model to existing cloud connection {label}.",
            flush=True,
        )
    print(
        "  ⚠ ACTION REQUIRED for refresh to succeed:\n"
        f"    • Ensure that the cloud connection {label} is configured with\n"
        f"      credentials that can access the lakehouse '{lh_info['lh_name']}'\n"
        f"      at {server}{path}.",
        flush=True,
    )


def _print_refresh_errors(dataset, workspace=None):
    """Fetch and display detailed refresh errors from the Power BI REST API."""
    import sempy.fabric as fabric
    from sempy_labs._helper_functions import resolve_workspace_name_and_id

    (ws_name, ws_id) = resolve_workspace_name_and_id(workspace)

    # Find the semantic model ID via Fabric API
    client = fabric.FabricRestClient()
    items = client.get(f"/v1/workspaces/{ws_id}/semanticModels").json().get("value", [])
    model = next((i for i in items if i["displayName"] == dataset), None)
    if not model:
        return

    model_id = model["id"]

    # Get refresh history via Power BI API (not Fabric Items API)
    pbi_client = fabric.PowerBIRestClient()
    resp = pbi_client.get(f"/v1.0/myorg/groups/{ws_id}/datasets/{model_id}/refreshes?$top=1")
    if resp.status_code != 200:
        print(f"    ⚠ Refresh history API returned {resp.status_code}: {resp.text[:200]}", flush=True)
        return

    refreshes = resp.json().get("value", [])
    if not refreshes:
        return

    # Show errors from the most recent refresh
    latest = refreshes[0]
    status = latest.get("status", "")

    # Dump all keys so we can see what's available
    if status.lower() not in ("completed", "succeeded"):
        print(f"    Refresh details (status={status}):", flush=True)

        # Check for top-level error
        for key in ("serviceExceptionJson", "error", "messages", "extendedStatus"):
            val = latest.get(key)
            if val:
                if isinstance(val, str):
                    print(f"      {key}: {val[:500]}", flush=True)
                elif isinstance(val, list):
                    for msg in val[:5]:
                        if isinstance(msg, dict):
                            print(f"      {msg.get('message', msg)}", flush=True)
                        else:
                            print(f"      {msg}", flush=True)
                elif isinstance(val, dict):
                    print(f"      {key}: {json.dumps(val, indent=2)[:500]}", flush=True)

        # Check per-object errors
        for obj in latest.get("objects", []):
            obj_status = obj.get("status", "")
            if obj_status.lower() not in ("completed", "succeeded", ""):
                table = obj.get("table", "?")
                partition = obj.get("partition", "?")
                msgs = obj.get("messages", [])
                if msgs:
                    for msg in msgs[:3]:
                        if isinstance(msg, dict):
                            print(f"      {table}: {msg.get('message', msg)}", flush=True)
                        else:
                            print(f"      {table}: {msg}", flush=True)
                else:
                    print(f"      {table}: status={obj_status}", flush=True)

        # If nothing printed, dump the raw response
        if not any(latest.get(k) for k in ("serviceExceptionJson", "error", "messages", "objects")):
            print(f"      Raw: {json.dumps(latest, indent=2)[:800]}", flush=True)


def _deploy_bim(bim, name, workspace=None, overwrite=False):
    """Deploy a model.bim via the Fabric REST API."""
    import sempy.fabric as fabric
    from sempy_labs._helper_functions import resolve_workspace_name_and_id
    import time

    (ws_name, ws_id) = resolve_workspace_name_and_id(workspace)

    # Build model.bim payload
    bim_json = json.dumps(bim, indent=2)
    bim_b64 = base64.b64encode(bim_json.encode("utf-8")).decode("utf-8")
    pbism_b64 = base64.b64encode(json.dumps({"version": "4.0", "settings": {}}).encode("utf-8")).decode("utf-8")

    definition = {
        "parts": [
            {"path": "model.bim", "payload": bim_b64, "payloadType": "InlineBase64"},
            {"path": "definition.pbism", "payload": pbism_b64, "payloadType": "InlineBase64"},
        ]
    }

    # Check if model exists
    client = fabric.FabricRestClient()
    items = client.get(f"/v1/workspaces/{ws_id}/semanticModels").json().get("value", [])
    existing = next((i for i in items if i["displayName"] == name), None)

    if existing and overwrite:
        model_id = existing["id"]
        print(f"    Updating existing model ({model_id}) ...", flush=True)
        resp = client.post(
            f"/v1/workspaces/{ws_id}/semanticModels/{model_id}/updateDefinition",
            json={"definition": definition},
        )
        if resp.status_code not in (200, 202):
            raise RuntimeError(f"Update failed ({resp.status_code}): {resp.text[:500]}")
        if resp.status_code == 202:
            _poll_async(client, resp)
        return existing["displayName"]

    elif existing and not overwrite:
        raise RuntimeError(
            f"Semantic model '{name}' already exists in '{ws_name}'. "
            f"Use overwrite=True or overwrite_model=True to replace it."
        )

    else:
        # Create new
        print(f"    Creating new semantic model ...", flush=True)
        resp = client.post(f"/v1/workspaces/{ws_id}/items", json={
            "displayName": name,
            "type": "SemanticModel",
            "definition": definition,
        })
        if resp.status_code not in (200, 201, 202):
            raise RuntimeError(f"Create failed ({resp.status_code}): {resp.text[:500]}")
        if resp.status_code == 202:
            _poll_async(client, resp)

        # Verify the model was actually created
        time.sleep(3)
        items = client.get(f"/v1/workspaces/{ws_id}/semanticModels").json().get("value", [])
        created = next((i for i in items if i["displayName"] == name), None)
        if created:
            return created["displayName"]

        all_names = [i["displayName"] for i in items]
        raise RuntimeError(
            f"Model '{name}' was not found after creation. "
            f"Models in workspace: {all_names}"
        )


def _poll_async(client, resp):
    """Poll an async Fabric REST API operation until completion."""
    import time
    location = resp.headers.get("Location", "")
    retry_after = int(resp.headers.get("Retry-After", "5"))
    if not location:
        time.sleep(10)
        return

    for attempt in range(60):
        time.sleep(retry_after)
        poll = client.get(location)

        body = {}
        try:
            body = poll.json()
        except Exception:
            pass

        status = body.get("status", "")
        error = body.get("error", {})

        if poll.status_code == 200:
            if status.lower() == "failed" or error:
                err_msg = error.get("message", "") or body.get("failureReason", "") or str(body)[:500]
                raise RuntimeError(f"Async operation failed: {err_msg}")
            return

        if poll.status_code == 202:
            retry_after = int(poll.headers.get("Retry-After", "5"))
            continue

        err_detail = body.get("error", {}).get("message", poll.text[:300])
        raise RuntimeError(f"Async poll returned {poll.status_code}: {err_detail}")

    raise RuntimeError("Async operation timed out after 60 poll attempts")


# ---------------------------------------------------------------------------
# TMDL offline generation (kept for version control / MCP import)
# ---------------------------------------------------------------------------

def build_tmdl(vpax_path, output_folder, **kwargs):
    """Generate a TMDL folder from a .vpax file (offline, for version control)."""
    # This is a simplified version — for full model fidelity, use deploy_semantic_model
    raise NotImplementedError(
        "build_tmdl is deprecated in favor of deploy_semantic_model which uses "
        "the full Model.bim from the VPAX. For offline use, extract Model.bim "
        "directly from the VPAX file."
    )
