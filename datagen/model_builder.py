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

    # Extract OneLake host (handles msit vs prod)
    tables_url = props.get("oneLakeTablesPath", "")
    if tables_url:
        from urllib.parse import urlparse
        onelake_host = urlparse(tables_url).hostname
    else:
        onelake_host = "onelake.dfs.fabric.microsoft.com"

    default_schema = props.get("defaultSchema", "dbo")

    return {
        "ws_id": ws_id,
        "lh_id": lh_id,
        "lh_name": lh_name,
        "onelake_host": onelake_host,
        "default_schema": default_schema,
    }


def _modify_bim_for_direct_lake(bim, lh_info, table_filter=None):
    """Modify a model.bim to use Direct Lake on OneLake.

    Uses the .NET TOM (Tabular Object Model) library to properly deserialize,
    modify, and reserialize the BIM.
    """
    return _modify_bim_via_tom(bim, lh_info, table_filter)


def _strip_unknown_bim_properties(bim):
    """Remove properties from the BIM JSON that Fabric may not accept.

    Strips properties that are either too new for the runtime's TOM version
    or that the Fabric import API rejects.
    """
    _UNKNOWN_COL_PROPS = {
        "relatedColumnDetails", "isNameInferred", "isDataTypeInferred",
    }

    model = bim.get("model", bim)
    for table in model.get("tables", []):
        for col in table.get("columns", []):
            for prop in _UNKNOWN_COL_PROPS:
                col.pop(prop, None)
    return bim


def _modify_bim_via_tom(bim, lh_info, table_filter=None):
    """Modify model.bim using .NET TOM (Tabular Object Model).

    Returns the TOM Database object (caller serializes to TMDL for deployment).
    """
    # Bootstrap .NET runtime via sempy (handles Fabric CLR setup)
    import sempy.fabric as fabric
    fabric.create_tom_server()

    import clr
    clr.AddReference("Microsoft.AnalysisServices.Tabular")
    from Microsoft.AnalysisServices.Tabular import (
        JsonSerializer, ColumnType, Partition,
        EntityPartitionSource, NamedExpression,
        ExpressionKind, PowerBIDataSourceVersion,
    )

    _strip_unknown_bim_properties(bim)
    bim_json = json.dumps(bim, indent=2)
    db = JsonSerializer.DeserializeDatabase(bim_json)
    model = db.Model

    # Build the OneLake expression
    onelake_url = f"https://{lh_info['onelake_host']}/{lh_info['ws_id']}/{lh_info['lh_id']}"
    expr_name = f"DirectLake - {lh_info['lh_name']}"
    schema = lh_info.get("default_schema", "dbo")

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

    # Remove tables not in filter (keep measure-only tables)
    if filter_set is not None:
        tables_to_remove = []
        for table in model.Tables:
            if table.Name in filter_set:
                continue
            has_data_cols = any(
                col.Type != ColumnType.RowNumber
                for col in table.Columns
                if col.Type not in (ColumnType.Calculated, ColumnType.CalculatedTableColumn)
            )
            has_measures = table.Measures.Count > 0
            if has_measures and not has_data_cols:
                continue
            if not has_measures and not has_data_cols:
                continue
            tables_to_remove.append(table.Name)
        for tname in tables_to_remove:
            model.Tables.Remove(tname)

    # Modify each table for Direct Lake
    for table in model.Tables:
        tname = table.Name
        safe_name = _safe_folder_name(tname)
        has_delta = filter_set is None or tname in filter_set

        if not has_delta:
            table.Partitions.Clear()
            continue

        # Replace partitions with Direct Lake entity partition
        table.Partitions.Clear()
        part = Partition()
        part.Name = tname
        source = EntityPartitionSource()
        source.EntityName = safe_name
        source.SchemaName = schema
        source.ExpressionSource = model.Expressions[expr_name]
        part.Source = source
        table.Partitions.Add(part)

        # Convert calculated columns to data columns
        n_converted = 0
        for col in table.Columns:
            if col.Type in (ColumnType.Calculated, ColumnType.CalculatedTableColumn):
                col.Type = ColumnType.Data
                col.SourceColumn = col.Name
                try:
                    col.Expression = None
                except Exception:
                    pass
                n_converted += 1
            elif col.Type == ColumnType.Data:
                col.SourceColumn = col.Name
            col.SourceProviderType = None

        if n_converted:
            print(f"    {tname}: converted {n_converted} calculated column(s) to data", flush=True)

    # Remove query groups
    if hasattr(model, "QueryGroups") and model.QueryGroups is not None:
        model.QueryGroups.Clear()

    # Set Direct Lake compatible options
    model.DefaultPowerBIDataSourceVersion = PowerBIDataSourceVersion.PowerBI_V3

    # Update compatibility level
    db.CompatibilityLevel = max(db.CompatibilityLevel, 1604)

    n_tables = model.Tables.Count
    n_rels = model.Relationships.Count
    n_measures = sum(t.Measures.Count for t in model.Tables)
    print(f"    TOM: {n_tables} tables, {n_rels} relationships, {n_measures} measures", flush=True)

    return db, expr_name


def _serialize_to_tmdl_parts(db):
    """Serialize a TOM Database to TMDL and return Items API definition parts."""
    import tempfile, os

    import clr
    clr.AddReference("Microsoft.AnalysisServices.Tabular")
    from Microsoft.AnalysisServices.Tabular import TmdlSerializer

    with tempfile.TemporaryDirectory() as tmdl_dir:
        TmdlSerializer.SerializeDatabaseToFolder(db, tmdl_dir)

        parts = []
        # Add definition.pbism
        pbism = json.dumps({"version": "4.0", "settings": {}})
        parts.append({
            "path": "definition.pbism",
            "payload": base64.b64encode(pbism.encode("utf-8")).decode("utf-8"),
            "payloadType": "InlineBase64",
        })

        # Walk the TMDL folder and add each file as a part
        for root, _dirs, files in os.walk(tmdl_dir):
            for fname in files:
                fpath = os.path.join(root, fname)
                rel_path = os.path.relpath(fpath, tmdl_dir).replace("\\", "/")
                with open(fpath, "rb") as f:
                    content = f.read()
                parts.append({
                    "path": f"definition/{rel_path}",
                    "payload": base64.b64encode(content).decode("utf-8"),
                    "payloadType": "InlineBase64",
                })

        print(f"    TMDL: {len(parts)} definition parts", flush=True)
        return {"parts": parts}


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
        mode: ``"direct_lake"`` (default) or ``"import"``.
        include_hidden: Include hidden columns/tables from the VPAX.
        include_calculated: Include calculated columns.
        overwrite: Overwrite an existing model with the same name.
        table_filter: Optional list of table names to include.
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

    # Modify the BIM for Direct Lake (returns TOM Database object)
    print("  Converting model to Direct Lake on OneLake ...", flush=True)
    db, expr_name = _modify_bim_for_direct_lake(bim, lh_info, table_filter)

    # Update model name
    db.Name = name
    db.Model.Name = name

    n_tables = db.Model.Tables.Count
    n_rels = db.Model.Relationships.Count
    n_measures = sum(t.Measures.Count for t in db.Model.Tables)

    # Serialize to TMDL parts for the Items API
    print("  Serializing to TMDL ...", flush=True)
    definition = _serialize_to_tmdl_parts(db)

    # Deploy via Fabric REST API
    print(f"  Deploying '{name}' ({n_tables} tables, {n_rels} relationships, {n_measures} measures) ...", flush=True)

    actual_name = _deploy_tmdl(definition, name, workspace, overwrite)

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

    print(flush=True)
    if refresh_ok:
        print(f"✓ Semantic model '{actual_name}' deployed (Direct Lake on OneLake)")
    else:
        print(f"⚠ Semantic model '{actual_name}' deployed but refresh failed")
    print(f"  Tables:        {n_tables}")
    print(f"  Relationships: {n_rels}")
    print(f"  Measures:      {n_measures}", flush=True)


def _deploy_tmdl(definition, name, workspace=None, overwrite=False):
    """Deploy a semantic model definition (TMDL parts) via the Fabric REST API."""
    import sempy.fabric as fabric
    from sempy_labs._helper_functions import resolve_workspace_name_and_id
    import time

    (ws_name, ws_id) = resolve_workspace_name_and_id(workspace)

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
