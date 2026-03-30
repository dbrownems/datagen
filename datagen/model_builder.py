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
    with zipfile.ZipFile(vpax_path, "r") as zf:
        names = zf.namelist()
        # Look for Model.bim (case-insensitive)
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

    - Replaces all table partitions with entity partitions pointing at
      OneLake Delta tables (using sanitized folder names).
    - Replaces expressions with a single OneLake data source.
    - Removes incompatible Import-mode settings.
    - Preserves everything else: relationships, measures, columns,
      hierarchies, roles, annotations, etc.
    """
    model = bim.get("model", bim)

    # Build the OneLake expression
    onelake_url = f"https://{lh_info['onelake_host']}/{lh_info['ws_id']}/{lh_info['lh_id']}"
    expr_name = f"DirectLake - {lh_info['lh_name']}"
    schema = lh_info.get("default_schema", "dbo")

    # Replace all expressions with our single OneLake source
    model["expressions"] = [
        {
            "name": expr_name,
            "kind": "m",
            "expression": [
                "let",
                f'    Source = AzureStorage.DataLake("{onelake_url}", [HierarchicalNavigation=true])',
                "in",
                "    Source",
            ],
        }
    ]

    # Filter tables if specified — but keep measure-only tables (no data columns)
    if table_filter is not None:
        filter_set = set(table_filter)
        kept_tables = []
        for t in model.get("tables", []):
            if t["name"] in filter_set:
                kept_tables.append(t)
            else:
                # Keep tables that only have measures (no data columns needing a Delta table)
                has_data_cols = any(
                    c.get("type", "data") != "calculated"
                    for c in t.get("columns", [])
                    if c.get("name", "").lower() != "rownumber-2662979b-1795-4f74-8f37-6a1ba8059b61"
                )
                has_measures = bool(t.get("measures"))
                if has_measures and not has_data_cols:
                    kept_tables.append(t)
        model["tables"] = kept_tables

    # Modify each table's partition to Direct Lake entity
    # Skip measure-only tables (they don't need partitions)
    for table in model.get("tables", []):
        tname = table["name"]
        safe_name = _safe_folder_name(tname)

        # Check if this table has a corresponding Delta table
        has_delta = table_filter is None or tname in (table_filter or [])
        if not has_delta:
            # Measure-only table — remove partitions entirely
            table["partitions"] = []
            continue

        # Replace partitions with a single Direct Lake entity partition
        table["partitions"] = [
            {
                "name": tname,
                "mode": "directLake",
                "source": {
                    "type": "entity",
                    "entityName": safe_name,
                    "schemaName": schema,
                    "expressionSource": expr_name,
                },
            }
        ]

        # Remove import-mode column source properties that conflict
        for col in table.get("columns", []):
            # sourceColumn must match column name for Direct Lake
            if "sourceColumn" in col:
                col["sourceColumn"] = col["name"]
            # Remove any import-specific properties
            col.pop("sourceProviderType", None)

    # Remove query groups (import-mode M query organization)
    model.pop("queryGroups", None)

    # Set Direct Lake compatible data access options
    model["defaultPowerBIDataSourceVersion"] = "powerBI_V3"
    if "dataAccessOptions" not in model:
        model["dataAccessOptions"] = {}

    # Update top-level compatibility
    bim["compatibilityLevel"] = max(bim.get("compatibilityLevel", 1604), 1604)

    return bim, expr_name


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

    # Modify the BIM for Direct Lake
    print("  Converting model to Direct Lake on OneLake ...", flush=True)
    bim, expr_name = _modify_bim_for_direct_lake(bim, lh_info, table_filter)

    # Update model name
    bim["name"] = name
    model = bim.get("model", bim)

    n_tables = len(model.get("tables", []))
    n_rels = len(model.get("relationships", []))
    n_measures = sum(
        len(t.get("measures", []))
        for t in model.get("tables", [])
    )

    # Deploy via Fabric REST API
    print(f"  Deploying '{name}' ({n_tables} tables, {n_rels} relationships, {n_measures} measures) ...", flush=True)

    _deploy_bim(bim, name, workspace, overwrite)

    # Refresh
    print("  Refreshing model ...", flush=True)
    try:
        sl.refresh_semantic_model(dataset=name, workspace=workspace)
        print("  Refresh complete.", flush=True)
    except Exception as e:
        err_msg = str(e)[:200]
        print(f"    ⚠ Refresh: {err_msg}", flush=True)

    print(flush=True)
    print(f"✓ Semantic model '{name}' deployed (Direct Lake on OneLake)")
    print(f"  Tables:        {n_tables}")
    print(f"  Relationships: {n_rels}")
    print(f"  Measures:      {n_measures}", flush=True)


def _deploy_bim(bim, name, workspace=None, overwrite=False):
    """Deploy a model.bim via the Fabric REST API."""
    import sempy.fabric as fabric
    from sempy_labs._helper_functions import resolve_workspace_name_and_id

    (ws_name, ws_id) = resolve_workspace_name_and_id(workspace)

    # Build model.bim payload
    bim_json = json.dumps(bim, indent=2)
    bim_b64 = base64.b64encode(bim_json.encode("utf-8")).decode("utf-8")

    # Build definition.pbism
    pbism = {
        "version": "4.0",
        "settings": {},
    }
    pbism_b64 = base64.b64encode(json.dumps(pbism).encode("utf-8")).decode("utf-8")

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
        # Update existing model definition
        model_id = existing["id"]
        resp = client.post(
            f"/v1/workspaces/{ws_id}/semanticModels/{model_id}/updateDefinition",
            json={"definition": definition},
        )
        if resp.status_code not in (200, 202):
            raise RuntimeError(f"Update failed ({resp.status_code}): {resp.text[:300]}")
    elif existing and not overwrite:
        raise RuntimeError(
            f"Semantic model '{name}' already exists in '{ws_name}'. "
            f"Use overwrite=True or overwrite_model=True to replace it."
        )
    else:
        # Create new
        body = {
            "displayName": name,
            "type": "SemanticModel",
            "definition": definition,
        }
        resp = client.post(f"/v1/workspaces/{ws_id}/items", json=body)
        if resp.status_code not in (200, 201, 202):
            raise RuntimeError(f"Create failed ({resp.status_code}): {resp.text[:300]}")


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
