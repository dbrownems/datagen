"""Parse .vpax files (VertiPaq Analyzer exports) to extract model metadata.

VPAX files are ZIP archives containing a DaxModel.json with table/column statistics.
"""

import json
import zipfile
from pathlib import Path


def _get(obj, *keys, default=None):
    """Try multiple possible field names, return the first match."""
    for key in keys:
        if key in obj:
            return obj[key]
    return default


def _parse_column(col_data):
    """Parse a single column entry from the VPAX model."""
    col_type = _get(col_data, "ColumnType", "columnType", "Type", default="Data")

    # Skip internal columns
    if col_type in ("RowNumber", "rownumber"):
        return None

    name = _get(col_data, "ColumnName", "Name", "name", "columnName", default="")
    if not name:
        return None

    data_type = _get(
        col_data, "DataType", "dataType", "data_type", default="String"
    )
    # Normalize data type names
    dtype_map = {
        "string": "string",
        "text": "string",
        "int64": "int64",
        "integer": "int64",
        "whole number": "int64",
        "long": "int64",
        "double": "double",
        "decimal": "double",
        "decimal number": "double",
        "currency": "double",
        "float": "double",
        "datetime": "datetime",
        "date": "datetime",
        "date/time": "datetime",
        "boolean": "boolean",
        "true/false": "boolean",
        "binary": "binary",
    }
    data_type = dtype_map.get(data_type.lower().strip(), "string")

    cardinality = _get(
        col_data,
        "ColumnCardinality", "Cardinality", "cardinality",
        "columnCardinality", "DistinctValueCount",
        default=0,
    )

    total_size = _get(
        col_data,
        "TotalSize", "ColumnSize", "columnSize", "totalSize", "Size",
        default=0,
    )
    dict_size = _get(
        col_data,
        "DictionarySize", "dictionarySize", "DictSize",
        default=0,
    )
    data_size = _get(
        col_data,
        "DataSize", "dataSize",
        default=0,
    )

    min_val = _get(
        col_data,
        "MinColumnValue", "MinValue", "minValue", "Min",
        default=None,
    )
    max_val = _get(
        col_data,
        "MaxColumnValue", "MaxValue", "maxValue", "Max",
        default=None,
    )

    is_hidden = _get(col_data, "IsHidden", "isHidden", default=False)

    encoding = _get(col_data, "Encoding", "encoding", default="")

    # Semantic model metadata (used by model_builder)
    format_string = _get(
        col_data, "FormatString", "formatString", default=None,
    )
    description = _get(
        col_data, "Description", "description", default=None,
    )
    display_folder = _get(
        col_data, "DisplayFolder", "displayFolder", default=None,
    )
    sort_by_column = _get(
        col_data, "SortByColumn", "sortByColumn", default=None,
    )
    data_category = _get(
        col_data, "DataCategory", "dataCategory", default=None,
    )
    summarize_by = _get(
        col_data, "SummarizeBy", "summarizeBy", default=None,
    )

    return {
        "name": name,
        "data_type": data_type,
        "cardinality": int(cardinality) if cardinality else 0,
        "total_size": int(total_size) if total_size else 0,
        "dictionary_size": int(dict_size) if dict_size else 0,
        "data_size": int(data_size) if data_size else 0,
        "min_value": str(min_val) if min_val is not None else None,
        "max_value": str(max_val) if max_val is not None else None,
        "is_hidden": bool(is_hidden),
        "is_calculated": col_type in ("Calculated", "calculated"),
        "encoding": str(encoding),
        # Semantic metadata
        "format_string": str(format_string) if format_string else None,
        "description": str(description) if description else None,
        "display_folder": str(display_folder) if display_folder else None,
        "sort_by_column": str(sort_by_column) if sort_by_column else None,
        "data_category": str(data_category) if data_category else None,
        "summarize_by": str(summarize_by) if summarize_by else None,
    }


def _parse_measure(measure_data):
    """Parse a single measure entry from the VPAX model."""
    name = _get(measure_data, "MeasureName", "Name", "name", "measureName", default="")
    if not name:
        return None

    expression = _get(
        measure_data,
        "Expression", "MeasureExpression", "expression",
        "measureExpression", "DaxExpression",
        default="",
    )

    return {
        "name": name,
        "expression": str(expression) if expression else "",
        "format_string": _get(measure_data, "FormatString", "formatString", default=None),
        "display_folder": _get(measure_data, "DisplayFolder", "displayFolder", default=None),
        "description": _get(measure_data, "Description", "description", default=None),
        "is_hidden": bool(_get(measure_data, "IsHidden", "isHidden", default=False)),
    }


def _parse_table(table_data):
    """Parse a single table entry from the VPAX model."""
    name = _get(table_data, "TableName", "Name", "name", "tableName", default="")
    if not name:
        return None

    row_count = _get(
        table_data,
        "RowsCount", "RowCount", "rowsCount", "rowCount", "Rows",
        default=0,
    )

    table_size = _get(
        table_data,
        "TableSize", "TotalSize", "tableSize", "totalSize", "Size",
        default=0,
    )

    columns_data = _get(table_data, "Columns", "columns", default=[])
    columns = []
    for col_data in columns_data:
        col = _parse_column(col_data)
        if col is not None:
            columns.append(col)

    # Parse measures
    measures_data = _get(table_data, "Measures", "measures", default=[])
    measures = []
    for m_data in measures_data:
        m = _parse_measure(m_data)
        if m is not None:
            measures.append(m)

    return {
        "name": name,
        "row_count": int(row_count) if row_count else 0,
        "table_size": int(table_size) if table_size else 0,
        "columns": columns,
        "measures": measures,
        "description": _get(table_data, "Description", "description", default=None),
        "is_hidden": bool(_get(table_data, "IsHidden", "isHidden", default=False)),
    }


def _find_tables(model_json):
    """Find the tables array in various VPAX JSON structures."""
    # Direct: {"Tables": [...]}
    tables = _get(model_json, "Tables", "tables")
    if tables:
        return tables

    # Wrapped: {"Model": {"Tables": [...]}}
    model = _get(model_json, "Model", "model")
    if model and isinstance(model, dict):
        tables = _get(model, "Tables", "tables")
        if tables:
            return tables

    # Nested ModelName: {"ModelName": {...}, "Tables": [...]}
    # Already handled by the first check

    return []


def _find_model_name(model_json):
    """Extract the model name from the VPAX JSON."""
    # {"ModelName": {"Name": "..."}}
    model_name = _get(model_json, "ModelName", "modelName")
    if isinstance(model_name, dict):
        return _get(model_name, "Name", "name", default="Model")
    if isinstance(model_name, str):
        return model_name

    # {"Model": {"Name": "..."}}
    model = _get(model_json, "Model", "model")
    if isinstance(model, dict):
        name = _get(model, "Name", "name", "ModelName")
        if isinstance(name, str):
            return name
        if isinstance(name, dict):
            return _get(name, "Name", "name", default="Model")

    return "Model"


def _parse_relationships(model_json):
    """Extract relationship data from the VPAX model JSON."""
    # Try top-level, then nested under Model
    rels_data = _get(model_json, "Relationships", "relationships")
    if not rels_data:
        model = _get(model_json, "Model", "model")
        if model and isinstance(model, dict):
            rels_data = _get(model, "Relationships", "relationships")

    if not rels_data:
        return []

    relationships = []
    for rel in rels_data:
        from_card = _get(rel, "FromCardinality", "fromCardinality", default="Many")
        to_card = _get(rel, "ToCardinality", "toCardinality", default="One")
        cross = _get(
            rel,
            "CrossFilteringBehavior", "crossFilteringBehavior",
            "CrossFilterDirection",
            default="OneDirection",
        )
        is_active = _get(rel, "IsActive", "isActive", default=True)
        sec = _get(
            rel,
            "SecurityFilteringBehavior", "securityFilteringBehavior",
            default="OneDirection",
        )

        relationships.append({
            "from_table": _get(rel, "FromTableName", "fromTableName", "FromTable", default=""),
            "from_column": _get(rel, "FromColumnName", "fromColumnName", "FromColumn", default=""),
            "to_table": _get(rel, "ToTableName", "toTableName", "ToTable", default=""),
            "to_column": _get(rel, "ToColumnName", "toColumnName", "ToColumn", default=""),
            "from_cardinality": str(from_card),
            "to_cardinality": str(to_card),
            "cross_filtering": str(cross),
            "is_active": bool(is_active) if is_active is not None else True,
            "security_filtering": str(sec),
        })
    return relationships


def parse_vpax(vpax_path):
    """Parse a .vpax file and extract model metadata.

    Args:
        vpax_path: Path to the .vpax file.

    Returns:
        Dict with model_name, tables, and relationships.

    Raises:
        ValueError: If no model JSON is found in the VPAX file.
    """
    vpax_path = Path(vpax_path)

    if not vpax_path.exists():
        raise FileNotFoundError(f"VPAX file not found: {vpax_path}")

    model_json = None

    with zipfile.ZipFile(vpax_path, "r") as zf:
        # Look for the model JSON file
        json_files = [
            n for n in zf.namelist()
            if n.lower().endswith(".json")
        ]

        # Prefer DaxModel.json
        for name in json_files:
            if "daxmodel" in name.lower() or "model" in name.lower():
                with zf.open(name) as f:
                    model_json = json.load(f)
                break

        # Fall back to any JSON file
        if model_json is None and json_files:
            with zf.open(json_files[0]) as f:
                model_json = json.load(f)

    if model_json is None:
        raise ValueError(
            f"No model JSON found in VPAX file: {vpax_path}. "
            f"Expected a DaxModel.json inside the ZIP archive."
        )

    model_name = _find_model_name(model_json)
    tables_data = _find_tables(model_json)

    tables = []
    for table_data in tables_data:
        table = _parse_table(table_data)
        if table is not None and table["columns"]:
            tables.append(table)

    relationships = _parse_relationships(model_json)

    return {
        "model_name": model_name,
        "tables": tables,
        "relationships": relationships,
    }
