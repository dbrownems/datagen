"""Generate a datagen YAML config from parsed VPAX model metadata.

Infers distribution parameters from column statistics (cardinality, size,
min/max values) and produces a tweakable configuration file.  Includes
heuristics to detect key columns and GUID columns.
"""

import math
from datetime import datetime, timedelta

from .config import (
    GenerationConfig,
    TableConfig,
    ColumnConfig,
    DistributionConfig,
    save_config,
)


# ---------------------------------------------------------------------------
# Key / GUID detection heuristics
# ---------------------------------------------------------------------------

_KEY_SUFFIXES = ("key", "id", "sk", "bk", "ak", "code", "number", "num", "no")
_KEY_EXACT = {"id", "key", "sku", "code"}


def _is_key_name(col_name):
    """Check if column name suggests it's a key."""
    lower = col_name.lower()
    if lower in _KEY_EXACT:
        return True
    return any(lower.endswith(s) for s in _KEY_SUFFIXES)


def _is_guid_column(col_name, avg_length):
    """Detect if a string column likely stores GUIDs.

    Checks column name and average string length (GUIDs are 32-38 chars).
    """
    lower = col_name.lower()
    if any(g in lower for g in ("guid", "uuid", "uniqueid", "unique_id")):
        return True
    # Length heuristic: UUID with hyphens=36, without=32, with braces=38
    if avg_length is not None and 30 <= avg_length <= 40:
        return True
    return False


def _derive_prefix(col_name):
    """Derive a short string prefix from a column name.

    Examples: ProductKey → PROD, CustomerID → CUST, SKU → SKU
    """
    name = col_name
    for suffix in ("Key", "ID", "Id", "Code", "Number", "Num", "No",
                    "key", "id", "code", "number", "num", "no",
                    "SK", "BK", "AK", "sk", "bk", "ak"):
        if name.endswith(suffix) and len(name) > len(suffix):
            name = name[: -len(suffix)]
            break
    # Strip trailing underscores / hyphens after stripping suffix
    name = name.rstrip("_-")
    return name[:4].upper() if len(name) >= 2 else col_name[:4].upper()


def _detect_key(col_meta, row_count, relationship_columns):
    """Score a column to decide if it's a key, and which style to use.

    Returns (is_key, key_style)  where key_style is one of
    "sequential" | "prefixed" | "guid" | None.
    """
    name = col_meta["name"]
    data_type = col_meta["data_type"]
    cardinality = col_meta.get("cardinality", 0)

    score = 0
    if _is_key_name(name):
        score += 3
    if name in relationship_columns:
        score += 3
    if row_count > 0 and cardinality > 0:
        ratio = cardinality / row_count
        if ratio > 0.9:
            score += 2
        elif ratio > 0.5:
            score += 1

    if score < 3:
        return False, None

    # Determine key style
    if data_type == "int64":
        return True, "sequential"
    if data_type == "string":
        avg_len = _infer_string_avg_length(col_meta)
        if _is_guid_column(name, avg_len):
            return True, "guid"
        return True, "prefixed"
    # Other types (double, datetime) are unusual as keys — mark but no style
    return True, None


# ---------------------------------------------------------------------------
# Geography column detection
# ---------------------------------------------------------------------------

_GEO_PATTERNS = {
    "country": ["country", "countryname", "countryregion", "nation"],
    "state": ["state", "statename", "province", "provincename",
              "stateorprovince", "stateprovince", "region"],
    "city": ["city", "cityname", "town", "municipality"],
    "postal_code": ["postalcode", "postal", "zipcode", "zip", "postcode"],
}


def _detect_geo_type(col_meta):
    """Detect if a column is a geography column by name or data category.

    Returns one of "country", "state", "city", "postal_code", or None.
    """
    name = col_meta.get("name", "")
    lower = name.lower().replace("_", "").replace(" ", "").replace("-", "")

    # Check data category first (from VPAX metadata)
    data_cat = (col_meta.get("data_category") or "").lower().strip()
    cat_map = {
        "country": "country", "countryregion": "country",
        "stateorprovince": "state", "state": "state", "province": "state",
        "city": "city",
        "postalcode": "postal_code", "postal code": "postal_code",
    }
    if data_cat in cat_map:
        return cat_map[data_cat]

    # Fall back to name matching
    for geo_type, patterns in _GEO_PATTERNS.items():
        for pattern in patterns:
            if lower == pattern or lower.endswith(pattern):
                return geo_type

    return None


def _parse_numeric(val_str, data_type):
    """Parse a string value into a numeric type."""
    if val_str is None:
        return None
    try:
        val_str = str(val_str).strip()
        if data_type == "int64":
            return int(float(val_str))
        return float(val_str)
    except (ValueError, TypeError):
        return None


def _parse_date(val_str):
    """Parse a date string into ISO format."""
    if val_str is None:
        return None
    val_str = str(val_str).strip()
    # Try common date formats
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(val_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _infer_string_avg_length(col_meta):
    """Estimate average string length from VPAX column statistics."""
    dict_size = col_meta.get("dictionary_size", 0)
    cardinality = col_meta.get("cardinality", 1)

    if dict_size > 0 and cardinality > 0:
        # Dictionary stores unique values; rough estimate of avg value size
        # Account for encoding overhead (~2 bytes per entry for metadata)
        raw_per_value = max(1, dict_size / cardinality - 2)
        # UTF-16 encoding in Analysis Services: ~2 bytes per character
        avg_length = max(3, int(raw_per_value / 2))
        return min(avg_length, 200)  # cap at reasonable length

    return 12  # default


def _infer_numeric_distribution(col_meta, data_type):
    """Infer mean, std_dev, skewness, min, max from VPAX column stats."""
    min_val = _parse_numeric(col_meta.get("min_value"), data_type)
    max_val = _parse_numeric(col_meta.get("max_value"), data_type)

    dist = DistributionConfig()

    if min_val is not None:
        dist.min = min_val
    if max_val is not None:
        dist.max = max_val

    if min_val is not None and max_val is not None:
        dist.mean = (min_val + max_val) / 2.0
        # Assume ~99.7% of values within [min, max] → range ≈ 6σ
        value_range = max_val - min_val
        dist.std_dev = value_range / 6.0 if value_range > 0 else 1.0

        # Default to slight positive skew for financial/count data
        if min_val >= 0:
            dist.skewness = 1.0
        else:
            dist.skewness = 0.0
    else:
        dist.mean = 0.0
        dist.std_dev = 1.0
        dist.skewness = 0.0

    # Round for cleaner YAML
    if dist.mean is not None:
        dist.mean = round(dist.mean, 4)
    if dist.std_dev is not None:
        dist.std_dev = round(dist.std_dev, 4)

    return dist


def _infer_date_distribution(col_meta):
    """Infer date range from VPAX column stats.

    Dates are anchored so the range ends at Jan 1 of the current year.
    If VPAX provides min/max, the original span is preserved but shifted.
    """
    year_start = f"{datetime.now().year}-01-01"

    raw_start = _parse_date(col_meta.get("min_value"))
    raw_end = _parse_date(col_meta.get("max_value"))

    dist = DistributionConfig()

    if raw_start and raw_end:
        # Preserve the original span, shift to end at current year start
        start_dt = datetime.strptime(raw_start, "%Y-%m-%d")
        end_dt = datetime.strptime(raw_end, "%Y-%m-%d")
        span = end_dt - start_dt
        anchor = datetime.strptime(year_start, "%Y-%m-%d")
        dist.end = year_start
        dist.start = (anchor - span).strftime("%Y-%m-%d")
    else:
        dist.end = year_start
        dist.start = f"{datetime.now().year - 3}-01-01"

    return dist


def _infer_boolean_distribution(col_meta):
    """Infer boolean distribution (default 50/50)."""
    dist = DistributionConfig()
    dist.true_ratio = 0.5  # No way to infer from VPAX
    return dist


def _infer_selection(cardinality, row_count):
    """Choose value selection strategy based on cardinality ratio."""
    if cardinality <= 2:
        return "uniform", 1.0
    if row_count > 0:
        ratio = cardinality / row_count
        if ratio > 0.8:
            # High cardinality relative to rows → mostly unique
            return "uniform", 1.0
        elif ratio > 0.1:
            return "zipf", 1.1
        else:
            # Low cardinality → some values much more common
            return "zipf", 1.3
    return "zipf", 1.0


def _infer_column_config(col_meta, row_count, relationship_columns=None, table_name=""):
    """Infer a ColumnConfig from VPAX column metadata."""
    name = col_meta["name"]
    data_type = col_meta["data_type"]
    cardinality = max(1, col_meta.get("cardinality", 1))

    # Cap cardinality at row_count
    if row_count > 0:
        cardinality = min(cardinality, row_count)

    rel_cols = relationship_columns or set()

    # Key detection
    is_key, key_style = _detect_key(col_meta, row_count, rel_cols)

    if is_key and key_style:
        # Primary keys (cardinality ≈ row_count) → uniform
        # Foreign keys (cardinality << row_count) → use normal selection inference
        is_primary = row_count > 0 and cardinality / row_count > 0.9
        if is_primary:
            selection, zipf_exp = "uniform", 1.0
        else:
            selection, zipf_exp = _infer_selection(cardinality, row_count)

        if key_style == "sequential":
            dist = DistributionConfig(min=1, max=float(cardinality))
            return ColumnConfig(
                name=name, data_type=data_type, cardinality=cardinality,
                selection=selection, zipf_exponent=zipf_exp,
                is_key=True, key_style=key_style, distribution=dist,
            )

        if key_style == "prefixed":
            prefix = _derive_prefix(name)
            width = len(str(cardinality))
            avg_length = len(prefix) + 1 + width  # "PROD-00001"
            dist = DistributionConfig(avg_length=avg_length, prefix=prefix)
            return ColumnConfig(
                name=name, data_type=data_type, cardinality=cardinality,
                selection=selection, zipf_exponent=zipf_exp,
                is_key=True, key_style=key_style, distribution=dist,
            )

        if key_style == "guid":
            dist = DistributionConfig(avg_length=36)
            return ColumnConfig(
                name=name, data_type=data_type, cardinality=cardinality,
                selection=selection, zipf_exponent=zipf_exp,
                is_key=True, key_style=key_style, distribution=dist,
            )

    # Non-key column: infer distribution normally
    selection, zipf_exp = _infer_selection(cardinality, row_count)

    # Detect geography columns by name or data category
    geo_type = _detect_geo_type(col_meta)
    if geo_type and data_type == "string":
        dist = DistributionConfig(style="geo", geo_type=geo_type)
        return ColumnConfig(
            name=name, data_type=data_type, cardinality=cardinality,
            selection=selection, zipf_exponent=round(zipf_exp, 2),
            distribution=dist,
        )

    # Detect email/username columns
    from .email_generator import is_email_column
    if is_email_column(name) and data_type == "string":
        dist = DistributionConfig(style="email")
        return ColumnConfig(
            name=name, data_type=data_type, cardinality=cardinality,
            selection=selection, zipf_exponent=round(zipf_exp, 2),
            distribution=dist,
        )

    # Infer distribution based on data type
    if data_type in ("int64", "double"):
        dist = _infer_numeric_distribution(col_meta, data_type)
    elif data_type == "datetime":
        dist = _infer_date_distribution(col_meta)
    elif data_type == "boolean":
        dist = _infer_boolean_distribution(col_meta)
        selection = "uniform"
    elif data_type == "string":
        avg_length = _infer_string_avg_length(col_meta)
        dist = DistributionConfig(avg_length=avg_length, style="docker")
    else:
        dist = DistributionConfig(avg_length=12, style="docker")

    return ColumnConfig(
        name=name,
        data_type=data_type,
        cardinality=cardinality,
        selection=selection,
        zipf_exponent=round(zipf_exp, 2),
        nullable=False,
        null_ratio=0.0,
        distribution=dist,
    )


def generate_config(
    vpax_model,
    output_path="Tables/",
    seed=42,
    include_hidden=False,
    include_calculated=False,
):
    """Generate a GenerationConfig from parsed VPAX model metadata.

    Args:
        vpax_model: Dict from vpax_parser.parse_vpax().
        output_path: Default output path for Delta tables.
        seed: Random seed for reproducibility.
        include_hidden: Whether to include hidden columns.
        include_calculated: Whether to include calculated columns.

    Returns:
        GenerationConfig instance.
    """
    model_name = vpax_model.get("model_name", "Model")
    tables = []

    # Build set of column names that participate in relationships
    relationship_columns = set()
    for rel in vpax_model.get("relationships", []):
        for key in ("from_column", "to_column"):
            col = rel.get(key, "")
            if col:
                relationship_columns.add(col)

    for table_meta in vpax_model.get("tables", []):
        table_name = table_meta["name"]
        row_count = table_meta.get("row_count", 1000)

        columns = []
        for col_meta in table_meta.get("columns", []):
            # Skip hidden/calculated columns unless requested
            if col_meta.get("is_hidden", False) and not include_hidden:
                continue
            if col_meta.get("is_calculated", False) and not include_calculated:
                continue

            col_config = _infer_column_config(col_meta, row_count, relationship_columns, table_name)
            columns.append(col_config)

        if columns:
            tables.append(TableConfig(
                name=table_name,
                row_count=row_count,
                columns=columns,
            ))

    return GenerationConfig(
        model_name=model_name,
        output_path=output_path,
        seed=seed,
        tables=tables,
    )


def vpax_to_config(vpax_path, config_path, **kwargs):
    """One-step: parse VPAX and save config YAML.

    Args:
        vpax_path: Path to the .vpax file.
        config_path: Output path for the YAML config.
        **kwargs: Additional arguments for generate_config().
    """
    from .vpax_parser import parse_vpax

    model = parse_vpax(vpax_path)
    config = generate_config(model, **kwargs)
    save_config(config, config_path)
    return config
