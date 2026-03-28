"""Configuration data models and YAML serialization for datagen."""

from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path

import yaml


@dataclass
class DistributionConfig:
    """Parameters for generating the pool of unique values."""

    # Numeric distributions
    mean: Optional[float] = None
    std_dev: Optional[float] = None
    skewness: Optional[float] = 0.0
    min: Optional[float] = None
    max: Optional[float] = None

    # String distributions
    avg_length: Optional[int] = None
    style: Optional[str] = "docker"  # "docker" | "hex" | "alpha"
    prefix: Optional[str] = None  # For prefixed key style

    # Date distributions
    start: Optional[str] = None  # ISO date string
    end: Optional[str] = None  # ISO date string

    # Boolean distributions
    true_ratio: Optional[float] = 0.5


@dataclass
class ColumnConfig:
    """Configuration for a single column."""

    name: str
    data_type: str  # "string", "int64", "double", "datetime", "boolean"
    cardinality: int = 100

    # Value selection distribution
    selection: str = "zipf"  # "uniform" | "zipf"
    zipf_exponent: float = 1.0

    # Key column detection
    is_key: bool = False
    key_style: Optional[str] = None  # "sequential" | "prefixed" | "guid"

    # Nullability
    nullable: bool = False
    null_ratio: float = 0.0

    # Distribution parameters for the value pool
    distribution: DistributionConfig = field(default_factory=DistributionConfig)


@dataclass
class TableConfig:
    """Configuration for a single table."""

    name: str
    row_count: int = 1000
    columns: list = field(default_factory=list)  # List[ColumnConfig]


@dataclass
class GenerationConfig:
    """Top-level configuration for data generation."""

    model_name: str = "Model"
    output_path: str = "Tables/"
    seed: int = 42
    tables: list = field(default_factory=list)  # List[TableConfig]


def _clean_dist(dist_dict, data_type):
    """Keep only distribution fields relevant to the data type."""
    if not isinstance(dist_dict, dict):
        return dist_dict

    # Fields relevant to each data type
    relevant = {
        "int64": {"mean", "std_dev", "skewness", "min", "max"},
        "double": {"mean", "std_dev", "skewness", "min", "max"},
        "datetime": {"start", "end"},
        "boolean": {"true_ratio"},
        "string": {"avg_length", "style", "prefix"},
    }

    keep = relevant.get(data_type, set())
    return {k: v for k, v in dist_dict.items() if k in keep and v is not None}


def _clean_dict(d, parent_key=None):
    """Remove None values, defaults, and empty sub-dicts for cleaner YAML."""
    if not isinstance(d, dict):
        return d

    cleaned = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, dict):
            v = _clean_dict(v, parent_key=k)
            if not v:
                continue
        if isinstance(v, list):
            v = [_clean_dict(item) if isinstance(item, dict) else item for item in v]
        cleaned[k] = v
    return cleaned


def _clean_column(col_dict):
    """Clean a column dict: filter distribution and remove zero-value defaults."""
    data_type = col_dict.get("data_type", "string")
    key_style = col_dict.get("key_style")

    # For key columns, override which distribution fields are relevant
    if key_style == "sequential":
        keep = {"min", "max"}
    elif key_style == "prefixed":
        keep = {"avg_length", "prefix"}
    elif key_style == "guid":
        keep = {"avg_length"}
    else:
        keep = None  # use default data-type logic

    if "distribution" in col_dict:
        if keep is not None:
            col_dict["distribution"] = {
                k: v for k, v in col_dict["distribution"].items()
                if k in keep and v is not None
            }
        else:
            col_dict["distribution"] = _clean_dist(col_dict["distribution"], data_type)
        if not col_dict["distribution"]:
            col_dict.pop("distribution", None)

    # Remove zero/false/None defaults that clutter YAML
    defaults_to_strip = {
        "nullable": False,
        "null_ratio": 0.0,
        "is_key": False,
        "key_style": None,
    }
    for key, default in defaults_to_strip.items():
        if col_dict.get(key) == default:
            col_dict.pop(key, None)

    return col_dict


def config_to_dict(config):
    """Convert a GenerationConfig to a plain dict (for YAML serialization)."""
    d = asdict(config)
    d = _clean_dict(d)

    # Apply per-column cleaning
    for table in d.get("tables", []):
        table["columns"] = [_clean_column(col) for col in table.get("columns", [])]

    return d


def dict_to_config(d):
    """Convert a plain dict (from YAML) to a GenerationConfig."""
    tables = []
    for table_data in d.get("tables", []):
        columns = []
        for col_data in table_data.get("columns", []):
            dist_data = col_data.pop("distribution", {})
            dist = DistributionConfig(**dist_data) if dist_data else DistributionConfig()
            col = ColumnConfig(distribution=dist, **col_data)
            columns.append(col)
        table_data["columns"] = columns
        tables.append(TableConfig(**table_data))

    return GenerationConfig(
        model_name=d.get("model_name", "Model"),
        output_path=d.get("output_path", "Tables/"),
        seed=d.get("seed", 42),
        tables=tables,
    )


def save_config(config, path):
    """Save a GenerationConfig to a YAML file.

    Args:
        config: GenerationConfig instance or dict.
        path: Output file path.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(config, GenerationConfig):
        data = config_to_dict(config)
    else:
        data = config

    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(
            data,
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=120,
        )


def load_config(path):
    """Load a GenerationConfig from a YAML file.

    Args:
        path: Path to the YAML config file.

    Returns:
        GenerationConfig instance.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return dict_to_config(data)
