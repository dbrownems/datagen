"""Histograms sidecar file: ``<config>.histograms.yaml``.

Hand-edited histograms in the main YAML are lost when the main YAML is
regenerated from the .vpax. This module keeps an optional sidecar file
that preserves histograms across regenerations.

The sidecar lives next to the main config (typically the lakehouse
``Files/datagen/`` folder) and is named after the .vpax/main-yaml stem,
e.g. ``my_model.vpax`` → ``my_model.yaml`` → ``my_model.histograms.yaml``.

Format::

    # my_model.histograms.yaml
    tables:
      Fact_Sales:
        - values:
            ProductCode: "X"
          rows: 1000000
        - values:
            ProductCode: "Y"
          rows: 0.5
      Fact_Returns:
        - values: {Region: "EU"}
          rows: 200000

When ``generate()`` runs, it:

* Loads the sidecar if present.
* Replaces ``TableConfig.histogram`` for every table mentioned in the
  sidecar (per-table override; sidecar wins).
* Logs how many tables were patched.
"""

from pathlib import Path

import yaml

from .config import HistogramEntry, GenerationConfig


def sidecar_path_for(config_path):
    """Return the sidecar path that pairs with a given main-config or
    .vpax path. ``model.vpax`` / ``model.yaml`` → ``model.histograms.yaml``.
    """
    p = Path(config_path)
    return str(p.with_suffix("").with_suffix(".histograms.yaml"))


def load_histograms_sidecar(path):
    """Load a histograms sidecar.

    Returns a dict ``{table_name: [HistogramEntry, ...]}`` (empty if
    the file doesn't exist).
    """
    p = Path(path)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    tables = data.get("tables", {})
    if not isinstance(tables, dict):
        raise ValueError(
            f"Histogram sidecar {p} has invalid 'tables': "
            f"expected mapping, got {type(tables).__name__}"
        )

    result = {}
    for tname, entries in tables.items():
        if entries is None:
            continue
        if not isinstance(entries, list):
            raise ValueError(
                f"Histogram sidecar {p}: tables['{tname}'] must be a list, "
                f"got {type(entries).__name__}"
            )
        parsed = []
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError(
                    f"Histogram sidecar {p}: every entry under "
                    f"tables['{tname}'] must be a mapping with 'values' "
                    f"and 'rows'"
                )
            parsed.append(HistogramEntry(
                values=dict(entry.get("values", {})),
                rows=entry.get("rows", 0),
            ))
        result[tname] = parsed
    return result


def apply_histograms_to_config(config, sidecar_map):
    """Overlay histograms from a sidecar map onto a ``GenerationConfig``.

    Per-table override: if ``sidecar_map`` mentions a table, that table's
    histogram is replaced. Tables not mentioned in the sidecar keep
    whatever histogram (if any) was already on them.

    Returns the count of tables patched. Tables in the sidecar that
    aren't in the config are reported via ``unknown_tables`` (logged
    by the caller).
    """
    if not sidecar_map:
        return 0, []
    by_name = {t.name: t for t in config.tables}
    patched = 0
    unknown = []
    for tname, entries in sidecar_map.items():
        tbl = by_name.get(tname)
        if tbl is None:
            unknown.append(tname)
            continue
        tbl.histogram = list(entries)
        patched += 1
    return patched, unknown


def extract_histograms_from_config(config):
    """Return a sidecar-shaped dict from a config (only tables with
    non-empty histograms are included). Useful for migrating an
    existing hand-edited main YAML to the sidecar."""
    out = {}
    tables = config.tables if isinstance(config, GenerationConfig) else config.get("tables", [])
    for tbl in tables:
        hist = getattr(tbl, "histogram", None)
        if not hist:
            continue
        entries = []
        for h in hist:
            if isinstance(h, HistogramEntry):
                entries.append({"values": dict(h.values), "rows": h.rows})
            else:
                entries.append({
                    "values": dict(h.get("values", {})),
                    "rows": h.get("rows", 0),
                })
        out[tbl.name if hasattr(tbl, "name") else tbl["name"]] = entries
    return out


def save_histograms_sidecar(sidecar_map, path):
    """Write a histograms sidecar to ``path``.

    ``sidecar_map`` is either:

    * a plain dict ``{table_name: [{values, rows}, ...]}`` — written as-is.
    * a dict ``{table_name: [HistogramEntry, ...]}`` — converted.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    out_tables = {}
    for tname, entries in (sidecar_map or {}).items():
        rendered = []
        for e in entries:
            if isinstance(e, HistogramEntry):
                rendered.append({"values": dict(e.values), "rows": e.rows})
            elif isinstance(e, dict):
                rendered.append({
                    "values": dict(e.get("values", {})),
                    "rows": e.get("rows", 0),
                })
            else:
                raise TypeError(
                    f"save_histograms_sidecar: unsupported entry type "
                    f"{type(e).__name__} for table {tname!r}"
                )
        out_tables[tname] = rendered

    payload = {"tables": out_tables}
    with open(p, "w", encoding="utf-8") as f:
        yaml.dump(
            payload, f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=120,
        )
