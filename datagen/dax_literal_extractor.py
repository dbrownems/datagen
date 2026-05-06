"""Extract literal column values from a DAX query trace (JSONL).

The trace is expected to come from XEvents with at least the ``QueryEnd``
event class.  Each line is a JSON object with ``cols.TextData`` containing
the executed DAX text.

Only ``QueryEnd`` events are processed — VertiPaq SE events are ignored.

Public entry points
-------------------
extract_literals(jsonl_path, vpax_model=None)
    Returns a dict {(table, column) -> Counter[value]} where the table /
    column are mapped back to the canonical VPAX names when ``vpax_model``
    is provided.

detect_pattern(values, col_meta)
    Classify the observed values into one of: enum, prefixed_seq,
    templated_offset, numeric, freeform.  Returns hints used by the
    config-merge step.

merge_literals_into_config(config, literals, top_n=200, observed_share=0.7)
    Update ``ColumnConfig`` entries in place with fixed ``values`` lists
    (and key_style / prefix when relevant) so the generated data contains
    the observed literals as the most common values.
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Regex patterns over user DAX text
# ---------------------------------------------------------------------------

# 'tbl'[col] [NOT] IN { v1, v2, ... }
_P_IN = re.compile(
    r"'([^']+)'\[([^\]]+)\]\s+(?:NOT\s+)?IN\s*\{([^}]+)\}",
    re.IGNORECASE,
)
# TREATAS({ v1, v2, ... }, 'tbl'[col])
_P_TREATAS = re.compile(
    r"TREATAS\s*\(\s*\{([^}]+)\}\s*,\s*'([^']+)'\[([^\]]+)\]",
    re.IGNORECASE,
)
# 'tbl'[col] = "value"
_P_EQ_STR = re.compile(r"'([^']+)'\[([^\]]+)\]\s*=\s*\"([^\"]*)\"")
# 'tbl'[col] = 123 / 1.5 / -7
_P_EQ_NUM = re.compile(r"'([^']+)'\[([^\]]+)\]\s*=\s*(-?\d+(?:\.\d+)?)\b")

# Within an IN/TREATAS body: extract individual literals (string or number).
_P_LITERAL = re.compile(r"\"([^\"]*)\"|(-?\d+(?:\.\d+)?)")


# ---------------------------------------------------------------------------
# Name normalisation
# ---------------------------------------------------------------------------

_NAME_NORM_RE = re.compile(r"[\s_\-]+")


def _normalise(name: str) -> str:
    """Lowercase, collapse whitespace/dash/underscore.

    `'025-Parent Opportunity'` ≈ `'025 Parent Opportunity'` ≈
    `'025_parent_opportunity'`.
    """
    if not name:
        return ""
    return _NAME_NORM_RE.sub(" ", name).strip().lower()


def _build_vpax_lookup(vpax_model):
    """Build {(norm_table, norm_col) -> (canonical_table, canonical_col)}."""
    lookup = {}
    if not vpax_model:
        return lookup
    for tbl in vpax_model.get("tables", []):
        tname = tbl.get("name") or ""
        nt = _normalise(tname)
        for col in tbl.get("columns", []):
            cname = col.get("name") or ""
            nc = _normalise(cname)
            lookup[(nt, nc)] = (tname, cname)
    return lookup


# ---------------------------------------------------------------------------
# Trace ingest
# ---------------------------------------------------------------------------

def _record_literals(body: str, table: str, column: str, sink) -> None:
    """Parse literals from an IN/TREATAS body and add them to ``sink``."""
    for m in _P_LITERAL.finditer(body):
        v = m.group(1) if m.group(1) is not None else m.group(2)
        if v is None:
            continue
        sink[(table, column)][v] += 1


def iter_dax_texts_from_jsonl(path):
    """Yield DAX query text from each ``QueryEnd`` event in a JSONL trace.

    The trace is an XEvents-style capture where each line is a JSON object
    with at least ``eventClass == "QueryEnd"`` and ``cols.TextData`` set
    to the DAX query text. Other fields are ignored. Lines that don't
    match (wrong eventClass, missing TextData, malformed JSON) are
    silently skipped.
    """
    with Path(path).open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("eventClass") != "QueryEnd":
                continue
            text = (event.get("cols") or {}).get("TextData") or ""
            if text:
                yield text


def _record_text_literals(text, raw):
    for m in _P_IN.finditer(text):
        _record_literals(m.group(3), m.group(1), m.group(2), raw)
    for m in _P_TREATAS.finditer(text):
        _record_literals(m.group(1), m.group(2), m.group(3), raw)
    for m in _P_EQ_STR.finditer(text):
        raw[(m.group(1), m.group(2))][m.group(3)] += 1
    for m in _P_EQ_NUM.finditer(text):
        raw[(m.group(1), m.group(2))][m.group(3)] += 1


def extract_literals(dax_texts, vpax_model=None, *, max_events=None):
    """Return literal observations per column from an iterable of DAX queries.

    Args:
        dax_texts: Iterable of DAX query strings. Use
            :func:`iter_dax_texts_from_jsonl` to convert a JSONL trace
            into such an iterable.
        vpax_model: Optional parsed VPAX (from ``vpax_parser.parse_vpax``).
            When provided, observed (table, column) names are mapped back
            to canonical VPAX names; observations with no VPAX match are
            dropped.
        max_events: Optional cap on the number of DAX queries parsed.

    Returns:
        Dict ``{(table, column): Counter[value]}``.
    """
    raw: dict = defaultdict(Counter)
    n = 0
    for text in dax_texts:
        if not text:
            continue
        n += 1
        _record_text_literals(text, raw)
        if max_events and n >= max_events:
            break

    if not vpax_model:
        return dict(raw)

    # Map raw names → canonical VPAX names
    lookup = _build_vpax_lookup(vpax_model)
    mapped: dict = defaultdict(Counter)
    for (tbl, col), values in raw.items():
        key = lookup.get((_normalise(tbl), _normalise(col)))
        if key is None:
            continue
        mapped[key].update(values)
    return dict(mapped)


# ---------------------------------------------------------------------------
# Pattern detection
# ---------------------------------------------------------------------------

_PREFIX_RE = re.compile(r"^([A-Za-z][A-Za-z0-9_\-]*?)(\d+)$")
_OFFSET_RE = re.compile(r"^(.*?)([+-]\d+)?$")


def _is_numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


def detect_pattern(values: Counter, col_meta=None) -> dict:
    """Classify observed values to choose a generation strategy.

    Args:
        values: Counter of observed string values.
        col_meta: Optional ColumnConfig (or dict) — used to consult
            ``cardinality`` and ``data_type`` when set.

    Returns:
        Dict with keys:
            ``kind``: one of "enum" | "prefixed_seq" | "templated_offset"
                     | "numeric" | "freeform"
            extra fields per kind (see code).
    """
    distinct = list(values.keys())
    n_distinct = len(distinct)
    if n_distinct == 0:
        return {"kind": "freeform"}

    cardinality = None
    data_type = None
    if col_meta is not None:
        cardinality = getattr(col_meta, "cardinality", None) \
            or (col_meta.get("cardinality") if isinstance(col_meta, dict) else None)
        data_type = getattr(col_meta, "data_type", None) \
            or (col_meta.get("data_type") if isinstance(col_meta, dict) else None)

    # Numeric columns: every observed value parses as a number
    if all(_is_numeric(v) for v in distinct):
        return {"kind": "numeric"}

    # Templated offsets: e.g. "Current Quarter", "Current Quarter+1"
    bases = set()
    offsets = []
    all_match_offset = True
    for v in distinct:
        m = _OFFSET_RE.match(v)
        if not m:
            all_match_offset = False
            break
        base, off = m.group(1).strip(), m.group(2)
        bases.add(base)
        offsets.append(int(off) if off else 0)
    if all_match_offset and len(bases) == 1 and len(set(offsets)) >= 2:
        base = next(iter(bases))
        return {
            "kind": "templated_offset",
            "base": base,
            "min_offset": min(offsets),
            "max_offset": max(offsets),
        }

    # Prefixed sequence: values look like <prefix><digits>
    prefixes = []
    digit_widths = []
    for v in distinct:
        m = _PREFIX_RE.match(v)
        if not m:
            prefixes = []
            break
        prefixes.append(m.group(1))
        digit_widths.append(len(m.group(2)))
    if prefixes and len(set(prefixes)) == 1 and len(distinct) >= 2:
        return {
            "kind": "prefixed_seq",
            "prefix": prefixes[0],
            "avg_length": len(prefixes[0]) + (sum(digit_widths) // len(digit_widths)),
        }

    # Enum: small distinct set and (if cardinality is known) nearly the
    # whole column. Treat as a closed set of allowed values.
    if n_distinct <= 50:
        if cardinality is None or n_distinct >= max(1, int(0.5 * cardinality)):
            return {"kind": "enum"}

    return {"kind": "freeform"}


# ---------------------------------------------------------------------------
# Config merge
# ---------------------------------------------------------------------------

def _values_with_frequencies(observed: Counter, top_n: int, share: float):
    """Build the YAML-friendly ``values`` list.

    Each entry is ``{"value": v, "frequency": pct}`` with the budget
    ``share`` (0–1) split proportionally to occurrence counts and
    expressed as percentages (0–100).
    """
    top = observed.most_common(top_n)
    if not top:
        return []
    total = sum(c for _, c in top)
    budget_pct = max(0.0, min(1.0, share)) * 100.0
    out = []
    for v, c in top:
        out.append({"value": v, "frequency": round(budget_pct * c / total, 4)})
    return out


def _coerce_numeric(values_list, data_type):
    """Convert string values to numbers when the column is numeric."""
    if data_type not in ("int64", "double"):
        return values_list
    out = []
    for entry in values_list:
        v = entry["value"]
        try:
            if data_type == "int64":
                v = int(float(v))
            else:
                v = float(v)
            out.append({"value": v, "frequency": entry["frequency"]})
        except (TypeError, ValueError):
            continue
    return out


def merge_literals_into_config(config, literals, *, top_n=200,
                               observed_share=0.7, verbose=False):
    """Update ``GenerationConfig`` columns to include observed literals.

    Skips columns the user has already customised (already has a
    ``values`` list, or a non-default ``key_style`` set by the user).

    Args:
        config: GenerationConfig instance (mutated in place).
        literals: Output of ``extract_literals`` keyed by canonical names.
        top_n: Cap on observed values per column.
        observed_share: Share (0–1) of the column's value population that
            observed literals should occupy.  Default 0.7.
        verbose: Print per-column decisions.

    Returns:
        Number of columns updated.
    """
    n_updated = 0
    by_table = {tbl.name: tbl for tbl in config.tables}

    for (tname, cname), observed in literals.items():
        tbl = by_table.get(tname)
        if tbl is None:
            continue
        col = next((c for c in tbl.columns if c.name == cname), None)
        if col is None:
            continue

        # Don't overwrite user customisation
        if col.values:
            if verbose:
                print(f"[skip] {tname}.{cname} — already has values")
            continue

        pattern = detect_pattern(observed, col)
        kind = pattern["kind"]

        if kind == "prefixed_seq":
            # Switch the column to prefixed key generation, with observed
            # values as the high-frequency fixed entries.
            col.is_key = True
            col.key_style = "prefixed"
            col.distribution.prefix = pattern["prefix"]
            col.distribution.avg_length = max(
                col.distribution.avg_length or 0,
                pattern["avg_length"],
            )
            values_list = _values_with_frequencies(observed, top_n, observed_share)
            col.values = values_list
        elif kind == "templated_offset":
            base = pattern["base"]
            lo = min(pattern["min_offset"], -2)
            hi = max(pattern["max_offset"], 2)
            extra = []
            for off in range(lo, hi + 1):
                if off == 0:
                    candidate = base
                else:
                    candidate = f"{base}{off:+d}"
                if candidate not in observed:
                    extra.append(candidate)
            values_list = _values_with_frequencies(observed, top_n, observed_share)
            for v in extra:
                values_list.append(v)  # plain entry: equal share of remaining
            col.values = values_list
        elif kind == "numeric":
            values_list = _values_with_frequencies(observed, top_n, observed_share)
            col.values = _coerce_numeric(values_list, col.data_type)
        else:
            # enum or freeform: just seed with observed values
            values_list = _values_with_frequencies(observed, top_n, observed_share)
            if col.data_type in ("int64", "double"):
                values_list = _coerce_numeric(values_list, col.data_type)
            col.values = values_list

        # Bump cardinality so the pool can hold all the fixed values
        n_obs = len(observed)
        if col.cardinality < n_obs:
            col.cardinality = n_obs

        n_updated += 1
        if verbose:
            print(f"[seed] {tname}.{cname}  kind={kind}  "
                  f"distinct={n_obs}  card={col.cardinality}")

    return n_updated


# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------

def seed_config_from_trace(config, dax_texts, vpax_model=None, **kwargs):
    """Extract literals from an iterable of DAX queries and merge into ``config``.

    Returns the number of columns updated.
    """
    literals = extract_literals(dax_texts, vpax_model=vpax_model)
    return merge_literals_into_config(config, literals, **kwargs)
