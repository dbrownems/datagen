"""Auto-align column data types across relationships for Direct Lake.

Direct Lake refuses any relationship whose FromColumn / ToColumn data
types differ. When that happens, ``model_builder`` silently drops the
relationship — almost always not what the user wants.

This module runs *before* table generation. It walks every relationship
from the parsed VPAX, inspects the corresponding column ``data_type``
values in the loaded ``GenerationConfig``, and where they disagree it
applies a **PK-wins** policy: the FK side (``from_column``, the "many"
side) is converted to match the PK side (``to_column``, the "one"
side). PK-wins minimises cascading changes since each PK is typically
referenced by many FKs but each FK references only one PK.

The mutation is done in memory on the config so the generated Delta
tables match what the BIM will expect at deploy time (the Spark schema
is built from ``ColumnConfig.data_type`` in ``spark_generator``).

For each conflict we log:

* the YAML entry we applied (the FK-side override that makes it match
  the PK), and
* the alternative YAML entry the user could paste in if they want the
  opposite conversion (FK wins — change the PK side instead).

Pass ``auto_fix=False`` to disable and just log the mismatches.
"""

from __future__ import annotations


def _find_column(config, table_name, column_name):
    for t in config.tables:
        if t.name == table_name:
            for c in t.columns:
                if c.name == column_name:
                    return t, c
            return t, None
    return None, None


def _yaml_override_snippet(table_name, column_name, new_type):
    return (
        f"        tables:\n"
        f"          - name: {table_name}\n"
        f"            columns:\n"
        f"              - name: {column_name}\n"
        f"                data_type: {new_type}"
    )


def fix_relationship_types(config, vpax_model, *, auto_fix=True, log=print):
    """Walk relationships, log type mismatches, optionally apply PK-wins.

    PK-wins policy: for each mismatched relationship, the FK side
    (``from_column``) is changed to the PK side's (``to_column``) type.

    Returns a list of dicts describing each mismatch (whether or not it
    was fixed) so callers can summarise / test.
    """
    rels = (vpax_model or {}).get("relationships", []) or []
    mismatches = []

    for rel in rels:
        if not rel.get("is_active", True):
            continue
        ft, fc_name = rel.get("from_table"), rel.get("from_column")
        tt, tc_name = rel.get("to_table"), rel.get("to_column")
        if not (ft and fc_name and tt and tc_name):
            continue
        _, fc = _find_column(config, ft, fc_name)
        _, tc = _find_column(config, tt, tc_name)
        if fc is None or tc is None:
            # Column may have been pruned from the config (hidden, calc, etc.) —
            # nothing we can do here, BIM still owns its declared type.
            continue
        from_type = (fc.data_type or "").lower()
        to_type = (tc.data_type or "").lower()
        if from_type == to_type:
            continue

        # PK-wins: FK side (from_*) is changed to match PK side (to_*).
        mismatches.append({
            "from_table": ft, "from_column": fc_name, "from_type": from_type,
            "to_table": tt, "to_column": tc_name, "to_type": to_type,
            "applied_to_table": ft, "applied_to_column": fc_name,
            "applied_type": to_type,
            "alt_to_table": tt, "alt_to_column": tc_name,
            "alt_type": from_type,
        })

        if auto_fix:
            fc.data_type = to_type

    if not mismatches:
        return mismatches

    action = "Auto-aligned" if auto_fix else "Detected"
    log("")
    log(f"  ⚠ {action} {len(mismatches)} relationship type mismatch(es). "
        f"Direct Lake requires both sides to share a data_type.")
    if auto_fix:
        log(f"    Default policy: PK WINS — the FK side (many) is converted to")
        log(f"    match the PK side (one). Override in the YAML to take control.")
    log("")

    for i, m in enumerate(mismatches, 1):
        log(f"  [{i}] FK {m['from_table']}[{m['from_column']}] ({m['from_type']})  "
            f"→  PK {m['to_table']}[{m['to_column']}] ({m['to_type']})")
        if auto_fix:
            log(f"      Applied (PK wins, FK → {m['applied_type']}):")
            log(_yaml_override_snippet(
                m['applied_to_table'], m['applied_to_column'], m['applied_type']))
            log(f"      For the opposite (FK wins, change PK → {m['alt_type']} "
                f"instead — may cascade to other FKs of this PK), add to the YAML:")
            log(_yaml_override_snippet(
                m['alt_to_table'], m['alt_to_column'], m['alt_type']))
        log("")

    return mismatches
