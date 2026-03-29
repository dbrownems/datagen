"""Convert all queries, then benchmark top 10 most expensive with/without RLS.

Usage: python scripts/convert_and_benchmark.py
"""
import json
import re
import time
import numpy as np
from pythonnet import load
load("coreclr")
import clr

dll = r"C:\Users\david\AppData\Local\Temp\adomd2\lib\net8.0\Microsoft.AnalysisServices.AdomdClient.dll"
clr.AddReference(dll)
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand
from datagen.dax_rewriter import extract_filter_bindings

XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld"
DATASET = "Sales Leader Dataset"

RLS_USERS = {
    "RLS Access - Geo": ("031-RLS Access Table - Geo", "EMAIL"),
    "RLS Access - Global": ("032-RLS Access Table - Global", "EMAIL"),
    "RLS Access - Other": ("023-RLS Access Table", "EMAIL"),
}


def get_token():
    with open(".token", "r") as f:
        return f.read().strip()


def get_conn(email=None, role=None):
    token = get_token()
    parts = [f"Data Source={XMLA_ENDPOINT}", f"Initial Catalog={DATASET}",
             f"password={token}", "Timeout=7200"]
    if email and role:
        parts += [f"CustomData={email}", f"Roles={role}"]
    conn = AdomdConnection(";".join(parts) + ";")
    conn.Open()
    return conn


def run_dax(dax, conn):
    clean = re.sub(r'\s*\[WaitTime:.*?\]\s*$', '', dax).strip()
    t0 = time.perf_counter()
    cmd = AdomdCommand(clean, conn)
    reader = cmd.ExecuteReader()
    cols = [reader.GetName(i) for i in range(reader.FieldCount)]
    rows = []
    while reader.Read():
        rows.append({cols[i]: str(reader.GetValue(i)) if reader.GetValue(i) is not None else None
                     for i in range(reader.FieldCount)})
    reader.Close()
    return rows, (time.perf_counter() - t0) * 1000


def get_top_values(table, column, conn, n=20):
    dax = (f"EVALUATE TOPN({n}, ADDCOLUMNS(VALUES('{table}'[{column}]), "
           f"\"Cnt\", CALCULATE(COUNTROWS('{table}'))), [Cnt], DESC)")
    rows, _ = run_dax(dax, conn)
    return [str(list(r.values())[0]) for r in rows if list(r.values())[0] is not None]


def format_val(val, is_numeric=False):
    if is_numeric:
        return str(val)
    try:
        float(val)
        return str(val)
    except (ValueError, TypeError):
        return f'"{val}"'


def rewrite_top_values(dax_text, value_map, skip_tables=None):
    bindings = extract_filter_bindings(dax_text)
    if not bindings:
        return dax_text, 0
    bindings.sort(key=lambda b: b["span"][0], reverse=True)
    result = dax_text
    n = 0
    for b in bindings:
        if skip_tables and b["table"] in skip_tables:
            continue
        key = (b["table"], b["column"])
        if key not in value_map or not value_map[key]:
            continue
        top = value_map[key][:len(b["values"])]
        if not top:
            continue
        is_num = b.get("is_numeric", False)
        if b["pattern"] in ("treatas", "in"):
            new_vals = ", ".join(str(v) if is_num else format_val(v) for v in top)
            s, e = b["span"]
            result = result[:s] + new_vals + result[e:]
            n += 1
        elif b["pattern"] == "compare":
            s, e = b["span"]
            result = result[:s] + top[0] + result[e:]
            n += 1
    return result, n


# ── Load queries ─────────────────────────────────────────────────────────────
with open("queries.json", "r", encoding="utf-8") as f:
    queries = json.load(f)
print(f"Loaded {len(queries)} queries")

conn = get_conn()
print(f"Connected: {conn.Database}")

# ── Get enter-data tables to skip ────────────────────────────────────────────
from datagen.model_builder import _extract_bim, _is_enter_data_table
import glob
vpax_files = glob.glob("**/*.vpax", recursive=True)
skip_tables = set()
if vpax_files:
    try:
        bim = _extract_bim(vpax_files[0])
        model = bim.get("model", bim)
        skip_tables = {t["name"] for t in model.get("tables", []) if _is_enter_data_table(t)}
        print(f"Skipping {len(skip_tables)} enter-data tables")
    except Exception:
        print("No Model.bim in local VPAX — skipping enter-data detection")

# ── Build value map ──────────────────────────────────────────────────────────
all_cols = set()
for q in queries:
    text = q.get("[EventText]", "")
    if text:
        for b in extract_filter_bindings(text):
            all_cols.add((b["table"], b["column"]))

print(f"Loading top values for {len(all_cols)} columns ...")
value_map = {}
for table, column in sorted(all_cols):
    try:
        vals = get_top_values(table, column, conn, n=20)
        if vals:
            value_map[(table, column)] = vals
    except Exception:
        pass
print(f"  {len(value_map)}/{len(all_cols)} loaded")

# ── Find best RLS user per role ──────────────────────────────────────────────
print("Finding RLS users ...")
rls_best = {}
for role, (tname, col) in RLS_USERS.items():
    try:
        rows, _ = run_dax(
            f"EVALUATE TOPN(1, ADDCOLUMNS(VALUES('{tname}'[{col}]), "
            f"\"Cnt\", CALCULATE(COUNTROWS('{tname}'))), [Cnt], DESC)", conn)
        if rows:
            email = str(list(rows[0].values())[0])
            rls_best[role] = email
            print(f"  {role}: {email}")
    except Exception:
        pass

conn.Close()

# ── Convert all queries ──────────────────────────────────────────────────────
print(f"\nConverting {len(queries)} queries ...")
rls_entries = list(rls_best.items())  # [(role, email), ...]
converted = []
n_rewritten = 0

for i, q in enumerate(queries):
    new_q = dict(q)
    text = q.get("[EventText]", "")
    if text:
        new_text, n = rewrite_top_values(text, value_map, skip_tables)
        new_q["[EventText]"] = new_text
        if n > 0:
            n_rewritten += 1
    if rls_entries:
        role, email = rls_entries[i % len(rls_entries)]
        new_q["[User Name]"] = email
        new_q["[Role]"] = role
    converted.append(new_q)

with open("queries_converted.json", "w", encoding="utf-8") as f:
    json.dump(converted, f, indent=2, ensure_ascii=False)
print(f"✓ Converted → queries_converted.json ({n_rewritten} rewritten)")

# ── Benchmark top 10 most expensive ──────────────────────────────────────────
print(f"\n{'='*70}")
print("BENCHMARK: Top 10 most expensive queries")
print(f"{'='*70}")

# Find top 10 by recorded duration that are valid
top_candidates = sorted(
    [(i, int(q.get("[Duration (ms)]", "0")), q) for i, q in enumerate(converted)
     if int(q.get("[Duration (ms)]", "0")) > 0],
    key=lambda x: -x[1]
)

# Validate — find 10 that actually run
top10 = []
conn_noRLS = get_conn()
for idx, dur, q in top_candidates:
    if len(top10) >= 10:
        break
    text = q["[EventText]"]
    try:
        run_dax(text, conn_noRLS)  # just validate
        top10.append((idx, dur, q))
    except Exception:
        continue
conn_noRLS.Close()

print(f"Selected {len(top10)} valid queries (recorded {top10[0][1]}-{top10[-1][1]}ms)")

# Run each: no RLS, then with each RLS user
contexts = [("No RLS", None, None)]
for role, email in rls_best.items():
    contexts.append((f"{role} ({email[:20]}...)", email, role))

print(f"\n{'Query':>6} {'Recorded':>10} ", end="")
for label, _, _ in contexts:
    print(f"{'|':>3} {label[:25]:>25}", end="")
print()
print("-" * (20 + 28 * len(contexts)))

for qi, (idx, recorded, q) in enumerate(top10):
    text = q["[EventText]"]
    print(f"  Q{qi+1:>3} {recorded:>8}ms ", end="", flush=True)

    for label, email, role in contexts:
        try:
            c = get_conn(email, role)
            run_dax(text, c)  # warm
            rows, dur = run_dax(text, c)
            c.Close()
            print(f"| {len(rows):>4}r {dur:>6.0f}ms", end="", flush=True)
        except Exception as e:
            print(f"|     ERROR    ", end="", flush=True)

    print()

print(f"\n{'='*70}")
print("Done.")
