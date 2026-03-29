"""Investigate a single >1000ms query to understand how filter values affect runtime."""
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
from datagen.dax_rewriter import extract_filter_bindings, rewrite_query

XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld"
DATASET = "Sales Leader Dataset - DL"


def get_connection():
    with open(".token", "r") as f:
        token = f.read().strip()
    conn = AdomdConnection(
        f"Data Source={XMLA_ENDPOINT};Initial Catalog={DATASET};password={token};Timeout=7200;"
    )
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
        row = {}
        for i, c in enumerate(cols):
            val = reader.GetValue(i)
            row[c] = str(val) if val is not None else None
        rows.append(row)
    reader.Close()
    return rows, (time.perf_counter() - t0) * 1000


def run_warm(dax, conn):
    run_dax(dax, conn)
    return run_dax(dax, conn)


# ── Find a good >1000ms query ────────────────────────────────────────────────
with open("queries.json", "r", encoding="utf-8") as f:
    queries = json.load(f)

conn = get_connection()
print(f"Connected: {conn.Database}\n")

# Find valid >1000ms queries
print("Searching for a valid >1000ms query that returns many rows ...")
selected = None
for q in queries:
    text = q.get("[EventText]", "")
    dur = int(q.get("[Duration (ms)]", "0"))
    bindings = extract_filter_bindings(text)
    if dur < 1000 or len(bindings) < 2 or len(text) > 8000:
        continue
    try:
        rows, d = run_dax(text, conn)
        if len(rows) > 10:  # want multi-row results
            selected = (q, text, dur, bindings)
            print(f"  Found: {dur}ms, {len(bindings)} bindings, {len(rows)} rows (cold={d:.0f}ms)")
            break
    except Exception:
        continue

if not selected:
    print("No valid query found!")
    conn.Close()
    exit()

q, original_text, target_ms, bindings = selected

# ── Analyze the bindings ─────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"Target runtime: {target_ms}ms")
print(f"Bindings:")
for b in bindings:
    print(f"  {b['pattern']:10s} '{b['table']}'[{b['column']}] = {b['values'][:3]}")

# ── Get available values and their row counts ────────────────────────────────
print(f"\n{'='*60}")
print("Analyzing filter selectivity ...")

value_map = {}
value_counts = {}  # (table, col) → {value: row_count}

for b in bindings:
    key = (b["table"], b["column"])
    if key in value_map:
        continue

    # Get all distinct values
    try:
        vals_rows = run_dax(f"EVALUATE VALUES('{b['table']}'[{b['column']}])", conn)[0]
        vals = [list(r.values())[0] for r in vals_rows if list(r.values())[0] is not None]
        value_map[key] = vals
    except Exception:
        continue

    # Get row counts per value (how selective is each value?)
    try:
        count_dax = (
            f"EVALUATE ADDCOLUMNS("
            f"VALUES('{b['table']}'[{b['column']}]), "
            f"\"Rows\", CALCULATE(COUNTROWS('{b['table']}')))"
        )
        count_rows = run_dax(count_dax, conn)[0]
        counts = {}
        for r in count_rows:
            v = list(r.values())
            if v[0] is not None:
                counts[str(v[0])] = int(float(v[1])) if v[1] else 0
        value_counts[key] = counts
        
        # Show distribution
        sorted_counts = sorted(counts.items(), key=lambda x: -x[1])
        total = sum(counts.values())
        print(f"\n  '{b['table']}'[{b['column']}]: {len(vals)} values, {total:,} total rows")
        print(f"    Most selective (fewest rows):")
        for v, c in sorted_counts[-3:]:
            print(f"      \"{v}\": {c:,} rows ({c/total*100:.1f}%)")
        print(f"    Least selective (most rows):")
        for v, c in sorted_counts[:3]:
            print(f"      \"{v}\": {c:,} rows ({c/total*100:.1f}%)")
    except Exception as e:
        print(f"  '{b['table']}'[{b['column']}]: error getting counts: {str(e)[:100]}")

# ── Strategy 1: Random replacement (baseline) ───────────────────────────────
print(f"\n{'='*60}")
print("Strategy 1: Random values (baseline)")
rng = np.random.default_rng(42)
rewritten, n = rewrite_query(original_text, value_map, rng)
rows, dur = run_warm(rewritten, conn)
print(f"  {len(rows)} rows, {dur:.0f}ms (target: {target_ms}ms, ratio: {dur/target_ms:.2f}x)")

# ── Strategy 2: Pick high-cardinality values (more rows → slower) ───────────
print(f"\n{'='*60}")
print("Strategy 2: High-row-count values (least selective)")

high_card_map = {}
for key, counts in value_counts.items():
    sorted_vals = sorted(counts.items(), key=lambda x: -x[1])
    high_card_map[key] = [v for v, c in sorted_vals[:20]]

if high_card_map:
    rng2 = np.random.default_rng(42)
    rewritten2, n2 = rewrite_query(original_text, high_card_map, rng2)
    rows2, dur2 = run_warm(rewritten2, conn)
    print(f"  {len(rows2)} rows, {dur2:.0f}ms (target: {target_ms}ms, ratio: {dur2/target_ms:.2f}x)")

# ── Strategy 3: Pick low-cardinality values (fewer rows → faster) ───────────
print(f"\n{'='*60}")
print("Strategy 3: Low-row-count values (most selective)")

low_card_map = {}
for key, counts in value_counts.items():
    sorted_vals = sorted(counts.items(), key=lambda x: x[1])
    # Skip zeros
    non_zero = [(v, c) for v, c in sorted_vals if c > 0]
    low_card_map[key] = [v for v, c in non_zero[:20]]

if low_card_map:
    rng3 = np.random.default_rng(42)
    rewritten3, n3 = rewrite_query(original_text, low_card_map, rng3)
    rows3, dur3 = run_warm(rewritten3, conn)
    print(f"  {len(rows3)} rows, {dur3:.0f}ms (target: {target_ms}ms, ratio: {dur3/target_ms:.2f}x)")

# ── Strategy 4: Binary search by selectivity ────────────────────────────────
print(f"\n{'='*60}")
print("Strategy 4: Binary search — vary number of IN values")

# For IN-pattern bindings, try different counts of values
in_bindings = [b for b in bindings if b["pattern"] == "in"]
if in_bindings:
    b = in_bindings[0]
    key = (b["table"], b["column"])
    if key in value_counts:
        sorted_vals = sorted(value_counts[key].items(), key=lambda x: -x[1])
        all_vals = [v for v, c in sorted_vals]
        
        # Try 1, 2, 4, 8, ... values
        for n_vals in [1, 2, 4, len(all_vals)//2, len(all_vals)]:
            if n_vals > len(all_vals):
                continue
            test_map = dict(value_map)
            test_map[key] = all_vals[:n_vals]
            rng_t = np.random.default_rng(42)
            rewritten_t, _ = rewrite_query(original_text, test_map, rng_t)
            rows_t, dur_t = run_warm(rewritten_t, conn)
            ratio_t = dur_t / target_ms if target_ms > 0 else 0
            print(f"  {n_vals} values: {len(rows_t)} rows, {dur_t:.0f}ms (ratio: {ratio_t:.2f}x)")

conn.Close()
print(f"\n{'='*60}")
print("Done.")
