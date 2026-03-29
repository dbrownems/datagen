"""Run a DAX query against the Fabric XMLA endpoint and test rewriting.

Finds a query with duration >= 1000ms, warms the cache, then tests
the rewritten query with generated values.
"""
import json
import re
import time
from pythonnet import load
load("coreclr")
import clr

dll = r"C:\Users\david\AppData\Local\Temp\adomd2\lib\net8.0\Microsoft.AnalysisServices.AdomdClient.dll"
clr.AddReference(dll)
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand

XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld"
DATASET = "Sales Leader Dataset - DL"
MIN_DURATION_MS = 1000


def get_connection():
    with open(".token", "r") as f:
        token = f.read().strip()
    conn_str = f"Data Source={XMLA_ENDPOINT};Initial Catalog={DATASET};password={token};Timeout=7200;"
    conn = AdomdConnection(conn_str)
    conn.Open()
    return conn


def run_dax(dax_text, conn):
    clean = re.sub(r'\s*\[WaitTime:.*?\]\s*$', '', dax_text).strip()
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


def run_dax_warm(dax_text, conn):
    """Run once to warm cache, then run again for timing."""
    run_dax(dax_text, conn)  # warmup (cold cache)
    return run_dax(dax_text, conn)  # timed (warm cache)


def get_distinct_values(table, column, conn):
    dax = f"EVALUATE VALUES('{table}'[{column}])"
    rows, _ = run_dax(dax, conn)
    return [list(r.values())[0] for r in rows if list(r.values())[0] is not None]


# --- Main ---

with open("queries.json", "r", encoding="utf-8") as f:
    queries = json.load(f)

from datagen.dax_rewriter import extract_filter_bindings, rewrite_query
import numpy as np

# Find a query >= 1000ms with filter bindings
candidates = []
for i, q in enumerate(queries):
    text = q.get("[EventText]", "")
    dur = int(q.get("[Duration (ms)]", "0"))
    bindings = extract_filter_bindings(text)
    # Skip truncated queries (must end with valid DAX — closing paren or keyword)
    clean = re.sub(r'\s*\[WaitTime:.*?\]\s*$', '', text).strip()
    if not clean or clean[-1] not in (')', '"', ']', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'):
        continue
    if dur >= MIN_DURATION_MS and 2 <= len(bindings) <= 8 and len(text) < 5000:
        candidates.append((i, dur, bindings, text))

print(f"Found {len(candidates)} queries >= {MIN_DURATION_MS}ms with bindings")
if not candidates:
    exit()

conn = get_connection()
print(f"Connected: {conn.Database}")
print()

# Find a query that actually runs
print("Searching for a valid query ...")
selected = None
for idx, (i, original_dur, bindings, original_dax) in enumerate(candidates[:20]):
    try:
        rows, dur = run_dax(original_dax, conn)
        print(f"  Query #{i}: {original_dur}ms recorded, {dur:.0f}ms cold, {len(rows)} rows ✓")
        selected = (i, original_dur, bindings, original_dax)
        break
    except Exception:
        print(f"  Query #{i}: failed (truncated?), skipping")

if not selected:
    print("No valid queries found!")
    conn.Close()
    exit()

i, original_dur, bindings, original_dax = selected
print(f"\nSelected query #{i}: {original_dur}ms, {len(bindings)} bindings")
for b in bindings:
    print(f"  {b['pattern']:10s} '{b['table']}'[{b['column']}] = {b['values'][:2]}")
print()

# Run original with warm cache
print("=== Original query (warm cache) ===")
try:
    rows, dur = run_dax_warm(original_dax, conn)
    print(f"  {len(rows)} rows, {dur:.0f}ms (recorded: {original_dur}ms)")
except Exception as e:
    print(f"  Error: {str(e)[:300]}")
print()

# Get replacement values
print("=== Replacement values ===")
value_map = {}
for b in bindings:
    key = (b["table"], b["column"])
    if key in value_map:
        continue
    try:
        vals = get_distinct_values(b["table"], b["column"], conn)
        value_map[key] = vals
        print(f"  '{b['table']}'[{b['column']}]: {len(vals)} values")
    except Exception as e:
        print(f"  '{b['table']}'[{b['column']}]: error")
print()

# Rewrite and test
rng = np.random.default_rng(42)
rewritten, n = rewrite_query(original_dax, value_map, rng)
print(f"=== Rewritten query ({n} replacements, warm cache) ===")

rewritten_bindings = extract_filter_bindings(rewritten)
for b in rewritten_bindings:
    print(f"  '{b['table']}'[{b['column']}] = {b['values'][:3]}")
print()

try:
    rows, dur = run_dax_warm(rewritten, conn)
    ratio = dur / original_dur if original_dur > 0 else 0
    print(f"  {len(rows)} rows, {dur:.0f}ms (target: {original_dur}ms, ratio: {ratio:.2f}x)")

    if not rows:
        print("  ⚠ No rows — trying correlated values ...")
        anchor = next((b for b in bindings if (b["table"], b["column"]) in value_map
                       and value_map[(b["table"], b["column"])]), None)
        if anchor:
            akey = (anchor["table"], anchor["column"])
            aval = value_map[akey][0]
            print(f"  Anchor: '{anchor['table']}'[{anchor['column']}] = '{aval}'")
            for b in bindings:
                key = (b["table"], b["column"])
                if key == akey:
                    continue
                try:
                    dax_check = (
                        f"EVALUATE CALCULATETABLE("
                        f"VALUES('{b['table']}'[{b['column']}]), "
                        f"'{anchor['table']}'[{anchor['column']}] = \"{aval}\")"
                    )
                    cr, _ = run_dax(dax_check, conn)
                    cv = [list(r.values())[0] for r in cr if list(r.values())[0]]
                    if cv:
                        value_map[key] = cv
                        print(f"    '{b['table']}'[{b['column']}]: {len(cv)} correlated")
                except Exception:
                    pass

            rng2 = np.random.default_rng(42)
            rewritten2, _ = rewrite_query(original_dax, value_map, rng2)
            rows2, dur2 = run_dax_warm(rewritten2, conn)
            ratio2 = dur2 / original_dur if original_dur > 0 else 0
            print(f"  Correlated: {len(rows2)} rows, {dur2:.0f}ms (ratio: {ratio2:.2f}x)")
    else:
        print("  ✓ Query returns data!")
except Exception as e:
    print(f"  Error: {str(e)[:300]}")

conn.Close()
print("\nDone.")
