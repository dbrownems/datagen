"""Run all converted queries and compare durations with observed values.

Connects with CustomData/Roles for RLS, warms cache per template,
then reports duration comparison statistics.
"""
import json
import re
import time
import hashlib
from pythonnet import load
load("coreclr")
import clr

dll = r"C:\Users\david\AppData\Local\Temp\adomd2\lib\net8.0\Microsoft.AnalysisServices.AdomdClient.dll"
clr.AddReference(dll)
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand

XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld"
DATASET = "Sales Leader Dataset - DL"


def get_token():
    with open(".token", "r") as f:
        return f.read().strip()


def get_connection(email=None, role=None):
    token = get_token()
    parts = [
        f"Data Source={XMLA_ENDPOINT}",
        f"Initial Catalog={DATASET}",
        f"password={token}",
        "Timeout=7200",
    ]
    if email and role:
        parts.append(f"CustomData={email}")
        parts.append(f"Roles={role}")
    conn = AdomdConnection(";".join(parts) + ";")
    conn.Open()
    return conn


def run_dax(dax, conn):
    clean = re.sub(r'\s*\[WaitTime:.*?\]\s*$', '', dax).strip()
    t0 = time.perf_counter()
    cmd = AdomdCommand(clean, conn)
    reader = cmd.ExecuteReader()
    n_rows = 0
    while reader.Read():
        n_rows += 1
    reader.Close()
    return n_rows, (time.perf_counter() - t0) * 1000


def get_template_key(dax):
    key = re.sub(r'"[^"]*"', '"???"', dax)
    key = re.sub(r'(?<=\{)\s*[\d.,\s-]+\s*(?=\})', '???', key)
    return hashlib.md5(key.encode()).hexdigest()


# ── Load queries ─────────────────────────────────────────────────────────────
with open("queries_converted.json", "r", encoding="utf-8") as f:
    queries = json.load(f)
print(f"Loaded {len(queries)} converted queries")

# ── Group by (role, email) to minimize reconnections ─────────────────────────
from collections import defaultdict
by_context = defaultdict(list)
for i, q in enumerate(queries):
    ctx = (q.get("[User Name]", ""), q.get("[Role]", ""))
    by_context[ctx].append((i, q))

print(f"Connection contexts: {len(by_context)}")

# ── Run all queries ──────────────────────────────────────────────────────────
results = [None] * len(queries)  # (n_rows, actual_ms, recorded_ms, error)
warmed_templates = set()

total_run = 0
total_error = 0
total_empty = 0
t_start = time.time()

for ctx_idx, ((email, role), group) in enumerate(by_context.items()):
    # Connect with RLS context
    try:
        if email and role:
            conn = get_connection(email, role)
        else:
            conn = get_connection()
    except Exception as e:
        print(f"  ⚠ Connection failed for {role}/{email}: {str(e)[:100]}")
        for i, q in group:
            recorded = int(q.get("[Duration (ms)]", "0"))
            results[i] = (0, 0, recorded, "connection_failed")
            total_error += 1
        continue

    print(f"  Context {ctx_idx+1}/{len(by_context)}: {role} / {email[:30]}... ({len(group)} queries)")

    for gi, (i, q) in enumerate(group):
        text = q.get("[EventText]", "")
        recorded = int(q.get("[Duration (ms)]", "0"))

        if not text:
            results[i] = (0, 0, recorded, "no_text")
            total_error += 1
            continue

        tkey = get_template_key(text)

        # Warm cache once per template
        if tkey not in warmed_templates:
            try:
                run_dax(text, conn)
                warmed_templates.add(tkey)
            except Exception:
                pass

        # Timed run
        try:
            n_rows, actual_ms = run_dax(text, conn)
            results[i] = (n_rows, actual_ms, recorded, None)
            total_run += 1
            if n_rows == 0:
                total_empty += 1
        except Exception as e:
            results[i] = (0, 0, recorded, str(e)[:80])
            total_error += 1

        if (gi + 1) % 500 == 0:
            elapsed = time.time() - t_start
            rate = total_run / elapsed if elapsed > 0 else 0
            print(f"    {gi+1}/{len(group)} queries, {rate:.1f} q/s", flush=True)

    conn.Close()

elapsed_total = time.time() - t_start
print(f"\nCompleted in {elapsed_total:.0f}s ({total_run/elapsed_total:.1f} q/s)")
print(f"  Ran: {total_run}, Errors: {total_error}, Empty: {total_empty}")

# ── Analyze results ──────────────────────────────────────────────────────────
import numpy as np

recorded_ms = []
actual_ms = []
ratios = []
rows_list = []

for r in results:
    if r is None or r[3] is not None:
        continue
    rec = r[2]
    act = r[1]
    if rec > 0:
        recorded_ms.append(rec)
        actual_ms.append(act)
        ratios.append(act / rec)
        rows_list.append(r[0])

recorded_arr = np.array(recorded_ms)
actual_arr = np.array(actual_ms)
ratio_arr = np.array(ratios)
rows_arr = np.array(rows_list)

print(f"\n{'='*60}")
print(f"Duration Comparison ({len(recorded_arr)} queries with recorded > 0)")
print(f"{'='*60}")
print(f"  Recorded (original):  mean={recorded_arr.mean():.0f}ms, median={np.median(recorded_arr):.0f}ms, p95={np.percentile(recorded_arr, 95):.0f}ms")
print(f"  Actual (generated):   mean={actual_arr.mean():.0f}ms, median={np.median(actual_arr):.0f}ms, p95={np.percentile(actual_arr, 95):.0f}ms")
print(f"  Ratio (actual/rec):   mean={ratio_arr.mean():.2f}x, median={np.median(ratio_arr):.2f}x, p95={np.percentile(ratio_arr, 95):.2f}x")
print(f"  Rows returned:        mean={rows_arr.mean():.0f}, median={np.median(rows_arr):.0f}, zero={np.sum(rows_arr == 0)}")

# Bucket analysis
print(f"\n  By recorded duration bucket:")
for lo, hi in [(0, 100), (100, 500), (500, 1000), (1000, 5000), (5000, 999999)]:
    mask = (recorded_arr >= lo) & (recorded_arr < hi)
    n = mask.sum()
    if n == 0:
        continue
    label = f"{lo}-{hi}ms" if hi < 999999 else f"{lo}ms+"
    r_mean = ratio_arr[mask].mean()
    a_mean = actual_arr[mask].mean()
    empty = (rows_arr[mask] == 0).sum()
    print(f"    {label:>10s}: {n:>5d} queries, ratio={r_mean:.2f}x, actual_mean={a_mean:.0f}ms, empty={empty}")

# Save detailed results
detail = []
for i, r in enumerate(results):
    if r is None:
        continue
    detail.append({
        "query_index": i,
        "recorded_ms": r[2],
        "actual_ms": round(r[1], 1),
        "rows": r[0],
        "ratio": round(r[1] / r[2], 3) if r[2] > 0 else None,
        "error": r[3],
    })

with open("benchmark_results.json", "w") as f:
    json.dump(detail, f, indent=2)
print(f"\nDetailed results saved to benchmark_results.json")
