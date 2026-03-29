"""Apply USERPRINCIPALNAME→CUSTOMDATA RLS changes to the import model and test queries."""
import json
import re
import time
import numpy as np
from pythonnet import load
load("coreclr")
import clr

# Load ADOMD and AMO
dll = r"C:\Users\david\AppData\Local\Temp\adomd2\lib\net8.0\Microsoft.AnalysisServices.AdomdClient.dll"
clr.AddReference(dll)
amo_dir = r"C:\Users\david\AppData\Local\Temp\amo\lib\net8.0"
import os
for f in os.listdir(amo_dir):
    if f.endswith(".dll"):
        try:
            clr.AddReference(os.path.join(amo_dir, f))
        except Exception:
            pass

from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand

XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld"
DATASET = "Sales Leader Dataset"


def get_token():
    with open(".token", "r") as f:
        return f.read().strip()


def get_adomd(email=None, role=None):
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
    cols = [reader.GetName(i) for i in range(reader.FieldCount)]
    rows = []
    while reader.Read():
        row = {cols[i]: str(reader.GetValue(i)) if reader.GetValue(i) is not None else None
               for i in range(reader.FieldCount)}
        rows.append(row)
    reader.Close()
    return rows, (time.perf_counter() - t0) * 1000


def run_dmv(query, conn):
    cmd = AdomdCommand(query, conn)
    reader = cmd.ExecuteReader()
    cols = [reader.GetName(i) for i in range(reader.FieldCount)]
    rows = []
    while reader.Read():
        row = {cols[i]: str(reader.GetValue(i)) if reader.GetValue(i) is not None else None
               for i in range(reader.FieldCount)}
        rows.append(row)
    reader.Close()
    return rows


# ── Step 1: Apply RLS changes via TOM ────────────────────────────────────────
print(f"Connecting to {DATASET} ...")
from Microsoft.AnalysisServices.Tabular import Server as TabularServer

token = get_token()
server = TabularServer()
server.Connect(f"Data Source={XMLA_ENDPOINT};password={token};Timeout=7200;")
db = server.Databases.GetByName(DATASET)
model = db.Model

n_modified = 0
for role in model.Roles:
    for tp in role.TablePermissions:
        original = tp.FilterExpression
        if not original:
            continue
        modified = re.sub(r'USERPRINCIPALNAME\s*\(\s*\)', 'CUSTOMDATA()', original, flags=re.IGNORECASE)
        modified = re.sub(r'USERNAME\s*\(\s*\)', 'CUSTOMDATA()', modified, flags=re.IGNORECASE)
        if modified != original:
            tp.FilterExpression = modified
            print(f"  [{role.Name}] {tp.Table.Name}: USERPRINCIPALNAME → CUSTOMDATA")
            n_modified += 1

if n_modified:
    print(f"Saving {n_modified} RLS changes ...")
    model.SaveChanges()
    print("  ✓ Saved")
else:
    print("  No changes needed (already using CUSTOMDATA)")

server.Disconnect()

# ── Step 2: Verify RLS works ─────────────────────────────────────────────────
print("\n=== Verifying RLS ===")
conn = get_adomd()

# Get roles
roles = run_dmv("SELECT * FROM $SYSTEM.TMSCHEMA_ROLES", conn)
print(f"Roles: {[r['Name'] for r in roles]}")

# Get a test user from the Geo RLS table
geo_users, _ = run_dax("EVALUATE TOPN(3, VALUES('031-RLS Access Table - Geo'[EMAIL]))", conn)
emails = [list(r.values())[0] for r in geo_users if list(r.values())[0]]
conn.Close()

if emails:
    test_email = emails[0]  # pass as-is (lowercase) — RLS does LOWER(CUSTOMDATA())
    test_role = "RLS Access - Geo"
    print(f"Test user: {test_email}, Role: {test_role}")

    rls_conn = get_adomd(test_email, test_role)
    rows_rls, _ = run_dax("EVALUATE ROW(\"Territories\", COUNTROWS('008-Territory'))", rls_conn)
    no_rls_conn = get_adomd()
    rows_no, _ = run_dax("EVALUATE ROW(\"Territories\", COUNTROWS('008-Territory'))", no_rls_conn)
    
    rls_count = list(rows_rls[0].values())[0] if rows_rls else "?"
    no_rls_count = list(rows_no[0].values())[0] if rows_no else "?"
    print(f"  Territories with RLS: {rls_count}")
    print(f"  Territories without:  {no_rls_count}")
    no_rls_conn.Close()
else:
    test_email = None
    test_role = None
    rls_conn = get_adomd()
    print("  No RLS users found, testing without RLS")

# ── Step 3: Test queries ─────────────────────────────────────────────────────
print("\n=== Testing queries ===")
from datagen.dax_rewriter import extract_filter_bindings, rewrite_query

with open("queries.json", "r", encoding="utf-8") as f:
    queries = json.load(f)

# Build value map from model
print("Loading replacement values ...")
all_cols = set()
for q in queries[:500]:
    text = q.get("[EventText]", "")
    if text:
        for b in extract_filter_bindings(text):
            all_cols.add((b["table"], b["column"]))

value_map = {}
for table, column in sorted(all_cols):
    try:
        vals, _ = run_dax(f"EVALUATE VALUES('{table}'[{column}])", rls_conn)
        v = [list(r.values())[0] for r in vals if list(r.values())[0] is not None]
        if v:
            value_map[(table, column)] = v
    except Exception:
        pass
print(f"  {len(value_map)} columns loaded")

# Get enter-data tables to skip
from datagen.model_builder import get_enter_data_tables
import glob
vpax_files = glob.glob("/lakehouse/default/Files/datagen/*.vpax") or glob.glob("**/*.vpax", recursive=True)
skip_tables = set()
if vpax_files:
    try:
        skip_tables = get_enter_data_tables(vpax_files[0])
        print(f"  {len(skip_tables)} enter-data tables to skip")
    except Exception:
        pass

# Find and test 5 valid queries > 500ms that return rows with ORIGINAL values
print("\nRunning test queries ...")
rng = np.random.default_rng(42)
n_tested = 0
for q in queries:
    if n_tested >= 5:
        break
    text = q.get("[EventText]", "")
    dur = int(q.get("[Duration (ms)]", "0"))
    bindings = extract_filter_bindings(text)
    if dur < 200 or len(bindings) < 2:
        continue

    # Try running original first — skip if it errors
    try:
        orig_rows, orig_dur = run_dax(text, rls_conn)
    except Exception:
        continue

    # Rewrite
    rewritten, n = rewrite_query(text, value_map, rng, skip_tables=skip_tables)
    if n == 0:
        continue

    # Warm + timed
    try:
        run_dax(rewritten, rls_conn)
        rows, actual_dur = run_dax(rewritten, rls_conn)
    except Exception as e:
        print(f"  Query (recorded={dur}ms): REWRITE ERROR — {str(e)[:100]}")
        n_tested += 1
        continue

    ratio = actual_dur / dur if dur > 0 else 0
    print(f"  Query (recorded={dur}ms): orig={len(orig_rows)} rows/{orig_dur:.0f}ms → rewritten={len(rows)} rows/{actual_dur:.0f}ms (ratio={ratio:.2f}x)")
    n_tested += 1

rls_conn.Close()
print("\nDone.")
