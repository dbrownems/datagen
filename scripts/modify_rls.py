"""Modify RLS rules to use CUSTOMDATA() instead of USERPRINCIPALNAME(), then test."""
import json
import re
import time
from pythonnet import load
load("coreclr")
import clr

dll = r"C:\Users\david\AppData\Local\Temp\adomd2\lib\net8.0\Microsoft.AnalysisServices.AdomdClient.dll"
clr.AddReference(dll)
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand

# Also need AMO for XMLA ALTER
amo_dir = r"C:\Users\david\AppData\Local\Temp\amo\lib\net8.0"
import os
for f in os.listdir(amo_dir):
    if f.endswith(".dll"):
        try:
            clr.AddReference(os.path.join(amo_dir, f))
        except Exception:
            pass

XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld"
DATASET = "Sales Leader Dataset - DL"


def get_token():
    with open(".token", "r") as f:
        return f.read().strip()


def get_adomd_connection():
    token = get_token()
    conn = AdomdConnection(
        f"Data Source={XMLA_ENDPOINT};Initial Catalog={DATASET};password={token};Timeout=7200;"
    )
    conn.Open()
    return conn


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


# ── Step 1: Read current RLS rules ──────────────────────────────────────────
conn = get_adomd_connection()
print(f"Connected: {conn.Database}\n")

perms = run_dmv(
    "SELECT * FROM $SYSTEM.TMSCHEMA_TABLE_PERMISSIONS",
    conn,
)
roles = run_dmv("SELECT * FROM $SYSTEM.TMSCHEMA_ROLES", conn)
role_map = {r["ID"]: r["Name"] for r in roles}

print("Current RLS filter expressions:")
for p in perms:
    expr = p.get("FilterExpression", "")
    if expr:
        role_name = role_map.get(p["RoleID"], p["RoleID"])
        print(f"  [{role_name}] {expr[:120]}")
print()
conn.Close()

# ── Step 2: Modify RLS via TOM (XMLA write) ─────────────────────────────────
print("Modifying RLS rules via TOM ...")

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
        # Replace USERPRINCIPALNAME() and USERNAME() with CUSTOMDATA()
        modified = re.sub(
            r'USERPRINCIPALNAME\s*\(\s*\)',
            'CUSTOMDATA()',
            original,
            flags=re.IGNORECASE,
        )
        modified = re.sub(
            r'USERNAME\s*\(\s*\)',
            'CUSTOMDATA()',
            modified,
            flags=re.IGNORECASE,
        )
        if modified != original:
            tp.FilterExpression = modified
            print(f"  [{role.Name}] {tp.Table.Name}")
            print(f"    Before: {original[:100]}")
            print(f"    After:  {modified[:100]}")
            n_modified += 1

if n_modified:
    print(f"\nSaving {n_modified} changes ...")
    model.SaveChanges()
    print("  ✓ Saved")
else:
    print("  No changes needed")

server.Disconnect()

# ── Step 3: Test with CustomData and Roles ───────────────────────────────────
print("\n=== Testing RLS with CustomData ===")

# Pick a user from the Geo RLS table
conn_noRLS = get_adomd_connection()
geo_users = run_dax(
    "EVALUATE TOPN(5, VALUES('031-RLS Access Table - Geo'[EMAIL]))",
    conn_noRLS,
)[0]
emails = [list(r.values())[0] for r in geo_users if list(r.values())[0]]
conn_noRLS.Close()

if not emails:
    print("No emails found!")
    exit()

test_email = emails[0].upper()  # RLS uses UPPER()
test_role = "RLS Access - Geo"
print(f"  User: {test_email}")
print(f"  Role: {test_role}")

# Connect with CustomData and Roles
token = get_token()
rls_conn_str = (
    f"Data Source={XMLA_ENDPOINT};"
    f"Initial Catalog={DATASET};"
    f"password={token};"
    f"Timeout=7200;"
    f"CustomData={test_email};"
    f"Roles={test_role};"
)
print(f"  Connecting with RLS ...")
rls_conn = AdomdConnection(rls_conn_str)
rls_conn.Open()
print(f"  ✓ Connected")

# Test: count rows in a filtered table
rows_rls, dur_rls = run_dax(
    "EVALUATE ROW(\"Territories\", COUNTROWS('008-Territory'))",
    rls_conn,
)
print(f"  Territories (with RLS): {rows_rls}")

# Compare without RLS
conn2 = get_adomd_connection()
rows_no, dur_no = run_dax(
    "EVALUATE ROW(\"Territories\", COUNTROWS('008-Territory'))",
    conn2,
)
print(f"  Territories (no RLS):   {rows_no}")
conn2.Close()

# Now run a real query with RLS
print(f"\n=== Running the 1766ms query with RLS ===")
with open("queries.json", "r", encoding="utf-8") as f:
    queries = json.load(f)

from datagen.dax_rewriter import extract_filter_bindings, rewrite_query
import numpy as np

# Find the same query we investigated before
for q in queries:
    dur = int(q.get("[Duration (ms)]", "0"))
    text = q.get("[EventText]", "")
    bindings = extract_filter_bindings(text)
    if dur >= 1000 and len(bindings) >= 2 and len(text) < 8000:
        try:
            test_rows, _ = run_dax(text, rls_conn)
            if len(test_rows) > 5:
                print(f"  Found query: {dur}ms recorded, {len(bindings)} bindings, {len(test_rows)} rows")
                # Run warm
                run_dax(text, rls_conn)
                rows_warm, dur_warm = run_dax(text, rls_conn)
                print(f"  Warm: {len(rows_warm)} rows, {dur_warm:.0f}ms (target: {dur}ms)")
                break
        except Exception:
            continue

rls_conn.Close()
print("\nDone.")
