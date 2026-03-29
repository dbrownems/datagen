"""Discover RLS roles and their filter expressions from the semantic model."""
import json
import re
from pythonnet import load
load("coreclr")
import clr

dll = r"C:\Users\david\AppData\Local\Temp\adomd2\lib\net8.0\Microsoft.AnalysisServices.AdomdClient.dll"
clr.AddReference(dll)
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand

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
    return rows


conn = get_connection()
print(f"Connected: {conn.Database}\n")

# Get roles via DMV
print("=== Roles ===")
try:
    roles = run_dax(
        "SELECT [Name], [Description] FROM $SYSTEM.TMSCHEMA_ROLES", conn
    )
    for r in roles:
        print(f"  Role: {r}")
except Exception as e:
    print(f"  DMV error: {str(e)[:200]}")

# Get role table permissions (where the RLS filter expressions live)
print("\n=== Role Table Permissions (RLS filters) ===")
try:
    perms = run_dax(
        "SELECT [RoleID], [TableID], [FilterExpression] "
        "FROM $SYSTEM.TMSCHEMA_TABLE_PERMISSIONS "
        "WHERE [FilterExpression] <> ''",
        conn,
    )
    for p in perms:
        print(f"  RoleID={p.get('[RoleID]')}, TableID={p.get('[TableID]')}")
        expr = p.get("[FilterExpression]", "")
        print(f"    Filter: {expr[:200]}")
        # Check for USERPRINCIPALNAME() or USERNAME()
        if "USERPRINCIPALNAME()" in expr.upper() or "USERNAME()" in expr.upper():
            print("    ^^^ Contains USERPRINCIPALNAME/USERNAME")
        print()
except Exception as e:
    print(f"  DMV error: {str(e)[:200]}")

# Also try to get role names mapped to IDs
print("\n=== Role ID mapping ===")
try:
    role_map = run_dax("SELECT [ID], [Name] FROM $SYSTEM.TMSCHEMA_ROLES", conn)
    for r in role_map:
        print(f"  ID={r.get('[ID]')}: {r.get('[Name]')}")
except Exception as e:
    print(f"  error: {str(e)[:200]}")

# Get table ID mapping
print("\n=== Table ID mapping (for RLS tables) ===")
try:
    table_map = run_dax("SELECT [ID], [Name] FROM $SYSTEM.TMSCHEMA_TABLES", conn)
    tid_to_name = {}
    for t in table_map:
        tid_to_name[t.get("[ID]")] = t.get("[Name]")
    # Show only RLS-related tables
    for tid, tname in tid_to_name.items():
        if tname and ("rls" in tname.lower() or "access" in tname.lower()):
            print(f"  ID={tid}: {tname}")
except Exception as e:
    print(f"  error: {str(e)[:200]}")

conn.Close()
print("\nDone.")
