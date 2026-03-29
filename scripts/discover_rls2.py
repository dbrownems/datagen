"""Discover RLS - dump raw DMV results."""
import re
from pythonnet import load
load("coreclr")
import clr

dll = r"C:\Users\david\AppData\Local\Temp\adomd2\lib\net8.0\Microsoft.AnalysisServices.AdomdClient.dll"
clr.AddReference(dll)
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand

def get_connection():
    with open(".token", "r") as f:
        token = f.read().strip()
    conn = AdomdConnection(
        f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld;"
        f"Initial Catalog=Sales Leader Dataset - DL;password={token};Timeout=7200;"
    )
    conn.Open()
    return conn

def run_dmv(query, conn):
    cmd = AdomdCommand(query, conn)
    reader = cmd.ExecuteReader()
    cols = [reader.GetName(i) for i in range(reader.FieldCount)]
    print(f"  Columns: {cols}")
    rows = []
    while reader.Read():
        row = {cols[i]: str(reader.GetValue(i)) if reader.GetValue(i) is not None else None
               for i in range(reader.FieldCount)}
        rows.append(row)
    reader.Close()
    return rows

conn = get_connection()

print("=== ROLES ===")
rows = run_dmv("SELECT * FROM $SYSTEM.TMSCHEMA_ROLES", conn)
for r in rows:
    print(f"  {r}")

print("\n=== TABLE PERMISSIONS (first 5) ===")
rows = run_dmv("SELECT * FROM $SYSTEM.TMSCHEMA_TABLE_PERMISSIONS", conn)
for r in rows[:5]:
    print(f"  {r}")

print("\n=== TABLES (first 3 RLS) ===")
rows = run_dmv("SELECT * FROM $SYSTEM.TMSCHEMA_TABLES", conn)
for r in rows:
    name = None
    for k, v in r.items():
        if v and ("rls" in str(v).lower() or "access" in str(v).lower()):
            name = v
            break
    if name:
        print(f"  {r}")
        break  # just show one

conn.Close()
