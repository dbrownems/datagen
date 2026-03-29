"""Analyze where data gets filtered out for quarterly reporting queries.

For a quarterly reporting model, the key filter chain is:
  Quarter → Territory/Geo → Account → Opportunity → Pipeline

Check how many rows survive each filter level.
"""
from pythonnet import load
load("coreclr")
import clr

clr.AddReference(r"C:\Users\david\AppData\Local\Temp\adomd2\lib\net8.0\Microsoft.AnalysisServices.AdomdClient.dll")
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand

with open(".token") as f:
    token = f.read().strip()
conn = AdomdConnection(
    f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld;"
    f"Initial Catalog=Sales Leader Dataset;password={token};Timeout=7200;"
)
conn.Open()


def dax(q):
    cmd = AdomdCommand(q, conn)
    r = cmd.ExecuteReader()
    cols = [r.GetName(i) for i in range(r.FieldCount)]
    rows = []
    while r.Read():
        rows.append({cols[i]: str(r.GetValue(i)) if r.GetValue(i) is not None else None
                     for i in range(r.FieldCount)})
    r.Close()
    return rows


# Key fact table
FACT = "100-Fixed Data Pipeline"

print(f"=== Filter funnel for '{FACT}' ===\n")

# 1. Total rows
r = dax(f"EVALUATE ROW(\"N\", COUNTROWS('{FACT}'))")
total = int(r[0]["[N]"])
print(f"Total fact rows: {total:,}")

# 2. Current Quarter filter
r = dax(f"EVALUATE ROW(\"N\", CALCULATE(COUNTROWS('{FACT}'), '001-Quarter'[Dynamic Quarter] = \"Current Quarter\"))")
cq = int(r[0]["[N]"])
print(f"  + Dynamic Quarter = 'Current Quarter': {cq:,} ({cq/total*100:.1f}%)")

# 3. Current Quarter + Forecast Category
for cat in ["Commit", "Upside", "Closed", "Expect"]:
    try:
        r = dax(
            f"EVALUATE ROW(\"N\", CALCULATE(COUNTROWS('{FACT}'), "
            f"'001-Quarter'[Dynamic Quarter] = \"Current Quarter\", "
            f"'025-Parent Opportunity'[Parent Forecast Category] = \"{cat}\"))"
        )
        n = int(r[0]["[N]"])
        print(f"    + Forecast Category = '{cat}': {n:,} ({n/total*100:.1f}%)")
    except Exception:
        print(f"    + Forecast Category = '{cat}': ERROR (column may not filter)")

# 4. Check if Forecast Category values exist
print(f"\n=== '025-Parent Opportunity'[Parent Forecast Category] values ===")
r = dax("EVALUATE VALUES('025-Parent Opportunity'[Parent Forecast Category])")
for row in r:
    print(f"  {list(row.values())[0]}")

# 5. Check if New Logo Flag values exist
print(f"\n=== '004-Opportunity'[New Logo Flag] values ===")
r = dax("EVALUATE VALUES('004-Opportunity'[New Logo Flag])")
for row in r:
    print(f"  {list(row.values())[0]}")

# 6. Territory/Geo filtering
print(f"\n=== Territory/Geo filter selectivity ===")
r = dax(f"EVALUATE ROW(\"N\", COUNTROWS('008-Territory'))")
total_terr = int(r[0]["[N]"])
print(f"Total territories: {total_terr:,}")

for geo in ["AMS", "APAC", "EMEA"]:
    try:
        r = dax(f"EVALUATE ROW(\"N\", CALCULATE(COUNTROWS('008-Territory'), '008-Territory'[Geo] = \"{geo}\"))")
        n = int(r[0]["[N]"])
        print(f"  Geo='{geo}': {n:,} ({n/total_terr*100:.1f}%)")
    except Exception:
        pass

# Check generated Geo values
print(f"\n=== '008-Territory'[Geo] values ===")
r = dax("EVALUATE VALUES('008-Territory'[Geo])")
for row in r:
    print(f"  {list(row.values())[0]}")

# 7. RLS impact — how much does RLS user see?
print(f"\n=== RLS filter impact ===")
rls_conn = AdomdConnection(
    f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/dbrowne_sld;"
    f"Initial Catalog=Sales Leader Dataset;password={token};Timeout=7200;"
    f"CustomData=bucky.barnes23@contoso.com;Roles=RLS Access - Geo;"
)
rls_conn.Open()

cmd = AdomdCommand(f"EVALUATE ROW(\"N\", COUNTROWS('{FACT}'))", rls_conn)
reader = cmd.ExecuteReader()
reader.Read()
rls_facts = int(str(reader.GetValue(0)))
reader.Close()

cmd = AdomdCommand("EVALUATE ROW(\"N\", COUNTROWS('008-Territory'))", rls_conn)
reader = cmd.ExecuteReader()
reader.Read()
rls_terr = int(str(reader.GetValue(0)))
reader.Close()

print(f"  User: bucky.barnes23@contoso.com (RLS Access - Geo)")
print(f"  Territories: {rls_terr:,}/{total_terr:,} ({rls_terr/total_terr*100:.1f}%)")
print(f"  Fact rows:   {rls_facts:,}/{total:,} ({rls_facts/total*100:.1f}%)")

# CQ + RLS
cmd = AdomdCommand(
    f"EVALUATE ROW(\"N\", CALCULATE(COUNTROWS('{FACT}'), "
    f"'001-Quarter'[Dynamic Quarter] = \"Current Quarter\"))",
    rls_conn,
)
reader = cmd.ExecuteReader()
reader.Read()
rls_cq = int(str(reader.GetValue(0)))
reader.Close()
print(f"  CQ facts:    {rls_cq:,}/{total:,} ({rls_cq/total*100:.1f}%)")

rls_conn.Close()
conn.Close()
print("\nDone.")
