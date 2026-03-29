"""Test script: create a trivial TMDL definition and inspect the output.

Run this in a Fabric notebook to verify TOM → TMDL serialization works.
"""
import json
import base64
import tempfile
import os


def test_tmdl_roundtrip():
    import sempy.fabric as fabric
    fabric.create_tom_server()

    import clr
    clr.AddReference("Microsoft.AnalysisServices.Tabular")
    from Microsoft.AnalysisServices.Tabular import (
        JsonSerializer, TmdlSerializer,
        Database, Model, Table, Partition, DataColumn,
        EntityPartitionSource, NamedExpression,
        ExpressionKind, DataType,
    )

    # Build a trivial model via TOM
    db = Database()
    db.Name = "TestModel"
    db.CompatibilityLevel = 1604

    model = Model()
    db.Model = model

    # Add expression
    expr = NamedExpression()
    expr.Name = "DL"
    expr.Kind = ExpressionKind.M
    expr.Expression = (
        'let\n'
        '    Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/ws/lh", [HierarchicalNavigation=true])\n'
        'in\n'
        '    Source'
    )
    model.Expressions.Add(expr)

    # Add a table
    table = Table()
    table.Name = "Sales"

    col1 = DataColumn()
    col1.Name = "ID"
    col1.DataType = DataType.Int64
    col1.SourceColumn = "ID"
    table.Columns.Add(col1)

    col2 = DataColumn()
    col2.Name = "Amount"
    col2.DataType = DataType.Double
    col2.SourceColumn = "Amount"
    table.Columns.Add(col2)

    part = Partition()
    part.Name = "Sales"
    source = EntityPartitionSource()
    source.EntityName = "Sales"
    source.SchemaName = "dbo"
    table.Partitions.Add(part)

    model.Tables.Add(table)

    # Set expression source after table is in model
    part.Source = source
    source.ExpressionSource = model.Expressions["DL"]

    # Serialize to TMDL
    with tempfile.TemporaryDirectory() as tmdl_dir:
        TmdlSerializer.SerializeDatabaseToFolder(db, tmdl_dir)

        print("=== TMDL files ===")
        for root, dirs, files in os.walk(tmdl_dir):
            for f in files:
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, tmdl_dir)
                content = open(fpath, "r").read()
                print(f"\n--- {rel} ---")
                print(content)

    print("\n=== SUCCESS ===")


if __name__ == "__main__":
    test_tmdl_roundtrip()
