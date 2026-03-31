"""Datagen - Generate realistic Delta tables from Power BI model metadata (.vpax files)."""

__version__ = "0.7.15"


def generate(
    spark,
    vpax_path,
    output_path="Tables/",
    seed=42,
    deploy_model=True,
    compare=True,
    dataset=None,
    workspace=None,
    lakehouse=None,
    overwrite_tables=True,
    overwrite_model=True,
    mode="direct_lake",
    output_format="delta",
    include_hidden=False,
    include_calculated=False,
):
    """One-call pipeline: .vpax → Delta tables → semantic model → comparison report.

    Infers data distributions directly from the .vpax file, generates
    Delta tables in the lakehouse, then (optionally) deploys a semantic
    model with all measures, relationships, and column metadata from the
    .vpax.

    Args:
        spark: Active SparkSession.
        vpax_path: Path to the .vpax file.
        output_path: Where to write Delta tables (default ``Tables/``).
        seed: Random seed for reproducible generation.
        deploy_model: If True, deploy a semantic model after generating
            the tables (requires ``semantic-link-labs``).
        compare: If True, compare generated tables against the config
            and print an accuracy report.
        dataset: Semantic model name (defaults to the VPAX model name).
        workspace: Target Fabric workspace (default: current).
        lakehouse: Lakehouse name (default: attached lakehouse).
        overwrite_tables: If True, regenerate all Delta tables. If False,
            skip tables that already exist and only generate missing ones.
        overwrite_model: If True, overwrite an existing semantic model.
            If False, fail if the model already exists.
        mode: ``"direct_lake"`` (default) or ``"import"``.
        output_format: ``"delta"`` (default) or ``"parquet"``.
        include_hidden: Include hidden columns/tables from the VPAX.
        include_calculated: Include calculated columns.

    Returns:
        pandas.DataFrame with the comparison report (if compare=True),
        otherwise None.

    Example::

        # Direct Lake (default):
        from datagen import generate
        report = generate(spark, "/lakehouse/default/Files/model.vpax")

        # Import mode:
        report = generate(spark, "model.vpax", mode="import")
    """
    from .vpax_parser import parse_vpax
    from .config_generator import generate_config
    from .spark_generator import generate_all_tables

    # Step 1 — infer generation config directly from VPAX
    import os, time as _time
    _t0 = _time.time()
    vpax_name = os.path.basename(vpax_path)
    print(f"Parsing {vpax_name} ...", flush=True)
    vpax_model = parse_vpax(vpax_path)
    print(f"  {len(vpax_model.get('tables', []))} tables, "
          f"{len(vpax_model.get('relationships', []))} relationships "
          f"({_time.time() - _t0:.1f}s)", flush=True)

    _t1 = _time.time()
    print("Inferring generation config ...", flush=True)
    config = generate_config(
        vpax_model,
        output_path=output_path,
        seed=seed,
        include_hidden=include_hidden,
        include_calculated=include_calculated,
    )
    print(f"  Config ready ({_time.time() - _t1:.1f}s)", flush=True)

    # Step 2 — generate Delta tables (pass vpax_model for date table detection)
    succeeded_tables, actual_output_path = generate_all_tables(
        spark, config, output_path=output_path,
        output_format=output_format, vpax_model=vpax_model,
        overwrite=overwrite_tables)

    # Step 3 — deploy semantic model (optional, only for tables that succeeded)
    if deploy_model:
        from .model_builder import deploy_semantic_model
        deploy_semantic_model(
            vpax_path=vpax_path,
            dataset=dataset,
            workspace=workspace,
            lakehouse=lakehouse,
            mode=mode,
            include_hidden=include_hidden,
            include_calculated=include_calculated,
            overwrite=overwrite_model,
            table_filter=succeeded_tables,
        )

    # Step 4 — compare generated tables against config (optional)
    if compare:
        from .compare import compare_tables
        return compare_tables(spark, config, output_path=actual_output_path)

    return None
