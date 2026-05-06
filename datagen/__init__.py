"""Datagen - Generate realistic Delta tables from Power BI model metadata (.vpax files)."""

from importlib.metadata import version as _v, PackageNotFoundError as _PNF
try:
    __version__ = _v("datagen-fabric")
except _PNF:
    __version__ = "0.0.0+local"


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
    mode="import",
    queries_path=None,
    skew_recent=False,
    replace_username_with_customdata=False,
    auto_fix_relationship_types=True,
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
        mode: ``"import"`` (default) or ``"direct_lake"``.

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
    from .config import save_config, load_config
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

    # Companion YAML lives next to the .vpax. If present, it represents
    # the user's tweaked config and is loaded instead of regenerating.
    # Otherwise, infer from VPAX and save it for future tweaks.
    yaml_path = os.path.splitext(vpax_path)[0] + ".yaml"
    _t1 = _time.time()
    loaded_from_yaml = os.path.exists(yaml_path)
    if loaded_from_yaml:
        print(f"Loading existing config: {os.path.basename(yaml_path)}", flush=True)
        config = load_config(yaml_path)
        # Honor caller's seed/output_path overrides
        if output_path:
            config.output_path = output_path
        if seed is not None:
            config.seed = seed
    else:
        print("Inferring generation config ...", flush=True)
        config = generate_config(
            vpax_model,
            output_path=output_path,
            seed=seed,
        )

    # Query seeding and BIM cross-ref only apply when freshly generating;
    # loaded YAML is treated as the source of truth.
    if not loaded_from_yaml and queries_path:
        # Detect format: .jsonl (DAX trace) → use new literal extractor;
        # .json (legacy structured queries) → use old extract_query_values.
        if str(queries_path).lower().endswith(".jsonl"):
            from .dax_literal_extractor import seed_config_from_trace
            n_fixed = seed_config_from_trace(config, queries_path, vpax_model=vpax_model)
        else:
            from .dax_rewriter import extract_query_values
            n_fixed = extract_query_values(config, queries_path)
        if n_fixed:
            print(f"  Applied {n_fixed} column value(s) from queries", flush=True)

    # Cross-reference with BIM to add columns missing from VPAX stats
    # (Direct Lake needs all BIM columns in Delta; Import mode doesn't)
    if not loaded_from_yaml and deploy_model and mode == "direct_lake":
        from .model_builder import _extract_bim, _add_missing_bim_columns
        try:
            bim = _extract_bim(vpax_path)
            n_added = _add_missing_bim_columns(config, bim)
            if n_added:
                print(f"  Added {n_added} column(s) from Model.bim not in VPAX stats", flush=True)
        except Exception:
            pass

    # For import mode, determine which tables to skip (measure-only, enter-data)
    tables_to_skip = set()
    if mode == "import":
        from .model_builder import get_tables_to_skip
        tables_to_skip = get_tables_to_skip(vpax_path, mode)
        if tables_to_skip:
            print(f"  Skipping {len(tables_to_skip)} table(s) (measure-only / enter-data)", flush=True)

    print(f"  Config ready ({_time.time() - _t1:.1f}s)", flush=True)

    # Save the freshly-inferred config so the user can tweak and re-run.
    # Skip if we loaded from YAML (don't clobber user edits).
    if not loaded_from_yaml:
        try:
            save_config(config, yaml_path)
            print(f"  Saved config: {os.path.basename(yaml_path)}", flush=True)
        except Exception as e:
            print(f"  ⚠ Could not save config to {yaml_path}: {e}", flush=True)

    # Validate per-table histograms and compute parent PK seeding map.
    # Raises ValueError on any constraint violation (mixing fractions/counts,
    # multiple histograms in the same relationship cluster, etc.).
    from .histogram_validator import validate_and_build_seed_map
    parent_seed_map, _hist_counts = validate_and_build_seed_map(config, vpax_model)
    if parent_seed_map:
        n_seeded = sum(len(cols) for cols in parent_seed_map.values())
        n_hist_tables = sum(1 for t in config.tables if getattr(t, "histogram", None))
        print(f"  Histogram: {n_hist_tables} table(s) with histograms; "
              f"seeding {n_seeded} parent column(s) "
              f"across {len(parent_seed_map)} parent table(s)", flush=True)

    # Direct Lake refuses relationships with mismatched FromColumn/ToColumn types.
    # Auto-widen by default; log a prominent warning + the YAML snippets the user
    # can paste to take control (including a narrowing alternative).
    if mode == "direct_lake":
        from .relationship_type_fixer import fix_relationship_types
        fix_relationship_types(config, vpax_model, auto_fix=auto_fix_relationship_types)

    # Step 2 — generate Delta tables (pass vpax_model for date table detection)
    succeeded_tables, actual_output_path = generate_all_tables(
        spark, config, output_path=output_path,
        vpax_model=vpax_model,
        overwrite=overwrite_tables, skip_tables=tables_to_skip,
        skew_recent=skew_recent,
        parent_seed_map=parent_seed_map)

    # Step 3 — deploy semantic model (optional, only for tables that succeeded)
    if deploy_model:
        from .model_builder import deploy_semantic_model
        deploy_semantic_model(
            vpax_path=vpax_path,
            dataset=dataset,
            workspace=workspace,
            lakehouse=lakehouse,
            mode=mode,
            overwrite=overwrite_model,
            table_filter=succeeded_tables,
            replace_username_with_customdata=replace_username_with_customdata,
            config=config,
        )

    # Step 4 — compare generated tables against config (optional)
    if compare:
        from .compare import compare_tables
        return compare_tables(spark, config, output_path=actual_output_path)

    return None
