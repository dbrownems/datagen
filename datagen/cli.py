"""Command-line interface for datagen.

Usage:
    python -m datagen parse  model.vpax -o config.yaml
    python -m datagen generate config.yaml [-o Tables/]
"""

import argparse
import sys


def cmd_parse(args):
    """Parse a .vpax file and generate a YAML config."""
    from .vpax_parser import parse_vpax
    from .config_generator import generate_config
    from .config import save_config

    model = parse_vpax(args.vpax_file)

    print(f"Model: {model['model_name']}")
    print(f"Tables: {len(model['tables'])}")
    for table in model["tables"]:
        n_cols = len(table["columns"])
        print(f"  {table['name']}: {table['row_count']:,} rows, {n_cols} columns")

    config = generate_config(
        model,
        output_path=args.output_path or "Tables/",
        seed=args.seed,
    )

    if args.dax_trace:
        from .dax_literal_extractor import (
            iter_dax_texts_from_jsonl, seed_config_from_trace,
        )
        n = seed_config_from_trace(
            config, iter_dax_texts_from_jsonl(args.dax_trace), vpax_model=model,
            top_n=args.top_n, observed_share=args.observed_share,
            verbose=args.verbose,
        )
        print(f"\nSeeded {n} columns from DAX trace: {args.dax_trace}")

    out_file = args.output or args.vpax_file.rsplit(".", 1)[0] + "_config.yaml"
    save_config(config, out_file)
    print(f"\nConfig written to: {out_file}")


def cmd_seed_literals(args):
    """Re-apply DAX-literal seeding to an existing YAML config."""
    from .config import load_config, save_config
    from .vpax_parser import parse_vpax
    from .dax_literal_extractor import (
        iter_dax_texts_from_jsonl, seed_config_from_trace,
    )

    config = load_config(args.config_file)
    model = parse_vpax(args.vpax_file) if args.vpax_file else None
    n = seed_config_from_trace(
        config, iter_dax_texts_from_jsonl(args.jsonl_file), vpax_model=model,
        top_n=args.top_n, observed_share=args.observed_share,
        verbose=args.verbose,
    )
    out_file = args.output or args.config_file
    save_config(config, out_file)
    print(f"Seeded {n} columns; config written to: {out_file}")


def cmd_generate(args):
    """Generate Delta tables from a YAML config (requires PySpark)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("Error: PySpark is required for generation. Install it or run in Fabric.")
        sys.exit(1)

    from .config import load_config
    from .spark_generator import generate_all_tables

    config = load_config(args.config_file)

    spark = SparkSession.builder \
        .appName(f"datagen-{config.model_name}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        generate_all_tables(spark, config, output_path=args.output_path)
    finally:
        spark.stop()


def cmd_model(args):
    """Generate a TMDL folder from a .vpax file."""
    from .model_builder import build_tmdl

    out_folder = args.output or args.vpax_file.rsplit(".", 1)[0] + ".SemanticModel"
    build_tmdl(
        vpax_path=args.vpax_file,
        output_folder=out_folder,
        lakehouse_name=args.lakehouse,
        lakehouse_sql_endpoint=args.endpoint,
        model_name=args.model_name,
        mode=args.mode,
    )


def main():
    parser = argparse.ArgumentParser(
        prog="datagen",
        description="Generate realistic Delta tables from Power BI model metadata.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # -- parse --
    p_parse = subparsers.add_parser("parse", help="Parse a .vpax file → YAML config")
    p_parse.add_argument("vpax_file", help="Path to the .vpax file")
    p_parse.add_argument("-o", "--output", help="Output YAML path (default: <vpax>_config.yaml)")
    p_parse.add_argument("--output-path", default="Tables/", help="Default output_path in config")
    p_parse.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    p_parse.add_argument("--dax-trace", help="Optional XEvents JSONL trace; literal column "
                         "values from QueryEnd events seed the config.")
    p_parse.add_argument("--top-n", type=int, default=200,
                         help="Cap on observed values per column (default: 200)")
    p_parse.add_argument("--observed-share", type=float, default=0.7,
                         help="Frequency budget share (0-1) for observed values (default: 0.7)")
    p_parse.add_argument("--verbose", action="store_true", help="Verbose seeding output")
    p_parse.set_defaults(func=cmd_parse)

    # -- seed-literals --
    p_seed = subparsers.add_parser(
        "seed-literals",
        help="Re-apply DAX literal seeding to an existing YAML config")
    p_seed.add_argument("config_file", help="Existing YAML config to update")
    p_seed.add_argument("jsonl_file", help="DAX trace JSONL file")
    p_seed.add_argument("--vpax-file", help="Optional VPAX for name normalisation")
    p_seed.add_argument("-o", "--output", help="Output path (default: overwrite input)")
    p_seed.add_argument("--top-n", type=int, default=200)
    p_seed.add_argument("--observed-share", type=float, default=0.7)
    p_seed.add_argument("--verbose", action="store_true")
    p_seed.set_defaults(func=cmd_seed_literals)

    # -- generate --
    p_gen = subparsers.add_parser("generate", help="Generate Delta tables from YAML config")
    p_gen.add_argument("config_file", help="Path to the YAML config file")
    p_gen.add_argument("-o", "--output-path", help="Override output path")
    p_gen.set_defaults(func=cmd_generate)

    # -- model --
    p_model = subparsers.add_parser(
        "model", help="Generate a TMDL definition from a .vpax file")
    p_model.add_argument("vpax_file", help="Path to the .vpax file")
    p_model.add_argument("-o", "--output", help="Output TMDL folder (default: <vpax>.SemanticModel)")
    p_model.add_argument("--lakehouse", help="Fabric lakehouse name")
    p_model.add_argument("--endpoint", help="Lakehouse SQL analytics endpoint")
    p_model.add_argument("--model-name", help="Override the model name from the VPAX")
    p_model.add_argument("--mode", choices=["direct_lake", "import"], default="import",
                         help="Model mode: import (default) or direct_lake")
    p_model.set_defaults(func=cmd_model)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
