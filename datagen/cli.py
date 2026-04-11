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
        include_hidden=args.include_hidden,
        include_calculated=args.include_calculated,
    )

    out_file = args.output or args.vpax_file.rsplit(".", 1)[0] + "_config.yaml"
    save_config(config, out_file)
    print(f"\nConfig written to: {out_file}")


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
        include_hidden=args.include_hidden,
        include_calculated=args.include_calculated,
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
    p_parse.add_argument("--include-hidden", action="store_true", help="Include hidden columns")
    p_parse.add_argument("--include-calculated", action="store_true", help="Include calculated columns")
    p_parse.set_defaults(func=cmd_parse)

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
    p_model.add_argument("--mode", choices=["direct_lake", "import"], default="direct_lake",
                         help="Model mode: direct_lake (default) or import")
    p_model.add_argument("--include-hidden", action="store_true", help="Include hidden objects")
    p_model.add_argument("--include-calculated", action="store_true", help="Include calculated columns")
    p_model.set_defaults(func=cmd_model)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
