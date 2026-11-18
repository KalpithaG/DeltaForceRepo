"""Command line / Databricks wheel-task entry point.

    dataops_hub --landing-path /Volumes/main/dataops/landing --catalog main --schema dataops
"""

from __future__ import annotations

import argparse

from .config import Settings
from .pipeline import STAGE_NAMES, run_all


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dataops_hub", description="Delta Force Data Quality & Monitoring Hub")
    p.add_argument("--landing-path", required=True, help="Folder or volume holding the landed CSV files")
    p.add_argument("--catalog", default=None, help="Unity Catalog name (omit for plain Spark)")
    p.add_argument("--schema", default="dataops", help="Schema for all hub tables")
    p.add_argument("--config-path", default=None, help="Folder with the config CSVs (default: packaged copies)")
    p.add_argument("--table-format", default=None, help="Table format (default: platform default)")
    p.add_argument("--stage", default="all", choices=["all"] + STAGE_NAMES, help="Run one stage or all")
    p.add_argument("--job-source", default="simulated", choices=["simulated", "system_tables", "both", "existing"],
                   help="Where job run history comes from")
    return p


def main(argv=None) -> None:
    args = build_parser().parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    settings = Settings(
        landing_path=args.landing_path,
        catalog=args.catalog or None,
        schema=args.schema,
        config_path=args.config_path or None,
        table_format=args.table_format or None,
        job_source=args.job_source,
    )
    run_all(spark, settings, stages=None if args.stage == "all" else [args.stage])


if __name__ == "__main__":
    main()
