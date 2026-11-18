"""Stage 0 - load every landed CSV into raw tables, untouched, with file lineage on each row.

Outputs
-------
raw_<dataset>   every row as strings + _file_name, _file_date, _file_ts, _landing_ts, _row_num, _row_id
file_inventory  one row per landed file: row count, content fingerprint, arrival times
"""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from .config import Settings, ensure_namespace, write_table
from .files import NAME_RE as _NAME_RE
from .files import read_dataset_files


def _clean(col):
    """Trim, strip stray carriage returns, and turn blanks into NULL."""
    c = F.trim(F.regexp_replace(col, r"[\r\n]+", ""))
    return F.when(c == "", None).otherwise(c)


def _from_table(spark, ds: dict, columns):
    """Source is a table that was loaded for us (monitored_datasets.csv, column ``source_table``).

    file_name_column  column holding the original file name (best: date and time are parsed from it)
    file_date_expr    SQL expression for the business date, used when there is no file name
    arrival_ts_expr   SQL expression for when the data arrived (optional)
    """
    src = spark.table(ds["source_table"])
    name_col = ds.get("file_name_column")
    if not name_col and not ds.get("file_date_expr"):
        raise ValueError(
            f"{ds['dataset']}: source_table needs file_name_column or file_date_expr in monitored_datasets.csv"
        )
    file_name = F.element_at(F.split(F.col(name_col).cast("string"), "/"), -1) if name_col else F.lit(None).cast("string")
    arrival = F.expr(f"try_cast(({ds['arrival_ts_expr']}) as timestamp)") if ds.get("arrival_ts_expr") else F.lit(None).cast("timestamp")
    hint = F.expr(f"try_cast(({ds['file_date_expr']}) as date)") if ds.get("file_date_expr") else F.lit(None).cast("date")
    return src.select(
        *[_clean(F.col(c).cast("string")).alias(c) for c in columns],
        file_name.alias("_file_name"),
        arrival.alias("_landing_ts"),
        hint.alias("_date_hint"),
    )


def run(spark, settings: Settings) -> None:
    ensure_namespace(spark, settings)
    inventories = []

    for ds in settings.datasets():
        columns = [name for name, _ in ds["columns"]]

        if ds.get("source_table"):
            raw = _from_table(spark, ds, columns)
        else:
            raw = read_dataset_files(spark, settings.landing_path, ds).select(
                *[_clean(F.col(c)).alias(c) for c in columns],
                "_file_name",
                "_landing_ts",
                F.lit(None).cast("date").alias("_date_hint"),
            )
        # Blank lines come through as all-NULL rows: drop them.
        raw = raw.dropna(how="all", subset=columns)

        fn = F.col("_file_name")
        iso_date = F.concat_ws(
            "-", F.regexp_extract(fn, _NAME_RE, 3), F.regexp_extract(fn, _NAME_RE, 2), F.regexp_extract(fn, _NAME_RE, 1)
        )
        iso_time = F.concat_ws(
            ":", F.regexp_extract(fn, _NAME_RE, 4), F.regexp_extract(fn, _NAME_RE, 5), F.regexp_extract(fn, _NAME_RE, 6)
        )
        raw = (
            raw.withColumn("_fd", iso_date)
            .withColumn("_ft", F.concat_ws(" ", iso_date, iso_time))
            # File date and time come from the file name; without one, from the configured expressions.
            .withColumn("_file_date", F.coalesce(F.expr("try_cast(_fd as date)"), F.col("_date_hint")))
            .withColumn("_file_ts", F.coalesce(F.expr("try_cast(_ft as timestamp)"), F.col("_landing_ts")))
            .withColumn(
                "_file_name",
                F.coalesce(fn, F.concat(F.lit(ds["dataset"] + "_"), F.date_format("_file_date", "ddMMyyyy"))),
            )
            .drop("_fd", "_ft", "_date_hint")
            .withColumn("_ord", F.monotonically_increasing_id())
            .withColumn("_row_num", F.row_number().over(Window.partitionBy("_file_name").orderBy("_ord")))
            .withColumn("_row_id", F.concat_ws(":", F.col("_file_name"), F.col("_row_num")))
            .drop("_ord")
        )
        write_table(raw, settings, f"raw_{ds['table_suffix']}")

        stored = spark.table(settings.table(f"raw_{ds['table_suffix']}"))
        row_hash = F.xxhash64(*[F.coalesce(F.col(c), F.lit("\u0000")) for c in columns])
        inventories.append(
            stored.groupBy("_file_name", "_file_date")
            .agg(
                F.min("_file_ts").alias("_file_ts"),
                F.min("_landing_ts").alias("_landing_ts"),
                F.count(F.lit(1)).alias("row_count"),
                F.concat_ws(
                    "-",
                    F.bit_xor(row_hash).cast("string"),
                    F.sum(F.pmod(row_hash, F.lit(1000000007))).cast("string"),
                    F.count(F.lit(1)).cast("string"),
                ).alias("content_fingerprint"),
            )
            .select(
                F.lit(ds["dataset"]).alias("dataset"),
                F.col("_file_name").alias("file_name"),
                F.col("_file_date").alias("file_date"),
                F.col("_file_ts").alias("file_ts"),
                F.col("_landing_ts").alias("landing_ts"),
                "row_count",
                "content_fingerprint",
            )
        )

    inventory = inventories[0]
    for other in inventories[1:]:
        inventory = inventory.unionByName(other)
    write_table(inventory, settings, "file_inventory")
