"""Stage 0a - load the landed CSV files into source tables.

For every dataset in monitored_datasets.csv with ``load_from_files = Y`` this reads all of the
dataset's CSV files from the landing folder and its subfolders and (re)creates ``source_table``:

    /Volumes/team_9/default/inputdata/Sales/*.csv      -> team_9.default.sales
    /Volumes/team_9/default/inputdata/Customers/*.csv  -> team_9.default.customers
    /Volumes/team_9/default/inputdata/Product/*.csv    -> team_9.default.product

Files are matched by their folder name or by their own name (see files.py).

The table keeps every value as text, exactly as it arrived, and adds three columns:
  source_file       name of the file the row came from
  file_arrival_ts   when that file reached the volume
  load_ts           when this load ran

The rest of the hub reads from these tables, not from the files.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from .config import Settings
from .files import check_file_names, read_dataset_files


def run(spark, settings: Settings) -> None:
    for ds in settings.datasets():
        if not ds.get("source_table") or (ds.get("load_from_files") or "N").upper() != "Y":
            continue
        columns = [name for name, _ in ds["columns"]]
        df = read_dataset_files(spark, settings.landing_path, ds).select(
            *columns,
            F.col("_file_name").alias("source_file"),
            F.col("_landing_ts").alias("file_arrival_ts"),
            F.current_timestamp().alias("load_ts"),
        )
        if not df.limit(1).collect():
            raise ValueError(
                f"No CSV files for dataset {ds['dataset']} found in {settings.landing_path} or its subfolders. "
                f"Expected a folder named '{ds['dataset']}' (or plural) or files matching '{ds['file_pattern']}'."
            )
        check_file_names(df, ds["dataset"], "source_file")
        writer = df.write.mode("overwrite").option("overwriteSchema", "true")
        if settings.table_format:
            writer = writer.format(settings.table_format)
        writer.saveAsTable(ds["source_table"])
