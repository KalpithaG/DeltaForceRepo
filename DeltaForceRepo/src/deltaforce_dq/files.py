"""Finding and reading the landed CSV files for one dataset.

Files are searched for in the landing folder and every folder below it. A file belongs to a
dataset when either
  * its name matches the dataset's ``file_pattern`` (for example ``[Ss]ales*.csv``), or
  * it sits in a folder named after the dataset, singular or plural, any letter case
    (for example ``.../inputdata/Sales/`` or ``.../inputdata/customers/``).

The file date and time are read from the end of the file name: ``DDMMYYYY_HHMMSS.csv``.
"""

from __future__ import annotations

import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ...DDMMYYYY_HHMMSS.csv
NAME_RE = r"(\d{2})(\d{2})(\d{4})_(\d{2})(\d{2})(\d{2})\.csv$"


def _glob_to_regex(glob: str) -> str:
    out, i = "", 0
    while i < len(glob):
        ch = glob[i]
        if ch == "*":
            out += "[^/]*"
        elif ch == "?":
            out += "[^/]"
        elif ch == "[" and "]" in glob[i:]:
            end = glob.index("]", i)
            out += glob[i:end + 1]
            i = end
        else:
            out += re.escape(ch)
        i += 1
    return out


def path_regex(ds: dict) -> str:
    """Regular expression, applied to the full file path, that selects this dataset's files."""
    name = re.escape(ds["dataset"].lower())
    plural = "" if name.endswith("s") else "s?"
    by_folder = f"(?i:.*/{name}{plural}/(?:[^/]+/)*[^/]+\\.csv)"
    by_name = f".*/{_glob_to_regex(ds['file_pattern'])}"
    return f"^(?:{by_folder}|{by_name})$"


def read_dataset_files(spark, landing_path: str, ds: dict):
    """All rows of all files of one dataset, every column as text, plus _file_name and _landing_ts."""
    columns = [name for name, _ in ds["columns"]]
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.csv")
        .schema(schema)
        .load(landing_path)
        .select(
            *columns,
            F.col("_metadata.file_path").alias("_file_path"),
            F.col("_metadata.file_name").alias("_file_name"),
            F.col("_metadata.file_modification_time").alias("_landing_ts"),
        )
        .filter(F.col("_file_path").rlike(path_regex(ds)))
        .drop("_file_path")
    )


def check_file_names(df, dataset: str, name_col: str = "_file_name") -> None:
    """The hub needs the date and time in the file name. Fail clearly if no file has them."""
    names = [r[0] for r in df.select(name_col).distinct().collect()]
    if not names:
        return
    undated = sorted(n for n in names if not re.search(NAME_RE, n or ""))
    if len(undated) == len(names):
        raise ValueError(
            f"{dataset}: none of the file names end in DDMMYYYY_HHMMSS.csv, so the hub cannot tell which day "
            f"each file belongs to. Files found: {', '.join(undated[:5])}"
        )
    if undated:
        print(f"[deltaforce_dq] WARNING: {dataset} files without a date in the name are ignored by the "
              f"daily checks: {', '.join(undated[:5])}")
