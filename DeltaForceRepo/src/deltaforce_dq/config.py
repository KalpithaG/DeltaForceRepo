"""Settings, config-file loading and small table helpers shared by every stage."""

from __future__ import annotations

import csv
import io
import os
from dataclasses import dataclass, field
from importlib import resources
from typing import Dict, List, Optional

SEVERITY_ORDER = {"Critical": 4, "High": 3, "Medium": 2, "Low": 1}

# Trust score: points removed for each file-level or trend-level alert.
SEVERITY_PENALTY = {"Critical": 50, "High": 30, "Medium": 15, "Low": 5}

# Trust score: every 1% of rows failing a quality rule removes this many points.
BAD_ROW_PENALTY_FACTOR = 5

# Trust score bands.
SAFE_THRESHOLD = 90
CAUTION_THRESHOLD = 70

CONFIG_FILES = {
    "datasets": "monitored_datasets.csv",
    "file_monitor": "file_monitor_config.csv",
    "rules": "dq_rules.csv",
    "jobs": "jobs_config.csv",
    "expected_issues": "expected_issues.csv",
}


@dataclass
class Settings:
    """Everything that changes between environments. Nothing else is hard-coded."""

    landing_path: str
    schema: str = "dataops"
    catalog: Optional[str] = None        # None = no catalog (plain Spark / hive_metastore)
    config_path: Optional[str] = None    # folder with the config CSVs; None = packaged defaults
    table_format: Optional[str] = None   # None = platform default (Delta on Databricks)
    healthy_max_bad_share: float = 0.05  # a day with more bad rows than this never becomes a baseline
    baseline_window: int = 7             # healthy days used for the trend baseline
    # Where job run history comes from:
    #   simulated      generated from jobs_config.csv (works anywhere)
    #   system_tables  real runs from the Databricks jobs system tables
    #   both           simulated history plus real runs (falls back to simulated if access fails)
    #   existing       read a job_runs table you loaded yourself
    job_source: str = "simulated"
    job_lookback_days: int = 7
    system_jobs_table: str = "system.lakeflow.jobs"
    system_job_runs_table: str = "system.lakeflow.job_run_timeline"
    _cache: Dict[str, List[dict]] = field(default_factory=dict, repr=False)

    # ---- naming -----------------------------------------------------------------
    @property
    def namespace(self) -> str:
        return f"{self.catalog}.{self.schema}" if self.catalog else self.schema

    def table(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    # ---- config files -----------------------------------------------------------
    def load(self, key: str) -> List[dict]:
        """Read one config CSV as a list of dicts (blank strings become None)."""
        if key in self._cache:
            return self._cache[key]
        filename = CONFIG_FILES[key]
        if self.config_path:
            path = os.path.join(self.config_path, filename)
            if not os.path.exists(path):
                if key == "expected_issues":  # optional file
                    self._cache[key] = []
                    return []
                raise FileNotFoundError(f"Config file not found: {path}")
            with open(path, newline="", encoding="utf-8-sig") as fh:
                text = fh.read()
        else:
            text = (
                resources.files("deltaforce_dq")
                .joinpath("default_config", filename)
                .read_text(encoding="utf-8-sig")
            )
        rows = []
        for row in csv.DictReader(io.StringIO(text)):
            clean = {k.strip(): (v.strip() if v and v.strip() else None) for k, v in row.items() if k}
            if any(clean.values()):
                rows.append(clean)
        self._cache[key] = rows
        return rows

    def datasets(self) -> List[dict]:
        out = []
        for d in self.load("datasets"):
            d = dict(d)
            d["columns"] = parse_schema(d["schema"])
            d["table_suffix"] = d["dataset"].lower()
            if d.get("source_table"):
                # {catalog} and {schema} follow the run settings; with no catalog the prefix is dropped.
                name = d["source_table"].replace("{schema}", self.schema)
                name = name.replace("{catalog}.", f"{self.catalog}." if self.catalog else "")
                d["source_table"] = name
            out.append(d)
        return out

    def dataset(self, name: str) -> dict:
        for d in self.datasets():
            if d["dataset"] == name:
                return d
        raise KeyError(f"Dataset '{name}' is not in monitored_datasets.csv")

    def rules(self) -> List[dict]:
        out = []
        for r in self.load("rules"):
            if (r.get("active") or "Y").upper() != "Y":
                continue
            r = dict(r)
            r["params"] = parse_params(r.get("params"))
            out.append(r)
        return out


def parse_params(text: Optional[str]) -> Dict[str, str]:
    """'a=1;b=x|y' -> {'a': '1', 'b': 'x|y'}. Only the first '=' in each part splits."""
    params: Dict[str, str] = {}
    if not text:
        return params
    for part in text.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            params[k.strip()] = v.strip()
    return params


def parse_schema(text: str) -> List[tuple]:
    """'A:string|B:int' -> [('A', 'string'), ('B', 'int')]."""
    cols = []
    for part in text.split("|"):
        name, _, dtype = part.partition(":")
        cols.append((name.strip(), (dtype or "string").strip().lower()))
    return cols


# ---- table helpers ----------------------------------------------------------------
def ensure_namespace(spark, settings: Settings) -> None:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.namespace}")
    except Exception:
        # No right to create schemas is fine as long as the schema is already there.
        if not spark.catalog.databaseExists(settings.namespace):
            raise


def write_table(df, settings: Settings, name: str, mode: str = "overwrite") -> None:
    writer = df.write.mode(mode)
    if settings.table_format:
        writer = writer.format(settings.table_format)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(settings.table(name))


def read_table(spark, settings: Settings, name: str):
    return spark.table(settings.table(name))


def table_exists(spark, settings: Settings, name: str) -> bool:
    return spark.catalog.tableExists(settings.table(name))
