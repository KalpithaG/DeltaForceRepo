"""Review decisions for quarantined records: approve, modify (with re-check) or reject.

Decisions are appended to ``quarantine_decisions`` by the review app or the review notebook.
The hub never overwrites that table; it applies the latest decision per record on every run:

  APPROVE  the record is released into the certified table exactly as received
  MODIFY   the corrected values are re-checked against every record-level rule, within the full
           day's data (so duplicates and references are judged properly). If the record now passes
           it is certified with the corrected values; if not, it stays quarantined and the rules it
           still fails are recorded
  REJECT   the record stays quarantined, marked as reviewed

Detection (alerts, scorecard, trust score) always runs on the data as received, so the audit
trail of what arrived is never changed. Only certification follows the review.
"""

from __future__ import annotations

import json
import uuid
from typing import Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .config import Settings, read_table

DECISIONS = "quarantine_decisions"
DECISION_COLUMNS = (
    "decision_id string, dataset string, row_id string, record_key string, decision string, "
    "corrections string, comment string, reviewer string, decided_at timestamp"
)
VALID_DECISIONS = ("APPROVE", "MODIFY", "REJECT")


def ensure_decisions_table(spark, settings: Settings) -> None:
    using = f" USING {settings.table_format}" if settings.table_format else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {settings.table(DECISIONS)} ({DECISION_COLUMNS}){using}")


def record_decision(spark, settings: Settings, dataset: str, row_id: str, decision: str,
                    corrections: Optional[Dict[str, str]] = None, comment: str = "",
                    reviewer: Optional[str] = None, record_key: Optional[str] = None) -> str:
    """Append one decision (used by the review notebook and the tests; the app writes the same row via SQL)."""
    decision = decision.upper()
    if decision not in VALID_DECISIONS:
        raise ValueError(f"decision must be one of {VALID_DECISIONS}")
    if decision == "MODIFY" and not corrections:
        raise ValueError("MODIFY needs at least one corrected value")
    ensure_decisions_table(spark, settings)
    decision_id = str(uuid.uuid4())
    row = [(decision_id, dataset, row_id, record_key, decision,
            json.dumps(corrections) if corrections else None, comment or None, reviewer)]
    df = spark.createDataFrame(row, DECISION_COLUMNS.replace(", decided_at timestamp", "")).withColumn(
        "decided_at", F.current_timestamp())
    if reviewer is None:
        df = df.withColumn("reviewer", F.expr("current_user()"))
    df.write.mode("append").insertInto(settings.table(DECISIONS))
    return decision_id


def latest_decisions(spark, settings: Settings) -> DataFrame:
    ensure_decisions_table(spark, settings)
    w = Window.partitionBy("dataset", "row_id").orderBy(F.col("decided_at").desc(), F.col("decision_id").desc())
    return (
        read_table(spark, settings, DECISIONS)
        .withColumn("_n", F.row_number().over(w))
        .filter("_n = 1")
        .drop("_n")
        .withColumn("decision", F.upper("decision"))
    )


def apply_corrections(raw: DataFrame, ds: dict, corrections: DataFrame) -> DataFrame:
    """Overlay corrected values (JSON object column -> text) on the raw rows they belong to.
    A field set to an empty string becomes NULL; fields not mentioned keep their original value."""
    cols = [c for c, _ in ds["columns"]]
    fixes = corrections.select(
        F.col("row_id").alias("_fix_row_id"),
        F.from_json("corrections", "map<string,string>").alias("_fix"),
    )
    joined = raw.join(fixes, raw["_row_id"] == fixes["_fix_row_id"], "left")
    out = []
    for c in cols:
        has = F.col("_fix").isNotNull() & F.map_contains_key(F.col("_fix"), F.lit(c))
        fixed = F.nullif(F.trim(F.col("_fix")[c]), F.lit(""))
        out.append(F.when(has, fixed).otherwise(F.col(c)).alias(c))
    others = [c for c in raw.columns if c not in cols]
    return joined.select(*out, *others)


def recheck(spark, settings: Settings, modify: DataFrame) -> DataFrame:
    """Re-run every record-level quarantine rule with the corrections applied.
    Returns (dataset, row_id, still_failing array) for the corrected records only."""
    from . import dq_engine  # local import: dq_engine imports config only

    empty = spark.createDataFrame([], "dataset string, row_id string, still_failing array<string>")
    if not modify.limit(1).collect():
        return empty
    datasets = {d["dataset"]: d for d in settings.datasets()}
    views = {}
    for name, ds in datasets.items():
        raw = read_table(spark, settings, f"raw_{ds['table_suffix']}")
        fixes = modify.filter(F.col("dataset") == name)
        views[name] = dq_engine.typed_from_raw(apply_corrections(raw, ds, fixes), ds)

    ids = modify.select("dataset", F.col("row_id").alias("_rid")).distinct()
    failures = None
    for rule in settings.rules():
        if rule["action"] != "quarantine" or rule["rule_type"] in ("metric_change", "key_disappeared"):
            continue
        hits = dq_engine.apply_rule(rule, datasets[rule["dataset"]], views, settings).select(
            F.lit(rule["dataset"]).alias("dataset"), F.col("row_id"), F.lit(rule["rule_name"]).alias("rule_name"))
        failures = hits if failures is None else failures.unionByName(hits)
    if failures is None:
        return empty
    return (
        failures.join(ids, (failures["dataset"] == ids["dataset"]) & (failures["row_id"] == ids["_rid"]), "left_semi")
        .groupBy("dataset", "row_id")
        .agg(F.sort_array(F.collect_set("rule_name")).alias("still_failing"))
    )
