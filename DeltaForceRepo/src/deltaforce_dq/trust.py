"""Stage 5 - the trust score and the detection scorecard.

Trust score (per dataset, per day), designed to be explained in one sentence:

    score = 100
            - 5 points for every 1% of rows that fail a quality rule
            - a penalty for every file-level or trend-level alert
              (Critical 50, High 30, Medium 15, Low 5)
    floored at 0

Trust by layer (same penalties, split by where the problem is):
    source_trust = 100 - file-level alert penalties          (late, missing, empty, re-sent)
    data_trust   = 100 - bad-row penalty - trend penalties   (quality of the processed data)
    job_trust    = 100 - job issue penalties                 (failed, SLA breach, long run, missed)
The overall trust_score covers source and data; job health is scored separately, because a
failed or slow job does not make the data itself wrong.

    90-100  SAFE TO REPORT      70-89  USE WITH CAUTION      below 70  DO NOT REPORT

Outputs
-------
trust_score           one row per dataset per day
detection_scorecard   every known seeded issue and whether the hub caught it
"""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from .config import (
    BAD_ROW_PENALTY_FACTOR,
    CAUTION_THRESHOLD,
    SAFE_THRESHOLD,
    SEVERITY_ORDER,
    SEVERITY_PENALTY,
    Settings,
    read_table,
    write_table,
)

SCORECARD_SCHEMA = (
    "issue_id string, file_date date, dataset string, rule_name string, record_key string, "
    "official_scenario string, description string"
)


def _lookup(mapping: dict, col):
    return F.create_map(*[x for k, v in mapping.items() for x in (F.lit(k), F.lit(v))])[col]


def run(spark, settings: Settings) -> None:
    alerts = read_table(spark, settings, "alert_log")
    grid = read_table(spark, settings, "file_monitor_results").select("dataset", "file_date", "arrival_status")
    metrics = read_table(spark, settings, "dq_daily_metrics").select(
        "dataset", "file_date", "total_rows", "bad_rows", "quarantined_rows", "bad_share"
    )

    data_alerts = alerts.filter(F.col("category") != "job")
    pts = _lookup(SEVERITY_PENALTY, F.col("severity"))
    penalties = (
        data_alerts.filter(F.col("category").isin("file", "trend"))
        .groupBy("dataset", "file_date")
        .agg(
            F.sum(pts).alias("penalty_points"),
            F.sum(F.when(F.col("category") == "file", pts).otherwise(0)).alias("file_penalty_points"),
            F.sum(F.when(F.col("category") == "trend", pts).otherwise(0)).alias("trend_penalty_points"),
        )
    )
    # Job issues count against the dataset the job serves; jobs for "All" count against every dataset.
    names = [d["dataset"] for d in settings.datasets()]
    job_pen = (
        read_table(spark, settings, "job_monitor_results")
        .withColumn(
            "_ds",
            F.explode(F.when(F.col("dataset").isin(*names), F.array(F.col("dataset")))
                      .otherwise(F.array(*[F.lit(n) for n in names]))),
        )
        .groupBy(F.col("_ds").alias("dataset"), F.col("run_date").alias("file_date"))
        .agg(F.sum(pts).alias("job_penalty_points"))
    )
    w = Window.partitionBy("dataset", "file_date").orderBy(
        _lookup(SEVERITY_ORDER, F.col("severity")).desc(), F.col("alert_id")
    )
    summary = (
        data_alerts.withColumn("_n", F.row_number().over(w))
        .groupBy("dataset", "file_date")
        .agg(
            F.count(F.lit(1)).alias("alert_count"),
            F.sum(F.when(F.col("severity") == "Critical", 1).otherwise(0)).alias("critical_alerts"),
            F.max(F.when(F.col("_n") == 1, F.col("message"))).alias("top_issue"),
        )
    )

    score = (
        grid.join(metrics, ["dataset", "file_date"], "left")
        .join(penalties, ["dataset", "file_date"], "left")
        .join(summary, ["dataset", "file_date"], "left")
        .join(job_pen, ["dataset", "file_date"], "left")
        .withColumn("row_quality_pct", F.round(100 * (1 - F.coalesce(F.col("bad_share"), F.lit(0.0))), 1))
        .withColumn(
            "row_penalty_points",
            F.round(F.least(F.lit(100.0), (100 - F.col("row_quality_pct")) * BAD_ROW_PENALTY_FACTOR), 0),
        )
        .withColumn("alert_penalty_points", F.coalesce(F.col("penalty_points"), F.lit(0)).cast("double"))
        .withColumn(
            "trust_score",
            F.greatest(F.lit(0.0), 100 - F.col("row_penalty_points") - F.col("alert_penalty_points")),
        )
        .withColumn("file_penalty_points", F.coalesce(F.col("file_penalty_points"), F.lit(0)).cast("double"))
        .withColumn("trend_penalty_points", F.coalesce(F.col("trend_penalty_points"), F.lit(0)).cast("double"))
        .withColumn("job_penalty_points", F.coalesce(F.col("job_penalty_points"), F.lit(0)).cast("double"))
        .withColumn("source_trust", F.greatest(F.lit(0.0), 100 - F.col("file_penalty_points")))
        .withColumn("data_trust", F.greatest(
            F.lit(0.0), 100 - F.col("row_penalty_points") - F.col("trend_penalty_points")))
        .withColumn("job_trust", F.greatest(F.lit(0.0), 100 - F.col("job_penalty_points")))
        .withColumn(
            "trust_status",
            F.when(F.col("trust_score") >= SAFE_THRESHOLD, "SAFE TO REPORT")
            .when(F.col("trust_score") >= CAUTION_THRESHOLD, "USE WITH CAUTION")
            .otherwise("DO NOT REPORT"),
        )
        .select(
            "dataset", "file_date", "trust_score", "trust_status", "row_quality_pct", "row_penalty_points",
            "alert_penalty_points", "source_trust", "data_trust", "job_trust",
            "file_penalty_points", "trend_penalty_points", "job_penalty_points",
            F.coalesce("total_rows", F.lit(0)).alias("total_rows"),
            F.coalesce("bad_rows", F.lit(0)).alias("bad_rows"),
            F.coalesce("quarantined_rows", F.lit(0)).alias("quarantined_rows"),
            F.coalesce("alert_count", F.lit(0)).alias("alert_count"),
            F.coalesce("critical_alerts", F.lit(0)).alias("critical_alerts"),
            "arrival_status",
            F.coalesce("top_issue", F.lit("No issues")).alias("top_issue"),
        )
    )
    write_table(score, settings, "trust_score")

    # ---- detection scorecard ---------------------------------------------------
    expected_rows = [
        (e["issue_id"], e["file_date"], e["dataset"], e["rule_name"], e.get("record_key"),
         e.get("official_scenario") or "N", e.get("description"))
        for e in settings.load("expected_issues")
    ]
    expected = spark.createDataFrame(
        expected_rows, SCORECARD_SCHEMA.replace("file_date date", "file_date string")
    ).withColumn("file_date", F.expr("try_cast(file_date as date)"))

    row_hits = (
        read_table(spark, settings, "dq_failed_rows")
        .select("dataset", "file_date", "rule_name", "record_key")
        .distinct()
        .withColumn("_row_hit", F.lit(True))
    )
    alert_hits = (
        alerts.select("dataset", "file_date", F.col("rule").alias("rule_name"), "alert_id", "severity")
        .dropDuplicates(["dataset", "file_date", "rule_name"])
    )
    scorecard = (
        expected.join(row_hits, ["dataset", "file_date", "rule_name", "record_key"], "left")
        .join(alert_hits, ["dataset", "file_date", "rule_name"], "left")
        .withColumn(
            "caught",
            F.when(F.col("record_key").isNotNull(), F.coalesce(F.col("_row_hit"), F.lit(False)))
            .otherwise(F.col("alert_id").isNotNull()),
        )
        .select(
            "issue_id", "file_date", "dataset", "rule_name", "record_key", "official_scenario",
            "description", "caught", "alert_id", "severity",
        )
    )
    write_table(scorecard, settings, "detection_scorecard")
