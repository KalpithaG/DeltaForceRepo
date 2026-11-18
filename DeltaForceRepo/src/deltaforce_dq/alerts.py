"""Stage 4 - turn every finding into one row in the alert log.

Output
------
alert_log  alert_id, dataset, severity, rule, message, timestamp
           + file_date, category, rule_id, failed_count, observed_value, expected_value, status
"""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from .config import Settings, read_table, write_table


def run(spark, settings: Settings) -> None:
    files = read_table(spark, settings, "file_monitor_results")
    fmt = "HH:mm"

    file_alerts = files.filter(F.col("arrival_status").isin("LATE", "MISSING", "EMPTY")).select(
        "dataset",
        "severity",
        F.concat(F.lit("file_"), F.lower("arrival_status")).alias("rule"),
        "message",
        "file_date",
        F.lit("file").alias("category"),
        F.lit(None).cast("string").alias("rule_id"),
        F.lit(1).cast("long").alias("failed_count"),
        F.coalesce(F.date_format("actual_ts", fmt), F.lit("not received")).alias("observed_value"),
        F.date_format("expected_ts", fmt).alias("expected_value"),
    )

    identical_severity = {c["dataset"]: c.get("severity_identical") or "Low" for c in settings.load("file_monitor")}
    sev_map = F.create_map(*[x for k, v in identical_severity.items() for x in (F.lit(k), F.lit(v))])
    identical_alerts = files.filter("identical_to_previous").select(
        "dataset",
        sev_map[F.col("dataset")].alias("severity"),
        F.lit("file_identical_content").alias("rule"),
        F.concat(F.col("dataset"), F.lit(" file content is identical to the previous file")).alias("message"),
        "file_date",
        F.lit("file").alias("category"),
        F.lit(None).cast("string").alias("rule_id"),
        F.lit(1).cast("long").alias("failed_count"),
        F.lit("identical").alias("observed_value"),
        F.lit("changed").alias("expected_value"),
    )

    dq_alerts = read_table(spark, settings, "dq_results").filter("status = 'FAIL'").select(
        "dataset",
        "severity",
        F.col("rule_name").alias("rule"),
        "message",
        "file_date",
        "category",
        "rule_id",
        "failed_count",
        "observed_value",
        "expected_value",
    )

    job_alerts = read_table(spark, settings, "job_monitor_results").select(
        F.col("job_name").alias("dataset"),
        "severity",
        F.col("issue").alias("rule"),
        "message",
        F.col("run_date").alias("file_date"),
        F.lit("job").alias("category"),
        F.lit(None).cast("string").alias("rule_id"),
        F.lit(1).cast("long").alias("failed_count"),
        "observed_value",
        "expected_value",
    )

    alerts = file_alerts.unionByName(identical_alerts).unionByName(dq_alerts).unionByName(job_alerts)
    cat_order = (
        F.when(F.col("category") == "file", 1)
        .when(F.col("category") == "row", 2)
        .when(F.col("category") == "cross_table", 3)
        .when(F.col("category") == "trend", 4)
        .otherwise(5)
    )
    w = Window.orderBy("file_date", cat_order, "dataset", "rule")
    alerts = alerts.withColumn("_n", F.row_number().over(w)).select(
        F.format_string("A%04d", F.col("_n")).alias("alert_id"),
        "dataset",
        "severity",
        "rule",
        "message",
        F.current_timestamp().alias("timestamp"),
        "file_date",
        "category",
        "rule_id",
        "failed_count",
        "observed_value",
        "expected_value",
        F.lit("OPEN").alias("status"),
    )
    write_table(alerts, settings, "alert_log")
