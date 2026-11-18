"""Stage 1 - file arrival monitoring: missing, late and identical-content files.

Output
------
file_monitor_results  one row per dataset per expected day
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import functions as F

from .config import Settings, read_table, write_table

RESULT_SCHEMA = (
    "dataset string, file_date date, file_name string, expected_ts string, actual_ts string, "
    "delay_minutes double, row_count long, arrival_status string, identical_to_previous boolean, "
    "severity string, message string"
)


def run(spark, settings: Settings) -> None:
    # Timestamps travel as text between Spark and Python so no time-zone conversion can shift them.
    fmt = "yyyy-MM-dd HH:mm:ss"
    inv_df = (
        read_table(spark, settings, "file_inventory")
        .withColumn("file_ts", F.date_format("file_ts", fmt))
        .withColumn("landing_ts", F.date_format("landing_ts", fmt))
    )
    inventory = []
    for r in inv_df.collect():
        d = r.asDict()
        for k in ("file_ts", "landing_ts"):
            d[k] = datetime.strptime(d[k], "%Y-%m-%d %H:%M:%S") if d[k] else None
        inventory.append(d)
    dated = [r for r in inventory if r["file_date"] is not None]
    if not dated:
        write_table(spark.createDataFrame([], RESULT_SCHEMA), settings, "file_monitor_results")
        return

    first_day = min(r["file_date"] for r in dated)
    last_day = max(r["file_date"] for r in dated)
    days = [first_day + timedelta(days=i) for i in range((last_day - first_day).days + 1)]

    results = []
    for cfg in settings.load("file_monitor"):
        dataset = cfg["dataset"]
        grace = float(cfg.get("grace_minutes") or 0)
        source = (cfg.get("arrival_time_source") or "filename").lower()
        exp_h, exp_m, exp_s = (int(x) for x in (cfg.get("expected_time") or "00:00:00").split(":"))
        by_day = {}
        for r in dated:
            if r["dataset"] == dataset:
                # If a file is delivered twice for one day, monitor the earliest arrival.
                current = by_day.get(r["file_date"])
                if current is None or (r["file_ts"] and current["file_ts"] and r["file_ts"] < current["file_ts"]):
                    by_day[r["file_date"]] = r

        previous = None
        for day in days:
            expected_ts = datetime(day.year, day.month, day.day, exp_h, exp_m, exp_s)
            f = by_day.get(day)
            if f is None:
                results.append(
                    (dataset, day, None, _s(expected_ts), None, None, None, "MISSING", False,
                     cfg.get("severity_missing") or "Critical",
                     f"No {dataset} file received for {day:%d %b %Y}")
                )
                continue

            actual_ts = f["landing_ts"] if source == "landing" else f["file_ts"]
            delay = (actual_ts - expected_ts).total_seconds() / 60.0 if actual_ts else None
            late = delay is not None and delay > grace
            identical = (
                (cfg.get("check_identical_content") or "N").upper() == "Y"
                and previous is not None
                and previous["content_fingerprint"] == f["content_fingerprint"]
            )
            if late:
                status, severity = "LATE", cfg.get("severity_late") or "Medium"
                message = (
                    f"{dataset} file arrived {delay:.0f} min after the expected {expected_ts:%H:%M} "
                    f"(allowed {grace:.0f} min)"
                )
            elif actual_ts is None:
                status, severity, message = "ARRIVAL_UNKNOWN", None, f"{dataset} data received; arrival time not recorded"
            else:
                status, severity, message = "ON_TIME", None, f"{dataset} file arrived on time"
            if f["row_count"] == 0:
                status, severity, message = "EMPTY", cfg.get("severity_missing") or "Critical", f"{dataset} file is empty"

            results.append(
                (dataset, day, f["file_name"], _s(expected_ts), _s(actual_ts), delay, f["row_count"], status,
                 bool(identical), severity, message)
            )
            previous = f

    out = (
        spark.createDataFrame(results, RESULT_SCHEMA)
        .withColumn("expected_ts", F.expr("try_cast(expected_ts as timestamp)"))
        .withColumn("actual_ts", F.expr("try_cast(actual_ts as timestamp)"))
    )
    write_table(out, settings, "file_monitor_results")


def _s(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S") if ts else None
