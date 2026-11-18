"""Stage 3 - job monitoring: failures, SLA breaches, long runs and missed runs.

Job run history can come from four places (``Settings.job_source``):

  simulated      a repeatable history generated from jobs_config.csv. Ingestion jobs wait for
                 their file, so a late or missing file shows up as a late or failed job too.
  system_tables  real runs from system.lakeflow.job_run_timeline, named via system.lakeflow.jobs.
  both           the simulated history plus the real runs.
  existing       a job_runs table you loaded yourself, with the same columns.

Jobs listed in jobs_config.csv are checked against their schedule and SLA. Real jobs that are
not listed are still checked for failures, and for long runs against their own median duration.

Outputs
-------
job_runs             one row per job run (column ``source`` says where it came from)
job_monitor_results  one row per problem found
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from statistics import median

from pyspark.sql import Window
from pyspark.sql import functions as F

from .config import Settings, read_table, table_exists, write_table

RUN_SCHEMA = (
    "run_id string, job_name string, dataset string, run_date date, scheduled_ts string, start_ts string, "
    "end_ts string, duration_min double, status string, error_message string, source string"
)
RUN_COLUMNS = [c.split()[0] for c in RUN_SCHEMA.split(", ")]
RESULT_SCHEMA = (
    "job_name string, dataset string, run_date date, run_id string, issue string, severity string, "
    "observed_value string, expected_value string, message string"
)
_FMT = "%Y-%m-%d %H:%M:%S"


def simulated_runs(spark, settings: Settings):
    files = (
        read_table(spark, settings, "file_monitor_results")
        .withColumn("actual_ts", F.date_format("actual_ts", "yyyy-MM-dd HH:mm:ss"))
        .collect()
    )
    days = sorted({f["file_date"] for f in files})
    arrivals = {(f["dataset"], f["file_date"]): f for f in files}
    rng = random.Random(42)
    runs = []

    for idx, day in enumerate(days, start=1):
        for job in settings.load("jobs"):
            h, m, s = (int(x) for x in job["schedule_time"].split(":"))
            scheduled = datetime(day.year, day.month, day.day, h, m, s)
            typical = float(job["typical_duration_min"])
            duration = round(typical * rng.uniform(0.85, 1.2), 1)
            start, status, error = scheduled, "SUCCESS", None

            if (job.get("waits_for_file") or "N").upper() == "Y":
                f = arrivals.get((job["dataset"], day))
                if f is None or f["arrival_status"] == "MISSING":
                    status, error, duration = "FAILED", f"Source file for {job['dataset']} not found", 1.0
                elif f["actual_ts"]:
                    arrived = datetime.strptime(f["actual_ts"], _FMT)
                    start = max(scheduled, arrived + timedelta(minutes=1))

            if job.get("sim_slow_day") and int(job["sim_slow_day"]) == idx:
                duration = round(typical * 3.4, 1)
            if job.get("sim_fail_day") and int(job["sim_fail_day"]) == idx:
                status, error = "FAILED", "Upstream validation failed: input volume outside expected range"
                duration = round(typical * 0.3, 1)

            end = start + timedelta(minutes=duration)
            runs.append(
                (f"{job['job_name']}-{day:%Y%m%d}", job["job_name"], job["dataset"], day,
                 scheduled.strftime(_FMT), start.strftime(_FMT), end.strftime(_FMT), duration, status, error,
                 "simulated")
            )

    df = spark.createDataFrame(runs, RUN_SCHEMA)
    for c in ("scheduled_ts", "start_ts", "end_ts"):
        df = df.withColumn(c, F.expr(f"try_cast({c} as timestamp)"))
    return df


def system_table_runs(spark, settings: Settings):
    """Real job runs. Long runs are stored as hourly slices, so slices are rolled up per run;
    the result state is only present on the last slice."""
    timeline = spark.table(settings.system_job_runs_table).filter(
        F.col("period_start_time") >= F.expr(f"current_timestamp() - INTERVAL {int(settings.job_lookback_days)} DAYS")
    )
    runs = (
        timeline.groupBy("workspace_id", "job_id", "run_id")
        .agg(
            F.min("period_start_time").alias("start_ts"),
            F.max("period_end_time").alias("end_ts"),
            F.max("result_state").alias("result_state"),
            F.max("termination_code").alias("termination_code"),
        )
        .filter(F.col("result_state").isNotNull())  # finished runs only
    )
    w = Window.partitionBy("workspace_id", "job_id").orderBy(F.col("change_time").desc())
    names = (
        spark.table(settings.system_jobs_table)
        .withColumn("_n", F.row_number().over(w))
        .filter("_n = 1")
        .select("workspace_id", "job_id", F.col("name").alias("job_name"))
    )
    cfg_rows = [(j["job_name"], j["dataset"], j["schedule_time"]) for j in settings.load("jobs")]
    cfg = spark.createDataFrame(cfg_rows, "job_name string, cfg_dataset string, schedule_time string")

    return (
        runs.join(names, ["workspace_id", "job_id"], "left")
        .withColumn("job_name", F.coalesce(F.col("job_name"), F.concat(F.lit("job_"), F.col("job_id"))))
        .join(cfg, "job_name", "left")
        .withColumn("run_date", F.to_date("start_ts"))
        .withColumn("_sched", F.concat_ws(" ", F.col("run_date").cast("string"), F.col("schedule_time")))
        .select(
            F.col("run_id").cast("string").alias("run_id"),
            "job_name",
            F.coalesce(F.col("cfg_dataset"), F.lit("All")).alias("dataset"),
            "run_date",
            F.when(F.col("schedule_time").isNotNull(), F.expr("try_cast(_sched as timestamp)")).alias("scheduled_ts"),
            "start_ts",
            "end_ts",
            F.round((F.unix_timestamp("end_ts") - F.unix_timestamp("start_ts")) / 60.0, 1).alias("duration_min"),
            F.when(F.col("result_state") == "SUCCEEDED", "SUCCESS").otherwise("FAILED").alias("status"),
            F.when(
                F.col("result_state") != "SUCCEEDED",
                F.concat_ws(" / ", F.col("result_state"), F.col("termination_code")),
            ).alias("error_message"),
            F.lit("system_tables").alias("source"),
        )
    )


def _load_runs(spark, settings: Settings) -> None:
    source = settings.job_source
    if source == "existing":
        if not table_exists(spark, settings, "job_runs"):
            raise ValueError("job_source='existing' but there is no job_runs table")
        return
    if source == "simulated":
        df = simulated_runs(spark, settings)
    elif source == "system_tables":
        df = system_table_runs(spark, settings)
    elif source == "both":
        df = simulated_runs(spark, settings)
        try:
            real = system_table_runs(spark, settings)
            real.limit(1).collect()  # fail here, not later, if the tables are not readable
            df = df.unionByName(real)
        except Exception as exc:  # noqa: BLE001
            print(f"[deltaforce_dq] WARNING: system tables not readable, using simulated job runs only: {exc}")
    else:
        raise ValueError(f"Unknown job_source '{source}'")
    write_table(df.select(*RUN_COLUMNS), settings, "job_runs")


def run(spark, settings: Settings) -> None:
    _load_runs(spark, settings)

    fmt = "yyyy-MM-dd HH:mm:ss"
    runs = [
        r.asDict()
        for r in read_table(spark, settings, "job_runs")
        .withColumn("scheduled_ts", F.date_format("scheduled_ts", fmt))
        .withColumn("end_ts", F.date_format("end_ts", fmt))
        .collect()
    ]
    config = {j["job_name"]: j for j in settings.load("jobs")}
    by_job = {}
    for r in runs:
        by_job.setdefault(r["job_name"], []).append(r)
    issues = []

    # A listed job is expected on every day that listed jobs from the same source ran.
    listed_runs = [r for r in runs if r["job_name"] in config]

    for name in sorted(set(config) | set(by_job)):
        job = config.get(name)
        job_runs = sorted(by_job.get(name, []), key=lambda r: (r["run_date"], r["run_id"]))
        dataset = job["dataset"] if job else "All"
        sla = float(job["sla_minutes"]) if job else None
        criticality = (job.get("criticality") if job else None) or "High"
        if job:
            typical = float(job["typical_duration_min"])
        else:
            ok = [r["duration_min"] for r in job_runs if r["status"] == "SUCCESS" and r["duration_min"] is not None]
            typical = median(ok) if len(ok) >= 3 else None

        if job:
            ran_on = {r["run_date"] for r in job_runs}
            sources = {r["source"] for r in job_runs}
            expected_days = sorted({r["run_date"] for r in listed_runs if not sources or r["source"] in sources})
            for day in expected_days:
                if day not in ran_on:
                    issues.append((name, dataset, day, None, "job_missed", "High", "no run", "1 run",
                                   f"Job {name} did not run on {day:%d %b}"))

        for r in job_runs:
            day = r["run_date"]
            if r["status"] == "FAILED":
                issues.append((name, dataset, day, r["run_id"], "job_failed", criticality, "FAILED", "SUCCESS",
                               f"Job {name} failed: {r['error_message']}"))
                continue
            if sla is not None and r["scheduled_ts"] and r["end_ts"]:
                scheduled = datetime.strptime(r["scheduled_ts"], _FMT)
                end = datetime.strptime(r["end_ts"], _FMT)
                minutes_after = (end - scheduled).total_seconds() / 60.0
                if minutes_after > sla:
                    issues.append((name, dataset, day, r["run_id"], "job_sla_breach", "Medium",
                                   f"{minutes_after:.0f} min", f"{sla:.0f} min",
                                   f"Job {name} finished {minutes_after:.0f} min after schedule (SLA {sla:.0f} min)"))
            if typical and r["duration_min"] is not None and r["duration_min"] > 2 * typical and r["duration_min"] >= 1:
                issues.append((name, dataset, day, r["run_id"], "job_long_running", "Low",
                               f"{r['duration_min']:.0f} min", f"{typical:.0f} min",
                               f"Job {name} ran {r['duration_min']:.0f} min against a typical {typical:.0f} min"))

    write_table(spark.createDataFrame(issues, RESULT_SCHEMA), settings, "job_monitor_results")
