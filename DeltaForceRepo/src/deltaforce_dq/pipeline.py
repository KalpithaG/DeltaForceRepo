"""Runs the hub end to end, or one stage at a time, and logs each stage's own run."""

from __future__ import annotations

import time
import traceback
from datetime import datetime, timezone

from . import alerts, certify, dq_engine, file_monitor, ingest, job_monitor, load_tables, trust, views
from .config import Settings, ensure_namespace, write_table

STAGES = [
    ("load_source_tables", load_tables.run),
    ("ingest", ingest.run),
    ("file_monitoring", file_monitor.run),
    ("dq_engine", dq_engine.run),
    ("job_monitor", job_monitor.run),
    ("generate_alerts", alerts.run),
    ("trust_score", trust.run),
    ("quarantine_certify", certify.run),
    ("dashboard_views", views.run),
]
STAGE_NAMES = [name for name, _ in STAGES]
LOG_SCHEMA = "run_ts string, stage string, status string, duration_sec double, error string"


def run_stage(spark, settings: Settings, stage: str) -> None:
    dict(STAGES)[stage](spark, settings)


def run_all(spark, settings: Settings, stages=None, verbose: bool = True) -> list:
    """Run the stages in order. Stops at the first failure and re-raises it."""
    ensure_namespace(spark, settings)
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log, error = [], None
    for name, fn in STAGES:
        if stages and name not in stages:
            continue
        started = time.time()
        try:
            fn(spark, settings)
            log.append((run_ts, name, "SUCCESS", round(time.time() - started, 1), None))
            if verbose:
                print(f"[deltaforce_dq] {name:<20} OK   {log[-1][3]:>6.1f}s")
        except Exception as exc:  # noqa: BLE001 - logged, then re-raised below
            log.append((run_ts, name, "FAILED", round(time.time() - started, 1), traceback.format_exc()[-2000:]))
            error = exc
            if verbose:
                print(f"[deltaforce_dq] {name:<20} FAILED: {exc}")
            break
    write_table(spark.createDataFrame(log, LOG_SCHEMA), settings, "hub_run_log", mode="append")
    if error:
        raise error
    return log
