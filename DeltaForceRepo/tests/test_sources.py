"""Alternative sources: data pre-loaded into tables, and job history from system tables.
The system tables are stood in for by local tables with the documented columns."""
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tests"))
LANDING = os.path.join(ROOT, "sample_data", "landing")


@pytest.fixture(scope="module")
def spark():
    from run_local import make_spark

    warehouse = tempfile.mkdtemp(prefix="dfdq_src_")
    os.chdir(warehouse)
    session = make_spark(warehouse)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _config_with(tmp, extra):
    """Copy resources/ and set the source columns of monitored_datasets.csv per dataset."""
    import csv

    cfg = os.path.join(tmp, "config")
    shutil.copytree(os.path.join(ROOT, "resources"), cfg)
    path = os.path.join(cfg, "monitored_datasets.csv")
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    for row in rows:
        row.update({"load_from_files": "N", "file_name_column": "", "file_date_expr": "", "arrival_ts_expr": ""})
        row.update(extra[row["dataset"]])
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return cfg


def _preload(spark, with_file_name):
    spark.sql("CREATE SCHEMA IF NOT EXISTS preloaded")
    for name in ("Customer", "Product", "Sales"):
        df = spark.read.option("header", "true").option("pathGlobFilter", f"{name}_*.csv").csv(LANDING)
        df = df.selectExpr("*", "_metadata.file_path AS source_file") if with_file_name else df.selectExpr(
            "*", "to_date(regexp_extract(_metadata.file_name, '_(\\\\d{8})_', 1), 'ddMMyyyy') AS load_date"
        )
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"preloaded.{name.lower()}")


def test_tables_with_file_name_column(spark, tmp_path):
    from deltaforce_dq import Settings
    from deltaforce_dq.pipeline import run_all

    _preload(spark, with_file_name=True)
    cfg = _config_with(str(tmp_path), {n: {"source_table": f"preloaded.{n.lower()}", "file_name_column": "source_file"} for n in ("Customer", "Product", "Sales")})
    run_all(spark, Settings(landing_path="unused", config_path=cfg, schema="from_tables"), verbose=False)
    assert spark.sql("SELECT COUNT(*) FROM from_tables.detection_scorecard WHERE NOT caught").first()[0] == 0
    assert spark.sql("SELECT COUNT(*) FROM from_tables.alert_log").first()[0] == 26


def test_tables_with_date_expression_only(spark, tmp_path):
    from deltaforce_dq import Settings
    from deltaforce_dq.pipeline import run_all

    _preload(spark, with_file_name=False)
    cfg = _config_with(str(tmp_path), {n: {"source_table": f"preloaded.{n.lower()}", "file_date_expr": "load_date"} for n in ("Customer", "Product", "Sales")})
    run_all(spark, Settings(landing_path="unused", config_path=cfg, schema="from_dates"), verbose=False)
    missed = {r[0] for r in spark.sql("SELECT rule_name FROM from_dates.detection_scorecard WHERE NOT caught").collect()}
    assert missed == {"file_late"}  # no arrival time without a file name; everything else is still caught


def test_job_history_from_system_tables(spark):
    from deltaforce_dq import Settings
    from deltaforce_dq.pipeline import run_all

    now = datetime.now().replace(microsecond=0)

    def ts(minutes_ago):
        return (now - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")

    spark.sql("CREATE SCHEMA IF NOT EXISTS fake_system")
    spark.createDataFrame(
        [("w1", "11", "Hub nightly", ts(9000)), ("w1", "11", "DataOps Hub", ts(600)), ("w1", "12", "Report refresh", ts(600))],
        "workspace_id string, job_id string, name string, change_time string",
    ).selectExpr("workspace_id", "job_id", "name", "cast(change_time as timestamp) change_time").write.mode(
        "overwrite").saveAsTable("fake_system.jobs")
    rows = [
        ("w1", "11", "r1", ts(500), ts(495), "SUCCEEDED", "SUCCESS"),
        ("w1", "11", "r2", ts(400), ts(395), "SUCCEEDED", "SUCCESS"),
        ("w1", "11", "r3", ts(300), ts(294), "SUCCEEDED", "SUCCESS"),
        ("w1", "11", "r4", ts(200), ts(140), None, None),              # long run: first hourly slice
        ("w1", "11", "r4", ts(140), ts(120), "SUCCEEDED", "SUCCESS"),  # long run: final slice
        ("w1", "12", "r5", ts(100), ts(98), "FAILED", "RUN_EXECUTION_ERROR"),
        ("w1", "12", "r6", ts(10), ts(5), None, None),                 # still running: ignored
    ]
    spark.createDataFrame(
        rows,
        "workspace_id string, job_id string, run_id string, s string, e string, result_state string, termination_code string",
    ).selectExpr(
        "workspace_id", "job_id", "run_id", "cast(s as timestamp) period_start_time",
        "cast(e as timestamp) period_end_time", "result_state", "termination_code",
    ).write.mode("overwrite").saveAsTable("fake_system.job_run_timeline")

    settings = Settings(
        landing_path=LANDING, config_path=os.path.join(ROOT, "resources"), schema="with_jobs", job_source="both",
        system_jobs_table="fake_system.jobs", system_job_runs_table="fake_system.job_run_timeline",
    )
    run_all(spark, settings, verbose=False)
    real = {r.run_id: r for r in spark.sql("SELECT * FROM with_jobs.job_runs WHERE source = 'system_tables'").collect()}
    assert set(real) == {"r1", "r2", "r3", "r4", "r5"}
    assert real["r4"].job_name == "DataOps Hub" and real["r4"].duration_min == 80.0
    issues = {(r.run_id, r.issue) for r in spark.sql("SELECT * FROM with_jobs.job_monitor_results").collect()}
    assert ("r5", "job_failed") in issues
    assert ("r4", "job_long_running") in issues
    assert spark.sql("SELECT COUNT(*) FROM with_jobs.job_runs WHERE source = 'simulated'").first()[0] == 20
    assert not [i for i in issues if i[1] == "job_missed"]


def test_both_falls_back_when_system_tables_are_unreadable(spark):
    from deltaforce_dq import Settings
    from deltaforce_dq.pipeline import run_all

    settings = Settings(
        landing_path=LANDING, config_path=os.path.join(ROOT, "resources"), schema="fallback", job_source="both",
        system_jobs_table="no_such.jobs", system_job_runs_table="no_such.job_run_timeline",
    )
    run_all(spark, settings, verbose=False)
    assert spark.sql("SELECT COUNT(*) FROM fallback.job_runs").first()[0] == 20


def _layout(tmp, folders, strip_prefix=False):
    """Copy the sample files into one subfolder per dataset."""
    base = os.path.join(tmp, "inputdata")
    for f in sorted(os.listdir(LANDING)):
        dataset, rest = f.split("_", 1)
        target = os.path.join(base, folders[dataset])
        os.makedirs(target, exist_ok=True)
        shutil.copy(os.path.join(LANDING, f), os.path.join(target, rest if strip_prefix else f))
    return base


@pytest.mark.parametrize(
    "folders, strip_prefix",
    [
        ({"Sales": "Sales", "Customer": "Customers", "Product": "Product"}, False),
        ({"Sales": "sales", "Customer": "customers", "Product": "products"}, True),  # 05112026_080000.csv
    ],
)
def test_files_in_one_subfolder_per_dataset(spark, tmp_path, folders, strip_prefix):
    from deltaforce_dq import Settings
    from deltaforce_dq.pipeline import run_all

    base = _layout(str(tmp_path), folders, strip_prefix)
    schema = "sub_b" if strip_prefix else "sub_a"
    run_all(spark, Settings(landing_path=base, config_path=os.path.join(ROOT, "resources"), schema=schema), verbose=False)
    assert spark.table("default.sales").count() == 29
    assert spark.table("default.customers").count() == 29
    assert spark.table("default.product").count() == 29
    assert spark.sql(f"SELECT COUNT(*) FROM {schema}.detection_scorecard WHERE NOT caught").first()[0] == 0
    assert spark.sql(f"SELECT COUNT(*) FROM {schema}.alert_log").first()[0] == 26


def test_file_names_without_a_date_are_rejected_clearly(spark, tmp_path):
    from deltaforce_dq import Settings
    from deltaforce_dq.pipeline import run_all

    base = os.path.join(str(tmp_path), "inputdata", "Sales")
    os.makedirs(base)
    shutil.copy(os.path.join(LANDING, "Sales_05112026_080000.csv"), os.path.join(base, "sales.csv"))
    for f in os.listdir(LANDING):
        if not f.startswith("Sales"):
            shutil.copy(os.path.join(LANDING, f), os.path.join(str(tmp_path), "inputdata", f))
    with pytest.raises(ValueError, match="DDMMYYYY_HHMMSS"):
        run_all(spark, Settings(landing_path=os.path.join(str(tmp_path), "inputdata"),
                                config_path=os.path.join(ROOT, "resources"), schema="undated"), verbose=False)
