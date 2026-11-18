"""End-to-end check on the 12 sample files: every official scenario must be caught.

    pip install pyspark pytest && pytest tests/
"""
import os
import sys
import tempfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tests"))


@pytest.fixture(scope="module")
def spark():
    from run_local import make_spark

    from deltaforce_dq import Settings
    from deltaforce_dq.pipeline import run_all

    warehouse = tempfile.mkdtemp(prefix="dfdq_test_")
    os.chdir(warehouse)
    session = make_spark(warehouse)
    session.sparkContext.setLogLevel("ERROR")
    settings = Settings(
        landing_path=os.path.join(ROOT, "sample_data", "landing"),
        config_path=os.path.join(ROOT, "resources"),
        schema="dataops",
    )
    run_all(session, settings, verbose=False)
    yield session
    session.stop()


def _alerts(spark):
    return {(str(r.file_date), r.dataset, r.rule) for r in spark.table("dataops.alert_log").collect()}


def test_every_seeded_issue_is_caught(spark):
    missed = spark.sql("SELECT issue_id FROM dataops.detection_scorecard WHERE NOT caught").collect()
    assert missed == []


def test_official_scenarios(spark):
    alerts = _alerts(spark)
    assert ("2026-11-05", "Sales", "invalid_customer_ref") in alerts
    assert ("2026-11-05", "Sales", "duplicate_sale") in alerts
    assert ("2026-11-06", "Customer", "file_late") in alerts
    assert ("2026-11-07", "Sales", "sales_volume_drop") in alerts
    assert ("2026-11-08", "Sales", "revenue_spike") in alerts


def test_clean_day_raises_no_sales_alerts(spark):
    assert not [a for a in _alerts(spark) if a[0] == "2026-11-06" and a[1] == "Sales"]


def test_revenue_before_and_after(spark):
    rows = {str(r.file_date): r for r in spark.table("dataops.vw_revenue_before_after").collect()}
    assert rows["2026-11-05"].reported_without_hub == 256900
    assert rows["2026-11-05"].certified_by_hub == 9950
    assert rows["2026-11-06"].certified_by_hub == 52400
    assert rows["2026-11-08"].certified_by_hub == 0
    assert rows["2026-11-08"].held_measure == 2789500


def test_trust_status(spark):
    rows = {(str(r.file_date), r.dataset): r.trust_status for r in spark.table("dataops.trust_score").collect()}
    assert rows[("2026-11-05", "Sales")] == "DO NOT REPORT"
    assert rows[("2026-11-06", "Sales")] == "SAFE TO REPORT"
    assert rows[("2026-11-06", "Customer")] == "USE WITH CAUTION"
    assert rows[("2026-11-08", "Sales")] == "DO NOT REPORT"


def test_source_tables_are_created_from_the_files(spark):
    for name, rows in (("sales", 29), ("customers", 29), ("product", 29)):
        table = spark.table(f"default.{name}")
        assert table.count() == rows
        assert {"source_file", "file_arrival_ts", "load_ts"} <= set(table.columns)
    files = {r[0] for r in spark.sql("SELECT DISTINCT source_file FROM default.sales").collect()}
    assert files == {f"Sales_0{d}112026_080000.csv" for d in (5, 6, 7, 8)}
