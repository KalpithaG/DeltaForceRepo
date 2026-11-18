"""Review decisions: approve, modify with re-check, reject. Detection must stay unchanged."""
import os
import sys
import tempfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tests"))

from deltaforce_dq import Settings  # noqa: E402
from deltaforce_dq import review  # noqa: E402
from deltaforce_dq.pipeline import run_all  # noqa: E402

SETTINGS = Settings(landing_path=os.path.join(ROOT, "sample_data", "landing"),
                    config_path=os.path.join(ROOT, "resources"), schema="rev")
APPLY = ["quarantine_certify", "dashboard_views"]


@pytest.fixture(scope="module")
def spark():
    from run_local import make_spark

    warehouse = tempfile.mkdtemp(prefix="dfdq_rev_")
    os.chdir(warehouse)
    session = make_spark(warehouse)
    session.sparkContext.setLogLevel("ERROR")
    run_all(session, SETTINGS, verbose=False)
    yield session
    session.stop()


def _row_id(spark, sales_id):
    return spark.sql(f"SELECT _row_id FROM rev.quarantine_sales WHERE Sales_ID = '{sales_id}'").first()[0]


def test_queue_starts_pending(spark):
    queue = spark.table("rev.vw_quarantine_queue").collect()
    assert len(queue) == 8  # 7 sales rows and the duplicate customer C005
    assert {r.review_status for r in queue} == {"PENDING"}
    assert not any(r.awaiting_hub_run for r in queue)
    s006 = [r for r in queue if r.record_key == "S006"][0]
    assert '"Quantity":"-1"' in s006.record_json and '"Sale_Date":"2026-11-05"' in s006.record_json
    s009 = [r for r in queue if r.record_key == "S009"][0]
    assert '"Sale_Date":null' in s009.record_json  # nulls are kept, so the app can show every field


def test_decisions_are_applied(spark):
    review.record_decision(spark, SETTINGS, "Sales", _row_id(spark, "S004"), "APPROVE",
                           comment="New customer C999, onboarding in progress", reviewer="tester", record_key="S004")
    review.record_decision(spark, SETTINGS, "Sales", _row_id(spark, "S006"), "MODIFY",
                           corrections={"Quantity": "5", "Revenue": "1250"}, reviewer="tester", record_key="S006")
    review.record_decision(spark, SETTINGS, "Sales", _row_id(spark, "S008"), "MODIFY",
                           corrections={"Quantity": "1", "Revenue": "120"}, reviewer="tester", record_key="S008")
    review.record_decision(spark, SETTINGS, "Sales", _row_id(spark, "S009"), "MODIFY",
                           corrections={"Customer_ID": "C002"}, reviewer="tester", record_key="S009")
    review.record_decision(spark, SETTINGS, "Sales", _row_id(spark, "S010"), "REJECT",
                           comment="Wrong file", reviewer="tester", record_key="S010")
    assert spark.sql("SELECT COUNT(*) FROM rev.vw_quarantine_queue WHERE awaiting_hub_run").first()[0] == 5

    run_all(spark, SETTINGS, stages=APPLY, verbose=False)

    status = {r.Sales_ID: (r._review_status, list(r._recheck_failed))
              for r in spark.table("rev.quarantine_sales").collect()}
    assert status["S004"] == ("APPROVED", [])
    assert status["S006"] == ("CORRECTED", [])
    assert status["S008"] == ("CORRECTED", [])           # no longer a duplicate of S007 within the same file
    assert status["S009"] == ("CORRECTION_FAILED", ["sale_date_missing"])  # the date is still missing
    assert status["S010"] == ("REJECTED", [])
    assert status["S005"] == ("PENDING", [])

    cert = {r.Sales_ID: r for r in spark.table("rev.certified_sales").collect()}
    assert cert["S004"].review_status == "APPROVED" and cert["S004"].Revenue == 1500
    assert (cert["S006"].Quantity, cert["S006"].Revenue, cert["S006"].review_status) == (5, 1250, "CORRECTED")
    assert (cert["S008"].Quantity, cert["S008"].Revenue) == (1, 120)
    assert "S009" not in cert and "S010" not in cert

    day = spark.sql("SELECT * FROM rev.certification_summary WHERE dataset = 'Sales' "
                    "AND file_date = DATE'2026-11-05'").first()
    assert day.raw_measure == 256900                     # what arrived never changes
    assert day.certified_measure == 9950 + 1500 + 1250 + 120
    assert (day.certified_rows, day.quarantined_rows, day.released_rows) == (6, 4, 3)
    assert spark.sql("SELECT COUNT(*) FROM rev.vw_quarantine_queue WHERE awaiting_hub_run").first()[0] == 0


def test_detection_is_untouched(spark):
    assert spark.sql("SELECT COUNT(*) FROM rev.detection_scorecard WHERE NOT caught").first()[0] == 0
    assert spark.sql("SELECT COUNT(*) FROM rev.alert_log").first()[0] == 26


def test_latest_decision_wins(spark):
    review.record_decision(spark, SETTINGS, "Sales", _row_id(spark, "S010"), "APPROVE",
                           comment="Confirmed with the data owner", reviewer="tester2", record_key="S010")
    run_all(spark, SETTINGS, stages=APPLY, verbose=False)
    row = spark.sql("SELECT _review_status, _reviewed_by FROM rev.quarantine_sales WHERE Sales_ID = 'S010'").first()
    assert (row[0], row[1]) == ("APPROVED", "tester2")


def test_bad_decisions_are_refused(spark):
    with pytest.raises(ValueError):
        review.record_decision(spark, SETTINGS, "Sales", "x", "MAYBE")
    with pytest.raises(ValueError):
        review.record_decision(spark, SETTINGS, "Sales", "x", "MODIFY", corrections={})
