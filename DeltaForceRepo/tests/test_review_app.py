"""Drive the review app with Streamlit's test harness. The SQL warehouse is replaced by local
Spark, so this checks the app's queries, its flow and the hub's handling of what it writes."""
import os
import re
import sys
import tempfile
from types import SimpleNamespace

import pytest

st_testing = pytest.importorskip("streamlit.testing.v1")
pytest.importorskip("databricks.sql")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tests"))
APP = os.path.join(ROOT, "apps", "quarantine_review", "app.py")

from deltaforce_dq import Settings  # noqa: E402
from deltaforce_dq.pipeline import run_all  # noqa: E402

SETTINGS = Settings(landing_path=os.path.join(ROOT, "sample_data", "landing"),
                    config_path=os.path.join(ROOT, "resources"), schema="appt")


@pytest.fixture(scope="module")
def spark():
    from run_local import make_spark

    warehouse = tempfile.mkdtemp(prefix="dfdq_app_")
    os.chdir(warehouse)
    session = make_spark(warehouse)
    session.sparkContext.setLogLevel("ERROR")
    run_all(session, SETTINGS, verbose=False)
    yield session
    session.stop()


class _Cursor:
    def __init__(self, spark):
        self.spark, self.description, self._rows = spark, None, []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, statement, params=None):
        def lit(m):
            v = (params or {})[m.group(1)]
            return "NULL" if v is None else "'" + str(v).replace("'", "\\'") + "'"
        df = self.spark.sql(re.sub(r":(\w+)", lit, statement))
        if statement.lstrip().upper().startswith("INSERT"):
            self.description, self._rows = None, []
        else:
            self.description = [(f.name,) for f in df.schema.fields]
            self._rows = [tuple(r) for r in df.collect()]

    def fetchall(self):
        return self._rows


@pytest.fixture()
def app(spark, monkeypatch):
    import databricks.sdk.core
    import databricks.sql

    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "test")
    monkeypatch.setenv("HUB_CATALOG", "spark_catalog")
    monkeypatch.setenv("HUB_SCHEMA", "appt")
    monkeypatch.setattr(databricks.sql, "connect", lambda **kw: SimpleNamespace(cursor=lambda: _Cursor(spark)))
    monkeypatch.setattr(databricks.sdk.core, "Config", lambda: SimpleNamespace(host="https://example", authenticate=None))
    at = st_testing.AppTest.from_file(APP, default_timeout=120)
    at.run()
    assert not at.exception, at.exception
    return at


def test_lists_the_queue(app):
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Quarantined records"] == "8" and metrics["Pending review"] == "8"
    assert "S004" in app.dataframe[0].value["Record"].tolist()


def _save(at):
    [b for b in at.button if b.label == "Save decision"][0].click().run()


def _choose(at, key):
    box = [b for b in at.selectbox if b.label == "Record"][0]
    target = [o for o in box.options if f"· {key} ·" in o][0]
    box.select_index(box.options.index(target)).run()


def test_approve_and_correct_through_the_app(app, spark):
    _choose(app, "S005")
    app.radio[0].set_value("Approve as received").run()
    app.text_area[0].input("Product P999 is a new launch, confirmed")
    _save(app)
    assert not app.exception, app.exception

    _choose(app, "S007")
    app.radio[0].set_value("Correct the values").run()
    fields = {t.label: t for t in app.text_input}
    fields["Quantity"].input("10")
    fields["Revenue"].input("1200")
    _save(app)
    assert not app.exception, app.exception

    decisions = {r.record_key: r for r in spark.table("appt.quarantine_decisions").collect()}
    assert decisions["S005"].decision == "APPROVE" and decisions["S005"].comment
    assert decisions["S007"].decision == "MODIFY"
    assert decisions["S007"].corrections == '{"Quantity": "10", "Revenue": "1200"}'

    run_all(spark, SETTINGS, stages=["quarantine_certify", "dashboard_views"], verbose=False)
    status = {r.Sales_ID: r._review_status for r in spark.table("appt.quarantine_sales").collect()}
    assert status["S005"] == "APPROVED" and status["S007"] == "CORRECTED"


def test_refuses_an_unexplained_approval(app, spark):
    _choose(app, "S010")
    app.radio[0].set_value("Reject").run()
    _save(app)
    assert any("comment" in e.value for e in app.error)
    assert "S010" not in {r.record_key for r in spark.table("appt.quarantine_decisions").collect()}
