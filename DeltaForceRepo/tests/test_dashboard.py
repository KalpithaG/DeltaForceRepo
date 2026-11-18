"""The dashboard definition: every dataset and every widget query must run against the hub's
tables, every widget must refer to fields it queries, and the layout must fit the grid.
deploy() is checked against a stand-in workspace client (no real workspace here)."""
import json
import os
import sys
import tempfile
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tests"))

from deltaforce_dq import Settings  # noqa: E402
from deltaforce_dq import dashboard  # noqa: E402

SETTINGS = Settings(landing_path=os.path.join(ROOT, "sample_data", "landing"),
                    config_path=os.path.join(ROOT, "resources"), schema="dash")


@pytest.fixture(scope="module")
def spark():
    from run_local import make_spark

    from deltaforce_dq.pipeline import run_all

    warehouse = tempfile.mkdtemp(prefix="dfdq_dash_")
    os.chdir(warehouse)
    session = make_spark(warehouse)
    session.sparkContext.setLogLevel("ERROR")
    run_all(session, SETTINGS, verbose=False)
    yield session
    session.stop()


def _widgets(defn):
    for page in defn["pages"]:
        for item in page["layout"]:
            yield page, item


def test_definition_is_json_and_names_are_unique():
    defn = dashboard.build_definition(SETTINGS)
    json.dumps(defn)
    names = [w["widget"]["name"] for _, w in _widgets(defn)]
    assert len(names) == len(set(names))
    assert len({d["name"] for d in defn["datasets"]}) == len(defn["datasets"])


def test_layout_fits_the_grid_without_overlaps():
    defn = dashboard.build_definition(SETTINGS)
    for page in defn["pages"]:
        cells = set()
        for item in page["layout"]:
            p = item["position"]
            assert p["x"] >= 0 and p["x"] + p["width"] <= dashboard.GRID_WIDTH
            for x in range(p["x"], p["x"] + p["width"]):
                for y in range(p["y"], p["y"] + p["height"]):
                    assert (x, y) not in cells, f"overlap on {page['displayName']} at {(x, y)}"
                    cells.add((x, y))


def test_datasets_run_and_return_the_declared_columns(spark):
    declared = {d["name"]: d for d in dashboard._datasets(SETTINGS)}
    kinds = {"string": ("string",), "integer": ("bigint", "int"), "float": ("double",)}
    for d in dashboard.build_definition(SETTINGS)["datasets"]:
        df = spark.sql("".join(d["queryLines"]))
        df.collect()
        got = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        want = declared[d["name"]]["columns"]
        assert [g[0] for g in got] == [w[0] for w in want], d["name"]
        for (name, sql_type), (_, kind) in zip(got, want):
            assert sql_type in kinds[kind], f"{d['name']}.{name}: {sql_type} is not {kind}"


def test_every_widget_query_runs_and_matches_its_encodings(spark):
    defn = dashboard.build_definition(SETTINGS)
    sql = {d["name"]: "".join(d["queryLines"]) for d in defn["datasets"]}
    for _, item in _widgets(defn):
        w = item["widget"]
        if "queries" not in w:
            continue
        q = w["queries"][0]["query"]
        fields = q["fields"]
        select = ", ".join(f"{f['expression']} AS `{f['name']}`" for f in fields)
        dims = [f["expression"] for f in fields if not f["expression"].upper().startswith("SUM(")]
        stmt = f"SELECT {select} FROM ({sql[q['datasetName']]})"
        if not q["disaggregated"] and dims:
            stmt += " GROUP BY " + ", ".join(dims)
        rows = spark.sql(stmt).collect()
        assert rows, w["name"]
        names = {f["name"] for f in fields}
        enc = w["spec"]["encodings"]
        used = [c["fieldName"] for c in enc["columns"]] if "columns" in enc else [e["fieldName"] for e in enc.values()]
        assert set(used) <= names, w["name"]


def _one_row(spark, defn, name):
    sql = {d["name"]: "".join(d["queryLines"]) for d in defn["datasets"]}
    rows = spark.sql(sql[name]).collect()
    assert len(rows) == 1, name
    return rows[0].asDict()


def test_kpi_values(spark):
    defn = dashboard.build_definition(SETTINGS)
    exec_ = _one_row(spark, defn, "kpi_exec")
    critical = spark.sql("SELECT COUNT(*) FROM dash.alert_log WHERE severity = 'Critical'").first()[0]
    assert exec_["trust_latest"] == 50 and exec_["critical_alerts"] == critical
    assert exec_["files_missing"] == 0 and exec_["revenue_kept_out"] == 256900 - 9950 + 2789500
    assert _one_row(spark, defn, "kpi_alerts")["seeded_caught"] == 23
    rev = _one_row(spark, defn, "kpi_revenue")
    assert rev["revenue_before"] == 256900 + 52400 + 15900 + 2789500
    assert rev["revenue_after"] == 9950 + 52400 + 15900
    assert rev["impact_pct"] == 97.5
    trust = _one_row(spark, defn, "kpi_trust")
    assert (trust["trust_latest"], trust["change_vs_previous"]) == (50, 10)
    assert (trust["source_trust_latest"], trust["data_trust_latest"], trust["job_trust_latest"]) == (100, 50, 80)
    assert _one_row(spark, defn, "kpi_files")["source_trust_latest"] == 100
    assert _one_row(spark, defn, "kpi_jobs")["job_trust_latest"] == 80
    files = _one_row(spark, defn, "kpi_files")
    assert (files["files_expected"], files["files_delayed"], files["files_missing"]) == (12, 1, 0)
    jobs = _one_row(spark, defn, "kpi_jobs")
    assert jobs["total_runs"] == 20 and jobs["failed_runs"] == 1
    dq = _one_row(spark, defn, "kpi_dq")
    assert dq["rules_executed"] == dq["rules_passed"] + dq["rules_failed"]


def test_revenue_by_region_reconciles(spark):
    defn = dashboard.build_definition(SETTINGS)
    sql = {d["name"]: "".join(d["queryLines"]) for d in defn["datasets"]}
    totals = {r[0]: r[1] for r in spark.sql(
        f"SELECT measure, SUM(amount) FROM ({sql['ds_revenue_region']}) GROUP BY measure").collect()}
    assert totals[dashboard.BEFORE] == 3114700 and totals[dashboard.AFTER] == 78250


def test_seven_pages():
    names = [p["displayName"] for p in dashboard.build_definition(SETTINGS)["pages"]]
    assert names == ["Executive overview", "Data quality", "Alerts and anomalies", "Source monitoring",
                     "Job monitoring", "Revenue impact", "Trust score"]


class _FakeApi:
    def __init__(self, exists):
        self.exists, self.calls = exists, []

    def do(self, method, path, query=None, body=None):
        self.calls.append((method, path, body))
        if path == "/api/2.0/workspace/get-status":
            if not self.exists:
                raise RuntimeError("RESOURCE_DOES_NOT_EXIST")
            return {"object_type": "DASHBOARD", "resource_id": "existing123"}
        if method == "POST" and path == "/api/2.0/lakeview/dashboards":
            return {"dashboard_id": "new456"}
        return {}


def _client(exists):
    warehouses = [SimpleNamespace(id="w_stopped", name="a", state="State.STOPPED", enable_serverless_compute=True),
                  SimpleNamespace(id="w_running", name="b", state="State.RUNNING", enable_serverless_compute=False)]
    return SimpleNamespace(
        api_client=_FakeApi(exists),
        warehouses=SimpleNamespace(list=lambda: warehouses),
        current_user=SimpleNamespace(me=lambda: SimpleNamespace(user_name="someone@example.com")),
        config=SimpleNamespace(host="https://example.cloud.databricks.com/"),
    )


def test_deploy_creates_then_publishes():
    client = _client(exists=False)
    out = dashboard.deploy(SETTINGS, client=client)
    methods = [(m, p) for m, p, _ in client.api_client.calls]
    assert ("POST", "/api/2.0/lakeview/dashboards") in methods
    assert ("POST", "/api/2.0/lakeview/dashboards/new456/published") in methods
    create = [b for m, p, b in client.api_client.calls if p == "/api/2.0/lakeview/dashboards"][0]
    assert create["parent_path"] == "/Users/someone@example.com"
    assert create["warehouse_id"] == "w_running"
    assert json.loads(create["serialized_dashboard"])["pages"]
    assert out["url"] == "https://example.cloud.databricks.com/dashboardsv3/new456/published"


def test_deploy_updates_an_existing_dashboard():
    client = _client(exists=True)
    out = dashboard.deploy(SETTINGS, client=client, warehouse_id="w1", parent_path="/Workspace/Shared/team9",
                           publish=False)
    methods = [(m, p) for m, p, _ in client.api_client.calls]
    assert ("PATCH", "/api/2.0/lakeview/dashboards/existing123") in methods
    assert not [p for _, p in methods if p.endswith("/published")]
    assert out["action"] == "updated" and out["path"] == "/Shared/team9/Delta Force DataOps Hub.lvdash.json"


def test_import_file_matches_the_code():
    """sql/DeltaForce_DataOps_Hub.lvdash.json must be regenerated whenever the dashboard code changes."""
    team = Settings(landing_path="/Volumes/team_9/default/inputdata", catalog="team_9", schema="default")
    with open(os.path.join(ROOT, "sql", "DeltaForce_DataOps_Hub.lvdash.json")) as fh:
        assert json.load(fh) == dashboard.build_definition(team)


def test_trust_by_layer(spark):
    rows = {(str(r.file_date), r.dataset): r for r in spark.table("dash.trust_score").collect()}
    c6 = rows[("2026-11-06", "Customer")]
    assert (c6.trust_score, c6.source_trust, c6.data_trust, c6.job_trust) == (80, 85, 95, 85)  # late file, slow ingest
    s7 = rows[("2026-11-07", "Sales")]
    assert (s7.trust_score, s7.data_trust, s7.job_trust) == (40, 40, 50)  # volume drop, report job failed
    for (day, _), r in rows.items():
        assert r.trust_score == max(0, 100 - r.row_penalty_points - r.file_penalty_points - r.trend_penalty_points)


def test_dimensions_and_columns(spark):
    defn = dashboard.build_definition(SETTINGS)
    sql = {d["name"]: "".join(d["queryLines"]) for d in defn["datasets"]}
    dims = {r[0]: r[1] for r in spark.sql(sql["ds_dimensions"]).collect()}
    assert set(dims) == {"Completeness", "Uniqueness", "Validity", "Integrity", "Consistency",
                         "Volume and anomalies", "Timeliness"}
    assert dims["Timeliness"] == 91.7
    cols = spark.sql(sql["ds_columns"]).collect()
    assert cols[0].failed_records >= cols[-1].failed_records
    inventory = spark.sql(sql["ds_sources"]).collect()
    assert {r.dataset for r in inventory} == {"Customer", "Product", "Sales"}
    assert [r.deliveries_late for r in inventory if r.dataset == "Customer"] == [1]
