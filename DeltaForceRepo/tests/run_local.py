"""Run the hub on local open-source Spark and print the key outputs.

    python tests/run_local.py [landing_path] [config_path]
"""
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from pyspark.sql import SparkSession  # noqa: E402

from deltaforce_dq import Settings  # noqa: E402
from deltaforce_dq.pipeline import run_all  # noqa: E402


def make_spark(warehouse):
    return (
        SparkSession.builder.master("local[2]")
        .appName("deltaforce_dq_local")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


if __name__ == "__main__":
    landing = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "sample_data", "landing")
    config = sys.argv[2] if len(sys.argv) > 2 else os.path.join(ROOT, "resources")
    warehouse = tempfile.mkdtemp(prefix="dfdq_wh_")
    os.chdir(warehouse)
    spark = make_spark(warehouse)
    spark.sparkContext.setLogLevel("ERROR")
    settings = Settings(landing_path=landing, config_path=config, schema="dataops")
    run_all(spark, settings)

    def show(sql, n=100):
        spark.sql(sql).show(n, truncate=False)

    show("SELECT alert_id, file_date, dataset, category, severity, rule, message FROM dataops.alert_log ORDER BY alert_id")
    show("SELECT file_date, dataset, trust_score, trust_status, row_quality_pct, row_penalty_points, alert_penalty_points FROM dataops.trust_score ORDER BY dataset, file_date")
    show("SELECT * FROM dataops.vw_trust_overall_daily ORDER BY file_date")
    show("SELECT file_date, reported_without_hub, certified_by_hub, quarantined_measure, held_measure, raw_rows, certified_rows, quarantined_rows, held_rows FROM dataops.vw_revenue_before_after ORDER BY file_date")
    show("SELECT issue_id, file_date, dataset, rule_name, record_key, official_scenario, caught FROM dataops.detection_scorecard WHERE NOT caught ORDER BY issue_id")
    show("SELECT official_scenario, COUNT(*) issues, SUM(CASE WHEN caught THEN 1 ELSE 0 END) caught FROM dataops.detection_scorecard GROUP BY 1")
    show("SELECT run_date, job_name, status, duration_min, issues FROM dataops.vw_job_health ORDER BY run_date, job_name")
    spark.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
