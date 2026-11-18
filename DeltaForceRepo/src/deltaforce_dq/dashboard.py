"""Create the Databricks AI/BI dashboard from the hub's views.

    from deltaforce_dq.dashboard import deploy
    deploy(settings)                       # creates (or updates) and publishes the dashboard

``build_definition(settings)`` returns the dashboard as a Python dict in the Databricks
``.lvdash.json`` format; ``write_definition`` saves it to a file you can import by hand.
``deploy`` sends it to the workspace through the Lakeview REST API, using the notebook's
own credentials, and runs on a SQL warehouse (picked automatically if not given).

Seven pages: Executive overview, Data quality, Alerts and anomalies, Source monitoring,
Job monitoring, Revenue impact, Trust score. "Latest day" means the latest file date in the data.
"""

from __future__ import annotations

import json
from typing import Dict, List, Optional

from .config import SEVERITY_PENALTY, Settings

DEFAULT_NAME = "Delta Force DataOps Hub"
GRID_WIDTH = 12  # AI/BI grid with layoutVersion GRID_V1

STRING, INTEGER, FLOAT = "string", "integer", "float"

BEFORE, AFTER = "Revenue before (as received)", "Revenue after (certified)"


# ---------------------------------------------------------------------------------
# Datasets: one SQL query each, every date already formatted as text
# ---------------------------------------------------------------------------------
def _datasets(settings: Settings) -> List[dict]:
    t = settings.table
    penalty = "CASE severity " + " ".join(
        f"WHEN '{sev}' THEN {pts}" for sev, pts in SEVERITY_PENALTY.items()
    ) + " ELSE 0 END"
    latest = lambda view, col="file_date": f"(SELECT MAX({col}) FROM {t(view)})"  # noqa: E731

    def ds(name, display, sql, columns):
        return {"name": name, "displayName": display, "sql": sql, "columns": columns}

    return [
        # ---- one-row KPI datasets ------------------------------------------------------
        ds("kpi_exec", "KPIs: executive overview", f"""
SELECT
  (SELECT CAST(overall_trust_score AS DOUBLE) FROM {t('vw_trust_overall_daily')}
   ORDER BY file_date DESC LIMIT 1) AS trust_latest,
  (SELECT COUNT(*) FROM {t('vw_alerts')} WHERE status = 'OPEN' AND file_date = {latest('vw_alerts')}) AS open_alerts_latest,
  (SELECT COUNT(*) FROM {t('vw_alerts')} WHERE severity = 'Critical') AS critical_alerts,
  (SELECT CAST(ROUND(100 * SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)
          / SUM(CASE WHEN status IN ('PASS', 'FAIL') THEN 1 ELSE 0 END), 1) AS DOUBLE)
   FROM {t('vw_dq_rule_results')}) AS dq_pass_rate,
  (SELECT COUNT(*) FROM {t('vw_job_health')} WHERE status = 'FAILED'
   AND run_date = {latest('vw_job_health', 'run_date')}) AS jobs_failed_latest,
  (SELECT COUNT(*) FROM {t('vw_file_arrivals')} WHERE arrival_status = 'MISSING') AS files_missing,
  (SELECT CAST(SUM(quarantined_measure + held_measure) AS DOUBLE) FROM {t('vw_revenue_before_after')}) AS revenue_kept_out""",
           [("trust_latest", FLOAT), ("open_alerts_latest", INTEGER), ("critical_alerts", INTEGER),
            ("dq_pass_rate", FLOAT), ("jobs_failed_latest", INTEGER), ("files_missing", INTEGER),
            ("revenue_kept_out", FLOAT)]),
        ds("kpi_dq", "KPIs: data quality", f"""
SELECT
  SUM(CASE WHEN status IN ('PASS', 'FAIL') THEN 1 ELSE 0 END) AS rules_executed,
  SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS rules_passed,
  SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS rules_failed,
  CAST(ROUND(100 * SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)
       / SUM(CASE WHEN status IN ('PASS', 'FAIL') THEN 1 ELSE 0 END), 1) AS DOUBLE) AS pass_pct,
  SUM(CASE WHEN status = 'FAIL' AND severity IN ('Critical', 'High') THEN 1 ELSE 0 END) AS critical_failures
FROM {t('vw_dq_rule_results')}""",
           [("rules_executed", INTEGER), ("rules_passed", INTEGER), ("rules_failed", INTEGER),
            ("pass_pct", FLOAT), ("critical_failures", INTEGER)]),
        ds("kpi_alerts", "KPIs: alerts and anomalies", f"""
SELECT
  (SELECT COUNT(*) FROM {t('vw_alerts')} WHERE status = 'OPEN') AS active_alerts,
  (SELECT COUNT(*) FROM {t('vw_alerts')} WHERE severity = 'Critical') AS critical_alerts,
  (SELECT COUNT(*) FROM {t('vw_alerts')} WHERE category = 'trend' AND file_date = {latest('vw_alerts')}) AS new_anomalies_latest,
  (SELECT COUNT(*) FROM {t('vw_detection_scorecard')} WHERE caught) AS seeded_caught""",
           [("active_alerts", INTEGER), ("critical_alerts", INTEGER), ("new_anomalies_latest", INTEGER),
            ("seeded_caught", INTEGER)]),
        ds("kpi_files", "KPIs: source monitoring", f"""
SELECT a.*, b.source_trust_latest
FROM (
  SELECT
    COUNT(*) AS files_expected,
    SUM(CASE WHEN arrival_status <> 'MISSING' THEN 1 ELSE 0 END) AS files_received,
    SUM(CASE WHEN arrival_status = 'LATE' THEN 1 ELSE 0 END) AS files_delayed,
    SUM(CASE WHEN arrival_status = 'MISSING' THEN 1 ELSE 0 END) AS files_missing,
    CAST(ROUND(100 * SUM(CASE WHEN arrival_status = 'ON_TIME' THEN 1 ELSE 0 END) / COUNT(*), 1) AS DOUBLE) AS sla_pct
  FROM {t('vw_file_arrivals')}
) a
CROSS JOIN (
  SELECT CAST(source_trust AS DOUBLE) AS source_trust_latest
  FROM {t('vw_trust_overall_daily')} ORDER BY file_date DESC LIMIT 1
) b""",
           [("files_expected", INTEGER), ("files_received", INTEGER), ("files_delayed", INTEGER),
            ("files_missing", INTEGER), ("sla_pct", FLOAT), ("source_trust_latest", FLOAT)]),
        ds("kpi_jobs", "KPIs: job monitoring", f"""
SELECT a.*, b.job_trust_latest
FROM (
  SELECT
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    CAST(ROUND(AVG(duration_min), 1) AS DOUBLE) AS avg_runtime_min
  FROM {t('vw_job_health')}
) a
CROSS JOIN (
  SELECT CAST(job_trust AS DOUBLE) AS job_trust_latest
  FROM {t('vw_trust_overall_daily')} ORDER BY file_date DESC LIMIT 1
) b""",
           [("total_runs", INTEGER), ("successful_runs", INTEGER), ("failed_runs", INTEGER),
            ("avg_runtime_min", FLOAT), ("job_trust_latest", FLOAT)]),
        ds("kpi_revenue", "KPIs: revenue impact", f"""
SELECT
  CAST(SUM(reported_without_hub) AS DOUBLE) AS revenue_before,
  CAST(SUM(certified_by_hub) AS DOUBLE) AS revenue_after,
  CAST(SUM(reported_without_hub) - SUM(certified_by_hub) AS DOUBLE) AS revenue_difference,
  CAST(ROUND(100 * (SUM(reported_without_hub) - SUM(certified_by_hub)) / SUM(reported_without_hub), 1) AS DOUBLE)
    AS impact_pct
FROM {t('vw_revenue_before_after')}""",
           [("revenue_before", FLOAT), ("revenue_after", FLOAT), ("revenue_difference", FLOAT),
            ("impact_pct", FLOAT)]),
        ds("kpi_trust", "KPIs: trust score", f"""
WITH o AS (
  SELECT CAST(overall_trust_score AS DOUBLE) AS score, CAST(source_trust AS DOUBLE) AS source_trust,
         CAST(data_trust AS DOUBLE) AS data_trust, CAST(job_trust AS DOUBLE) AS job_trust,
         ROW_NUMBER() OVER (ORDER BY file_date DESC) AS rn
  FROM {t('vw_trust_overall_daily')}
)
SELECT
  MAX(CASE WHEN rn = 1 THEN score END) AS trust_latest,
  MAX(CASE WHEN rn = 1 THEN source_trust END) AS source_trust_latest,
  MAX(CASE WHEN rn = 1 THEN data_trust END) AS data_trust_latest,
  MAX(CASE WHEN rn = 1 THEN job_trust END) AS job_trust_latest,
  MAX(CASE WHEN rn = 1 THEN score END) - MAX(CASE WHEN rn = 2 THEN score END) AS change_vs_previous
FROM o""",
           [("trust_latest", FLOAT), ("source_trust_latest", FLOAT), ("data_trust_latest", FLOAT),
            ("job_trust_latest", FLOAT), ("change_vs_previous", FLOAT)]),

        # ---- trust ----------------------------------------------------------------------
        ds("ds_trust_overall", "Overall trust score by day", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, CAST(overall_trust_score AS DOUBLE) AS overall_trust_score,
       overall_status, CAST(alerts AS BIGINT) AS alerts, CAST(critical_alerts AS BIGINT) AS critical_alerts,
       weakest_dataset, CAST(source_trust AS DOUBLE) AS source_trust, CAST(data_trust AS DOUBLE) AS data_trust,
       CAST(job_trust AS DOUBLE) AS job_trust
FROM {t('vw_trust_overall_daily')}
ORDER BY file_day""",
           [("file_day", STRING), ("overall_trust_score", FLOAT), ("overall_status", STRING), ("alerts", INTEGER),
            ("critical_alerts", INTEGER), ("weakest_dataset", STRING), ("source_trust", FLOAT),
            ("data_trust", FLOAT), ("job_trust", FLOAT)]),
        ds("ds_trust_layers", "Trust by layer", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, '1 Overall (source + data)' AS layer,
       CAST(overall_trust_score AS DOUBLE) AS score FROM {t('vw_trust_overall_daily')}
UNION ALL
SELECT date_format(file_date, 'yyyy-MM-dd'), '2 Source', CAST(source_trust AS DOUBLE) FROM {t('vw_trust_overall_daily')}
UNION ALL
SELECT date_format(file_date, 'yyyy-MM-dd'), '3 Processed data', CAST(data_trust AS DOUBLE)
FROM {t('vw_trust_overall_daily')}
UNION ALL
SELECT date_format(file_date, 'yyyy-MM-dd'), '4 Jobs', CAST(job_trust AS DOUBLE) FROM {t('vw_trust_overall_daily')}
ORDER BY 1, 2""",
           [("file_day", STRING), ("layer", STRING), ("score", FLOAT)]),
        ds("ds_trust", "Trust score by dataset and day", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, dataset,
       CAST(trust_score AS DOUBLE) AS trust_score, trust_status, CAST(row_quality_pct AS DOUBLE) AS row_quality_pct,
       CAST(total_rows AS BIGINT) AS total_rows, CAST(quarantined_rows AS BIGINT) AS quarantined_rows,
       CAST(alert_count AS BIGINT) AS alert_count, arrival_status, top_issue,
       CAST(source_trust AS DOUBLE) AS source_trust, CAST(data_trust AS DOUBLE) AS data_trust,
       CAST(job_trust AS DOUBLE) AS job_trust
FROM {t('vw_trust_score_daily')}
ORDER BY file_day, dataset""",
           [("file_day", STRING), ("dataset", STRING), ("trust_score", FLOAT), ("trust_status", STRING),
            ("row_quality_pct", FLOAT), ("total_rows", INTEGER), ("quarantined_rows", INTEGER),
            ("alert_count", INTEGER), ("arrival_status", STRING), ("top_issue", STRING),
            ("source_trust", FLOAT), ("data_trust", FLOAT), ("job_trust", FLOAT)]),
        ds("ds_trust_drivers", "Trust score drivers", f"""
SELECT file_day, driver, CAST(SUM(points) AS DOUBLE) AS points_lost
FROM (
  SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, '1 Data quality (bad rows)' AS driver,
         row_penalty_points AS points
  FROM {t('vw_trust_score_daily')}
  UNION ALL
  SELECT date_format(file_date, 'yyyy-MM-dd'),
         CASE WHEN category = 'file' THEN '2 File arrivals' ELSE '3 Anomalies (trend checks)' END,
         {penalty}
  FROM {t('vw_alerts')}
  WHERE category IN ('file', 'trend')
) d
GROUP BY file_day, driver
ORDER BY file_day, driver""",
           [("file_day", STRING), ("driver", STRING), ("points_lost", FLOAT)]),

        # ---- alerts and anomalies ------------------------------------------------------------
        ds("ds_alerts", "Alert log", f"""
SELECT alert_id, date_format(file_date, 'yyyy-MM-dd') AS file_day, dataset, category, severity, rule, message,
       observed_value, expected_value, status, CAST(severity_rank AS BIGINT) AS severity_rank,
       CASE
         WHEN rule IN ('file_missing', 'file_empty') THEN 'File missing'
         WHEN rule = 'file_late' THEN 'File late'
         WHEN rule = 'file_identical_content' THEN 'File re-sent'
         WHEN rule = 'sales_volume_drop' THEN 'Volume drop'
         WHEN rule IN ('revenue_drop', 'revenue_spike') THEN 'Revenue variance'
         WHEN category = 'trend' THEN 'Snapshot change'
         WHEN category = 'job' THEN 'Job failure or delay'
         ELSE 'Data quality failure'
       END AS alert_category,
       CAST(1 AS BIGINT) AS alert_one
FROM {t('vw_alerts')}
ORDER BY file_day, severity_rank DESC, alert_id""",
           [("alert_id", STRING), ("file_day", STRING), ("dataset", STRING), ("category", STRING),
            ("severity", STRING), ("rule", STRING), ("message", STRING), ("observed_value", STRING),
            ("expected_value", STRING), ("status", STRING), ("severity_rank", INTEGER),
            ("alert_category", STRING), ("alert_one", INTEGER)]),
        ds("ds_anomalies", "Anomalies by type", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day,
       CASE
         WHEN category = 'trend' AND rule LIKE '%volume%' THEN 'Volume anomaly'
         WHEN category = 'trend' AND rule LIKE 'revenue%' THEN 'Revenue anomaly'
         WHEN category = 'trend' THEN 'Snapshot change'
         ELSE 'Quality anomaly'
       END AS anomaly_type,
       CAST(1 AS BIGINT) AS anomaly_one
FROM {t('vw_alerts')}
WHERE category IN ('trend', 'row', 'cross_table')
ORDER BY file_day""",
           [("file_day", STRING), ("anomaly_type", STRING), ("anomaly_one", INTEGER)]),
        ds("ds_trend", "Trend checks", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, dataset, rule_name, status, observed_value,
       expected_value AS healthy_baseline, message
FROM {t('vw_dq_rule_results')}
WHERE category = 'trend' AND rule_name NOT LIKE '%disappeared'
ORDER BY file_day, dataset, rule_name""",
           [("file_day", STRING), ("dataset", STRING), ("rule_name", STRING), ("status", STRING),
            ("observed_value", STRING), ("healthy_baseline", STRING), ("message", STRING)]),
        ds("ds_scorecard", "Detection scorecard", f"""
SELECT issue_id, date_format(file_date, 'yyyy-MM-dd') AS file_day, dataset, description, rule_name, record_key,
       CASE WHEN official_scenario = 'Y' THEN 'Official scenario' ELSE 'Extra finding' END AS source,
       CASE WHEN caught THEN 'Caught' ELSE 'Missed' END AS result, alert_id, severity
FROM {t('vw_detection_scorecard')}
ORDER BY issue_id""",
           [("issue_id", STRING), ("file_day", STRING), ("dataset", STRING), ("description", STRING),
            ("rule_name", STRING), ("record_key", STRING), ("source", STRING), ("result", STRING),
            ("alert_id", STRING), ("severity", STRING)]),

        # ---- data quality ------------------------------------------------------------------
        ds("ds_rules", "Quality rule results", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, dataset, category, rule_name, status,
       CAST(failed_count AS BIGINT) AS failed_count,
       CAST(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END AS BIGINT) AS failed_rule
FROM {t('vw_dq_rule_results')}
ORDER BY file_day, dataset, rule_name""",
           [("file_day", STRING), ("dataset", STRING), ("category", STRING), ("rule_name", STRING),
            ("status", STRING), ("failed_count", INTEGER), ("failed_rule", INTEGER)]),
        ds("ds_dq_by_dataset", "Rule pass rate by dataset", f"""
SELECT dataset,
       CAST(ROUND(100 * SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) AS DOUBLE) AS pass_pct
FROM {t('vw_dq_rule_results')}
WHERE status IN ('PASS', 'FAIL')
GROUP BY dataset
ORDER BY dataset""",
           [("dataset", STRING), ("pass_pct", FLOAT)]),
        ds("ds_top_failed", "Top failed rules", f"""
SELECT rule_name, dataset, category, severity, CAST(SUM(failed_count) AS BIGINT) AS failure_count,
       CAST(COUNT(*) AS BIGINT) AS days_failed
FROM {t('vw_dq_rule_results')}
WHERE status = 'FAIL'
GROUP BY rule_name, dataset, category, severity
ORDER BY failure_count DESC, days_failed DESC, rule_name""",
           [("rule_name", STRING), ("dataset", STRING), ("category", STRING), ("severity", STRING),
            ("failure_count", INTEGER), ("days_failed", INTEGER)]),
        ds("ds_failed_records", "Failed records", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, dataset, rule_name, severity, action, record_key,
       failed_value, file_name
FROM {t('dq_failed_rows')}
ORDER BY file_day, dataset, rule_name, record_key""",
           [("file_day", STRING), ("dataset", STRING), ("rule_name", STRING), ("severity", STRING),
            ("action", STRING), ("record_key", STRING), ("failed_value", STRING), ("file_name", STRING)]),

        ds("ds_dimensions", "Pass rate by data quality dimension", f"""
SELECT dimension, CAST(ROUND(100 * SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) AS DOUBLE)
         AS pass_pct,
       CAST(COUNT(*) AS BIGINT) AS checks, CAST(SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS BIGINT) AS failed
FROM {t('vw_dq_rule_results')}
WHERE status IN ('PASS', 'FAIL')
GROUP BY dimension
UNION ALL
SELECT 'Timeliness', CAST(ROUND(100 * SUM(CASE WHEN arrival_status = 'ON_TIME' THEN 1 ELSE 0 END) / COUNT(*), 1)
         AS DOUBLE),
       CAST(COUNT(*) AS BIGINT), CAST(SUM(CASE WHEN arrival_status <> 'ON_TIME' THEN 1 ELSE 0 END) AS BIGINT)
FROM {t('vw_file_arrivals')}
ORDER BY 1""",
           [("dimension", STRING), ("pass_pct", FLOAT), ("checks", INTEGER), ("failed", INTEGER)]),
        ds("ds_columns", "Quality by column", f"""
SELECT dataset, column_name, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dimension))) AS dimensions,
       CAST(COUNT(*) AS BIGINT) AS checks,
       CAST(SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS BIGINT) AS failed_checks,
       CAST(SUM(CASE WHEN status = 'FAIL' THEN failed_count ELSE 0 END) AS BIGINT) AS failed_records,
       CAST(ROUND(100 * SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) AS DOUBLE) AS pass_pct
FROM {t('vw_dq_rule_results')}
WHERE status IN ('PASS', 'FAIL') AND column_name IS NOT NULL
GROUP BY dataset, column_name
ORDER BY failed_records DESC, failed_checks DESC, dataset, column_name""",
           [("dataset", STRING), ("column_name", STRING), ("dimensions", STRING), ("checks", INTEGER),
            ("failed_checks", INTEGER), ("failed_records", INTEGER), ("pass_pct", FLOAT)]),
        ds("ds_sources", "Source inventory", f"""
SELECT dataset, source_type, source_location, expected_time, CAST(grace_minutes AS BIGINT) AS grace_minutes,
       CAST(deliveries_expected AS BIGINT) AS deliveries_expected,
       CAST(deliveries_received AS BIGINT) AS deliveries_received,
       CAST(deliveries_late AS BIGINT) AS deliveries_late, CAST(deliveries_missing AS BIGINT) AS deliveries_missing,
       CAST(on_time_pct AS DOUBLE) AS on_time_pct, latest_status,
       CAST(source_trust_latest AS DOUBLE) AS source_trust_latest, CAST(source_trust_avg AS DOUBLE) AS source_trust_avg
FROM {t('vw_source_inventory')}
ORDER BY dataset""",
           [("dataset", STRING), ("source_type", STRING), ("source_location", STRING), ("expected_time", STRING),
            ("grace_minutes", INTEGER), ("deliveries_expected", INTEGER), ("deliveries_received", INTEGER),
            ("deliveries_late", INTEGER), ("deliveries_missing", INTEGER), ("on_time_pct", FLOAT),
            ("latest_status", STRING), ("source_trust_latest", FLOAT), ("source_trust_avg", FLOAT)]),

        # ---- files -------------------------------------------------------------------------
        ds("ds_files", "File arrivals", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, dataset, file_name,
       date_format(expected_ts, 'HH:mm') AS expected_time, date_format(actual_ts, 'HH:mm') AS actual_time,
       CAST(COALESCE(delay_minutes, 0) AS DOUBLE) AS delay_minutes, CAST(row_count AS BIGINT) AS row_count,
       arrival_status, CASE WHEN identical_to_previous THEN 'Yes' ELSE 'No' END AS identical_to_previous,
       CAST(1 AS BIGINT) AS file_one
FROM {t('vw_file_arrivals')}
ORDER BY file_day, dataset""",
           [("file_day", STRING), ("dataset", STRING), ("file_name", STRING), ("expected_time", STRING),
            ("actual_time", STRING), ("delay_minutes", FLOAT), ("row_count", INTEGER),
            ("arrival_status", STRING), ("identical_to_previous", STRING), ("file_one", INTEGER)]),
        ds("ds_files_delayed", "Files delayed or missing", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, dataset, COALESCE(file_name, 'not received') AS file_name,
       date_format(expected_ts, 'HH:mm') AS expected_time, COALESCE(date_format(actual_ts, 'HH:mm'), '-') AS actual_time,
       CAST(COALESCE(delay_minutes, 0) AS DOUBLE) AS delay_minutes, arrival_status
FROM {t('vw_file_arrivals')}
WHERE arrival_status IN ('LATE', 'MISSING', 'EMPTY')
ORDER BY file_day, dataset""",
           [("file_day", STRING), ("dataset", STRING), ("file_name", STRING), ("expected_time", STRING),
            ("actual_time", STRING), ("delay_minutes", FLOAT), ("arrival_status", STRING)]),

        # ---- jobs ----------------------------------------------------------------------------
        ds("ds_jobs", "Job runs", f"""
SELECT date_format(run_date, 'yyyy-MM-dd') AS run_day, job_name, dataset, status,
       date_format(start_ts, 'HH:mm') AS start_time, date_format(end_ts, 'HH:mm') AS end_time,
       CAST(duration_min AS DOUBLE) AS duration_min, issues, source, CAST(1 AS BIGINT) AS run_one
FROM {t('vw_job_health')}
ORDER BY run_day, job_name""",
           [("run_day", STRING), ("job_name", STRING), ("dataset", STRING), ("status", STRING),
            ("start_time", STRING), ("end_time", STRING), ("duration_min", FLOAT), ("issues", STRING),
            ("source", STRING), ("run_one", INTEGER)]),
        ds("ds_jobs_failed", "Failed jobs", f"""
SELECT date_format(run_date, 'yyyy-MM-dd') AS run_day, job_name, date_format(end_ts, 'HH:mm') AS failure_time,
       COALESCE(error_message, '') AS error_message, source
FROM {t('vw_job_health')}
WHERE status = 'FAILED'
ORDER BY run_day, job_name""",
           [("run_day", STRING), ("job_name", STRING), ("failure_time", STRING), ("error_message", STRING),
            ("source", STRING)]),
        ds("ds_jobs_slow", "Long-running and late jobs", f"""
SELECT date_format(run_date, 'yyyy-MM-dd') AS run_day, job_name,
       CASE issue WHEN 'job_long_running' THEN 'Long-running' WHEN 'job_sla_breach' THEN 'Finished late (SLA)'
            ELSE issue END AS issue,
       observed_value, expected_value, message
FROM {t('job_monitor_results')}
WHERE issue IN ('job_long_running', 'job_sla_breach')
ORDER BY run_day, job_name""",
           [("run_day", STRING), ("job_name", STRING), ("issue", STRING), ("observed_value", STRING),
            ("expected_value", STRING), ("message", STRING)]),

        # ---- revenue -------------------------------------------------------------------------
        ds("ds_revenue_compare", "Revenue before and after", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, '{BEFORE}' AS measure,
       CAST(reported_without_hub AS DOUBLE) AS amount FROM {t('vw_revenue_before_after')}
UNION ALL
SELECT date_format(file_date, 'yyyy-MM-dd'), '{AFTER}', CAST(certified_by_hub AS DOUBLE)
FROM {t('vw_revenue_before_after')}
ORDER BY 1, 2""",
           [("file_day", STRING), ("measure", STRING), ("amount", FLOAT)]),
        ds("ds_revenue_parts", "Where the difference went", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, '1 Certified (reportable)' AS revenue_part,
       CAST(certified_by_hub AS DOUBLE) AS amount FROM {t('vw_revenue_before_after')}
UNION ALL
SELECT date_format(file_date, 'yyyy-MM-dd'), '2 Quarantined (bad rows)', CAST(quarantined_measure AS DOUBLE)
FROM {t('vw_revenue_before_after')}
UNION ALL
SELECT date_format(file_date, 'yyyy-MM-dd'), '3 Held for review', CAST(held_measure AS DOUBLE)
FROM {t('vw_revenue_before_after')}
ORDER BY 1, 2""",
           [("file_day", STRING), ("revenue_part", STRING), ("amount", FLOAT)]),
        ds("ds_revenue_region", "Revenue by region, before and after", f"""
WITH cust AS (
  SELECT _file_date, Customer_ID, MAX(Region) AS region
  FROM {t('raw_customer')}
  WHERE Customer_ID IS NOT NULL
  GROUP BY _file_date, Customer_ID
),
rev_before AS (
  SELECT COALESCE(c.region, 'Unknown customer') AS region, try_cast(s.Revenue AS DOUBLE) AS amount
  FROM {t('raw_sales')} s LEFT JOIN cust c ON s._file_date = c._file_date AND s.Customer_ID = c.Customer_ID
),
rev_after AS (
  SELECT COALESCE(c.region, 'Unknown customer') AS region, s.Revenue AS amount
  FROM {t('certified_sales')} s LEFT JOIN cust c ON s._file_date = c._file_date AND s.Customer_ID = c.Customer_ID
  WHERE s.certification_status = 'CERTIFIED'
)
SELECT region, '{BEFORE}' AS measure, CAST(COALESCE(SUM(amount), 0) AS DOUBLE) AS amount FROM rev_before GROUP BY region
UNION ALL
SELECT region, '{AFTER}', CAST(COALESCE(SUM(amount), 0) AS DOUBLE) FROM rev_after GROUP BY region
ORDER BY 1, 2""",
           [("region", STRING), ("measure", STRING), ("amount", FLOAT)]),
        ds("ds_cert", "Certification summary", f"""
SELECT date_format(file_date, 'yyyy-MM-dd') AS file_day, dataset,
       CAST(raw_rows AS BIGINT) AS raw_rows, CAST(certified_rows AS BIGINT) AS certified_rows,
       CAST(quarantined_rows AS BIGINT) AS quarantined_rows, CAST(held_rows AS BIGINT) AS held_rows,
       CAST(released_rows AS BIGINT) AS released_rows,
       CAST(raw_measure AS DOUBLE) AS raw_revenue, CAST(certified_measure AS DOUBLE) AS certified_revenue
FROM {t('certification_summary')}
ORDER BY file_day, dataset""",
           [("file_day", STRING), ("dataset", STRING), ("raw_rows", INTEGER), ("certified_rows", INTEGER),
            ("quarantined_rows", INTEGER), ("held_rows", INTEGER), ("released_rows", INTEGER),
            ("raw_revenue", FLOAT), ("certified_revenue", FLOAT)]),
        ds("ds_quarantine", "Quarantined sales rows", f"""
SELECT date_format(_file_date, 'yyyy-MM-dd') AS file_day, Sales_ID, Sale_Date, Customer_ID, Product_ID, Quantity,
       Revenue, CONCAT_WS(', ', _dq_rules_failed) AS reasons, _review_status AS review_status,
       COALESCE(_reviewed_by, '') AS reviewed_by
FROM {t('quarantine_sales')}
ORDER BY file_day, Sales_ID""",
           [("file_day", STRING), ("Sales_ID", STRING), ("Sale_Date", STRING), ("Customer_ID", STRING),
            ("Product_ID", STRING), ("Quantity", STRING), ("Revenue", STRING), ("reasons", STRING),
            ("review_status", STRING), ("reviewed_by", STRING)]),
    ]


# ---------------------------------------------------------------------------------
# Widget builders
# ---------------------------------------------------------------------------------
def _frame(title: str, description: Optional[str] = None) -> dict:
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update({"showDescription": True, "description": description})
    return frame


def _dim(col: str) -> dict:
    return {"name": col, "expression": f"`{col}`"}


def _sum(col: str) -> dict:
    return {"name": f"sum({col})", "expression": f"SUM(`{col}`)"}


def _query(dataset: str, fields: List[dict], disaggregated: bool) -> List[dict]:
    return [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields, "disaggregated": disaggregated}}]


def _place(widget: dict, x: int, y: int, w: int, h: int) -> dict:
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


def _text(name: str, line: str) -> dict:
    return {"name": name, "multilineTextboxSpec": {"lines": [line]}}


def _kpi(name: str, dataset: str, col: str, title: str, description: str) -> dict:
    """Counter on a one-row KPI dataset: the value is shown as it is."""
    return {
        "name": name,
        "queries": _query(dataset, [_dim(col)], True),
        "spec": {
            "version": 2,
            "widgetType": "counter",
            "encodings": {"value": {"fieldName": col, "displayName": title}},
            "frame": _frame(title, description),
        },
    }


def _chart(name: str, kind: str, dataset: str, x: tuple, y: tuple, color: Optional[tuple], title: str,
           description: Optional[str] = None, grouped: bool = False) -> dict:
    """kind is 'bar' or 'line'; x, y and color are (column, label). y is summed."""
    fields = [_dim(x[0]), _sum(y[0])]
    encodings = {
        "x": {"fieldName": x[0], "scale": {"type": "categorical"}, "displayName": x[1]},
        "y": {"fieldName": f"sum({y[0]})", "scale": {"type": "quantitative"}, "displayName": y[1]},
    }
    if color:
        fields.append(_dim(color[0]))
        encodings["color"] = {"fieldName": color[0], "scale": {"type": "categorical"}, "displayName": color[1]}
    spec = {"version": 3, "widgetType": kind, "encodings": encodings, "frame": _frame(title, description)}
    if grouped:
        spec["mark"] = {"layout": "group"}
    return {"name": name, "queries": _query(dataset, fields, False), "spec": spec}


def _pie(name: str, dataset: str, value: tuple, color: tuple, title: str, description: Optional[str] = None) -> dict:
    fields = [_sum(value[0]), _dim(color[0])]
    return {
        "name": name,
        "queries": _query(dataset, fields, False),
        "spec": {
            "version": 3,
            "widgetType": "pie",
            "encodings": {
                "angle": {"fieldName": f"sum({value[0]})", "scale": {"type": "quantitative"}, "displayName": value[1]},
                "color": {"fieldName": color[0], "scale": {"type": "categorical"}, "displayName": color[1]},
            },
            "frame": _frame(title, description),
        },
    }


def _table(name: str, dataset: dict, columns: List[tuple], title: str, description: Optional[str] = None) -> dict:
    """columns: list of (column, label). Rows appear in the dataset's ORDER BY order."""
    known = dict(dataset["columns"])
    for col, _ in columns:
        if col not in known:
            raise KeyError(f"{name}: column {col} is not in dataset {dataset['name']}")
    return {
        "name": name,
        "queries": _query(dataset["name"], [_dim(c) for c, _ in columns], True),
        "spec": {
            "version": 2,
            "widgetType": "table",
            "encodings": {"columns": [{"fieldName": c, "displayName": label} for c, label in columns]},
            "frame": _frame(title, description),
        },
    }


# ---------------------------------------------------------------------------------
# The dashboard
# ---------------------------------------------------------------------------------
def build_definition(settings: Settings) -> dict:
    ds_list = _datasets(settings)
    ds: Dict[str, dict] = {d["name"]: d for d in ds_list}
    LATEST = "On the latest file date"

    executive = [
        _place(_text("txt_title", "## Enterprise Data Operations Dashboard"), 0, 0, 12, 1),
        _place(_text("txt_intro", "Daily visibility into data quality, trust scores, anomalies, alerts, file arrivals, "
                     "job health and revenue impact, so issues are found and fixed before they reach a report. "
                     "Trust **90+** safe to report, **70-89** use with caution, **below 70** do not report."),
               0, 1, 12, 2),
        _place(_kpi("k_trust", "kpi_exec", "trust_latest", "Overall data trust score", LATEST + "; 0 to 100"), 0, 3, 3, 3),
        _place(_kpi("k_open", "kpi_exec", "open_alerts_latest", "Open alerts today", LATEST), 3, 3, 3, 3),
        _place(_kpi("k_crit", "kpi_exec", "critical_alerts", "Critical alerts", "All days"), 6, 3, 3, 3),
        _place(_kpi("k_pass", "kpi_exec", "dq_pass_rate", "DQ pass rate (%)", "Quality rules passed, all days"),
               9, 3, 3, 3),
        _place(_kpi("k_jobs", "kpi_exec", "jobs_failed_latest", "Jobs failed today", LATEST), 0, 6, 4, 3),
        _place(_kpi("k_missing", "kpi_exec", "files_missing", "Files missing", "All days"), 4, 6, 4, 3),
        _place(_kpi("k_rev", "kpi_exec", "revenue_kept_out", "Revenue impact",
                    "Revenue kept out of reports: quarantined plus held"), 8, 6, 4, 3),
        _place(_chart("c_trust_trend", "line", "ds_trust_overall", ("file_day", "Day"),
                      ("overall_trust_score", "Trust score"), None, "Trust score trend",
                      "Lowest dataset score each day"), 0, 9, 6, 6),
        _place(_pie("c_open_sev", "ds_alerts", ("alert_one", "Alerts"), ("severity", "Severity"),
                    "Open alerts by severity"), 6, 9, 6, 6),
        _place(_table("t_health", ds["ds_trust_overall"], [
            ("file_day", "Day"), ("overall_trust_score", "Trust score"), ("overall_status", "Status"),
            ("source_trust", "Source trust"), ("data_trust", "Data trust"), ("job_trust", "Job trust"),
            ("alerts", "Alerts"), ("critical_alerts", "Critical"), ("weakest_dataset", "Weakest dataset"),
        ], "Daily health summary", "Lowest score across datasets, per layer"), 0, 15, 12, 6),
    ]

    quality = [
        _place(_kpi("k_exec", "kpi_dq", "rules_executed", "Total rules executed", "Rule checks with a result"),
               0, 0, 3, 3),
        _place(_kpi("k_passed", "kpi_dq", "rules_passed", "Passed rules", "All days"), 3, 0, 3, 3),
        _place(_kpi("k_failed", "kpi_dq", "rules_failed", "Failed rules", "All days"), 6, 0, 2, 3),
        _place(_kpi("k_passpct", "kpi_dq", "pass_pct", "Pass %", "Passed / executed"), 8, 0, 2, 3),
        _place(_kpi("k_critfail", "kpi_dq", "critical_failures", "Critical failures", "Critical or High"), 10, 0, 2, 3),
        _place(_chart("c_by_dataset", "bar", "ds_dq_by_dataset", ("dataset", "Dataset"), ("pass_pct", "Pass %"),
                      None, "Rule pass rate by dataset", "Domains come from monitored_datasets.csv"), 0, 3, 4, 6),
        _place(_chart("c_by_dimension", "bar", "ds_dimensions", ("dimension", "Dimension"), ("pass_pct", "Pass %"),
                      None, "Pass rate by data quality dimension",
                      "Completeness, uniqueness, validity, integrity, consistency, timeliness, anomalies"), 4, 3, 4, 6),
        _place(_chart("c_fail_trend", "bar", "ds_rules", ("file_day", "Day"), ("failed_rule", "Failed rules"),
                      ("category", "Layer"), "Failed rules trend", "By layer: row, cross_table and trend"),
               8, 3, 4, 6),
        _place(_table("t_columns", ds["ds_columns"], [
            ("dataset", "Dataset"), ("column_name", "Column"), ("dimensions", "Dimensions"), ("checks", "Checks"),
            ("failed_checks", "Failed checks"), ("failed_records", "Failed records"), ("pass_pct", "Pass %"),
        ], "Quality by column", "Which columns fail most"), 0, 9, 6, 6),
        _place(_table("t_top_failed", ds["ds_top_failed"], [
            ("rule_name", "Rule name"), ("dataset", "Dataset"), ("category", "Layer"), ("severity", "Severity"),
            ("failure_count", "Failure count"), ("days_failed", "Days failed"),
        ], "Top failed rules"), 6, 9, 6, 6),
        _place(_table("t_failed_records", ds["ds_failed_records"], [
            ("file_day", "Day"), ("dataset", "Dataset"), ("rule_name", "Rule"), ("severity", "Severity"),
            ("action", "Action"), ("record_key", "Record"), ("failed_value", "Failed value"), ("file_name", "File"),
        ], "Failed records drill-down", "Exactly which record failed which rule"), 0, 15, 12, 8),
    ]

    alerts = [
        _place(_kpi("k_active", "kpi_alerts", "active_alerts", "Active alerts", "Status OPEN"), 0, 0, 3, 3),
        _place(_kpi("k_crit2", "kpi_alerts", "critical_alerts", "Critical alerts", "All days"), 3, 0, 3, 3),
        _place(_kpi("k_new_anom", "kpi_alerts", "new_anomalies_latest", "New anomalies today",
                    LATEST + "; trend checks"), 6, 0, 3, 3),
        _place(_kpi("k_seeded", "kpi_alerts", "seeded_caught", "Seeded issues caught",
                    "From the detection scorecard"), 9, 0, 3, 3),
        _place(_pie("c_sev_split", "ds_alerts", ("alert_one", "Alerts"), ("severity", "Severity"),
                    "Alert severity split"), 0, 3, 4, 6),
        _place(_chart("c_anomaly_trend", "bar", "ds_anomalies", ("file_day", "Day"), ("anomaly_one", "Anomalies"),
                      ("anomaly_type", "Type"), "Anomaly trend", "Volume, revenue and quality anomalies"), 4, 3, 4, 6),
        _place(_chart("c_categories", "bar", "ds_alerts", ("alert_category", "Category"), ("alert_one", "Alerts"),
                      None, "Top alert categories"), 8, 3, 4, 6),
        _place(_table("t_alerts", ds["ds_alerts"], [
            ("alert_id", "Alert"), ("file_day", "Day"), ("dataset", "Dataset"), ("alert_category", "Category"),
            ("severity", "Severity"), ("rule", "Rule"), ("message", "Message"), ("status", "Status"),
        ], "Alert log"), 0, 9, 12, 8),
        _place(_table("t_trend", ds["ds_trend"], [
            ("file_day", "Day"), ("dataset", "Dataset"), ("rule_name", "Check"), ("status", "Result"),
            ("observed_value", "Observed"), ("healthy_baseline", "Healthy baseline"), ("message", "Detail"),
        ], "Trend checks", "Volume and revenue against the median of recent healthy days"), 0, 17, 12, 6),
        _place(_table("t_scorecard", ds["ds_scorecard"], [
            ("issue_id", "Issue"), ("file_day", "Day"), ("dataset", "Dataset"), ("description", "Seeded issue"),
            ("result", "Result"), ("source", "Source"), ("alert_id", "Alert"), ("severity", "Severity"),
        ], "Detection scorecard"), 0, 23, 12, 8),
    ]

    files = [
        _place(_text("txt_sources", "Sources are configured in `monitored_datasets.csv`: files from a path (Unity Catalog "
                     "volume, Amazon S3, ADLS Gen2) or any Unity Catalog table, including Delta Sharing, Lakehouse "
                     "Federation (external databases) and data shared from other workspaces."), 0, 0, 12, 2),
        _place(_kpi("k_fexp", "kpi_files", "files_expected", "Deliveries expected", "Sources x days"), 0, 2, 2, 3),
        _place(_kpi("k_frec", "kpi_files", "files_received", "Received", "All days"), 2, 2, 2, 3),
        _place(_kpi("k_fdel", "kpi_files", "files_delayed", "Delayed", "Beyond the grace period"), 4, 2, 2, 3),
        _place(_kpi("k_fmiss", "kpi_files", "files_missing", "Missing", "All days"), 6, 2, 2, 3),
        _place(_kpi("k_fsla", "kpi_files", "sla_pct", "On-time %", "On time / expected"), 8, 2, 2, 3),
        _place(_kpi("k_fsrc", "kpi_files", "source_trust_latest", "Source trust today", LATEST), 10, 2, 2, 3),
        _place(_table("t_sources", ds["ds_sources"], [
            ("dataset", "Source"), ("source_type", "Type"), ("source_location", "Location"),
            ("expected_time", "Expected"), ("grace_minutes", "Grace (min)"), ("deliveries_received", "Received"),
            ("deliveries_late", "Late"), ("deliveries_missing", "Missing"), ("on_time_pct", "On-time %"),
            ("latest_status", "Latest"), ("source_trust_latest", "Source trust today"),
            ("source_trust_avg", "Average source trust"),
        ], "Source inventory"), 0, 5, 12, 4),
        _place(_chart("c_receipt", "bar", "ds_files", ("dataset", "Source"), ("file_one", "Deliveries"),
                      ("arrival_status", "Status"), "Delivery status by source"), 0, 9, 6, 6),
        _place(_chart("c_delay", "bar", "ds_files", ("file_day", "Day"), ("delay_minutes", "Minutes late"),
                      ("dataset", "Source"), "Minutes after expected arrival"), 6, 9, 6, 6),
        _place(_table("t_delayed", ds["ds_files_delayed"], [
            ("file_day", "Day"), ("dataset", "Source"), ("file_name", "Delivery"), ("expected_time", "Expected"),
            ("actual_time", "Arrived"), ("delay_minutes", "Delay (min)"), ("arrival_status", "Status"),
        ], "Deliveries delayed beyond buffer", "Late, missing or empty deliveries"), 0, 15, 12, 4),
        _place(_table("t_files", ds["ds_files"], [
            ("file_day", "Day"), ("dataset", "Source"), ("arrival_status", "Status"),
            ("expected_time", "Expected"), ("actual_time", "Arrived"), ("delay_minutes", "Delay (min)"),
            ("row_count", "Rows"), ("identical_to_previous", "Same as previous"), ("file_name", "Delivery"),
        ], "All deliveries"), 0, 19, 12, 8),
    ]

    jobs = [
        _place(_kpi("k_jtot", "kpi_jobs", "total_runs", "Total job runs", "All days"), 0, 0, 3, 3),
        _place(_kpi("k_jok", "kpi_jobs", "successful_runs", "Successful runs", "All days"), 3, 0, 2, 3),
        _place(_kpi("k_jfail", "kpi_jobs", "failed_runs", "Failed runs", "All days"), 5, 0, 2, 3),
        _place(_kpi("k_javg", "kpi_jobs", "avg_runtime_min", "Average runtime (min)", "All runs"), 7, 0, 2, 3),
        _place(_kpi("k_jtrust", "kpi_jobs", "job_trust_latest", "Job trust today",
                    LATEST + "; failures, SLA breaches and slow runs"), 9, 0, 3, 3),
        _place(_pie("c_job_status", "ds_jobs", ("run_one", "Runs"), ("status", "Status"), "Job health distribution",
                    "Finished runs; running jobs are not tracked"), 0, 3, 4, 6),
        _place(_chart("c_job_duration", "line", "ds_jobs", ("run_day", "Day"), ("duration_min", "Minutes"),
                      ("job_name", "Job"), "Job runtime by day"), 4, 3, 8, 6),
        _place(_table("t_jobs_failed", ds["ds_jobs_failed"], [
            ("run_day", "Day"), ("job_name", "Job name"), ("failure_time", "Failure time"),
            ("error_message", "Error"), ("source", "Source"),
        ], "Failed jobs"), 0, 9, 6, 5),
        _place(_table("t_jobs_slow", ds["ds_jobs_slow"], [
            ("run_day", "Day"), ("job_name", "Job name"), ("issue", "Issue"),
            ("observed_value", "Observed"), ("expected_value", "Expected"),
        ], "Long-running and late jobs", "Runs over twice their typical time, or past their SLA"), 6, 9, 6, 5),
        _place(_table("t_jobs", ds["ds_jobs"], [
            ("run_day", "Day"), ("job_name", "Job"), ("status", "Status"), ("start_time", "Start"),
            ("end_time", "End"), ("duration_min", "Minutes"), ("issues", "Issues"), ("source", "Source"),
        ], "All job runs", "source = simulated or system_tables (real runs)"), 0, 14, 12, 8),
    ]

    revenue = [
        _place(_kpi("k_rbefore", "kpi_revenue", "revenue_before", "Revenue before", "As received, all days"),
               0, 0, 3, 3),
        _place(_kpi("k_rafter", "kpi_revenue", "revenue_after", "Revenue after", "Certified, reportable"), 3, 0, 3, 3),
        _place(_kpi("k_rdiff", "kpi_revenue", "revenue_difference", "Difference", "Kept out of reports"), 6, 0, 3, 3),
        _place(_kpi("k_rimpact", "kpi_revenue", "impact_pct", "Impact %", "Difference / revenue before"), 9, 0, 3, 3),
        _place(_chart("c_rev_compare", "bar", "ds_revenue_compare", ("file_day", "Day"), ("amount", "Revenue"),
                      ("measure", "Measure"), "Before vs after revenue", grouped=True), 0, 3, 6, 6),
        _place(_chart("c_rev_trend", "line", "ds_revenue_compare", ("file_day", "Day"), ("amount", "Revenue"),
                      ("measure", "Measure"), "Revenue trend"), 6, 3, 6, 6),
        _place(_chart("c_rev_region", "bar", "ds_revenue_region", ("region", "Region"), ("amount", "Revenue"),
                      ("measure", "Measure"), "Revenue impact by region",
                      "Region from the customer file of the same day", grouped=True), 0, 9, 6, 6),
        _place(_chart("c_rev_parts", "bar", "ds_revenue_parts", ("file_day", "Day"), ("amount", "Revenue"),
                      ("revenue_part", "Revenue"), "Where the difference went",
                      "Full bar = revenue as received"), 6, 9, 6, 6),
        _place(_table("t_cert", ds["ds_cert"], [
            ("file_day", "Day"), ("dataset", "Dataset"), ("raw_rows", "Rows received"),
            ("certified_rows", "Certified"), ("quarantined_rows", "Quarantined"), ("held_rows", "Held"),
            ("released_rows", "Released by review"),
            ("raw_revenue", "Revenue received"), ("certified_revenue", "Revenue certified"),
        ], "Certification summary", "Revenue columns apply to Sales"), 0, 15, 12, 6),
        _place(_table("t_quarantine", ds["ds_quarantine"], [
            ("file_day", "Day"), ("Sales_ID", "Sales ID"), ("Sale_Date", "Sale date"),
            ("Customer_ID", "Customer"), ("Product_ID", "Product"), ("Quantity", "Quantity"),
            ("Revenue", "Revenue"), ("reasons", "Why quarantined"), ("review_status", "Review"),
            ("reviewed_by", "Reviewed by"),
        ], "Quarantined sales rows", "Original values as received; review status from the review app"), 0, 21, 12, 6),
    ]

    trust = [
        _place(_kpi("k_tnow", "kpi_trust", "trust_latest", "Overall trust today", LATEST + "; source and data"),
               0, 0, 3, 3),
        _place(_kpi("k_tsrc", "kpi_trust", "source_trust_latest", "Source trust", "Delivery: late, missing, re-sent"),
               3, 0, 2, 3),
        _place(_kpi("k_tdata", "kpi_trust", "data_trust_latest", "Data trust", "Processed data: bad rows, anomalies"),
               5, 0, 2, 3),
        _place(_kpi("k_tjob", "kpi_trust", "job_trust_latest", "Job trust", "Failures, SLA breaches, slow runs"),
               7, 0, 2, 3),
        _place(_kpi("k_tchange", "kpi_trust", "change_vs_previous", "Change vs previous day", "Overall, points"),
               9, 0, 3, 3),
        _place(_chart("c_t_layers", "line", "ds_trust_layers", ("file_day", "Day"), ("score", "Trust score"),
                      ("layer", "Layer"), "Trust by layer",
                      "Lowest dataset score each day, per layer"), 0, 3, 6, 6),
        _place(_chart("c_t_dataset", "line", "ds_trust", ("file_day", "Day"), ("trust_score", "Trust score"),
                      ("dataset", "Dataset"), "Overall trust by dataset"), 6, 3, 6, 6),
        _place(_chart("c_t_drivers", "bar", "ds_trust_drivers", ("file_day", "Day"), ("points_lost", "Points lost"),
                      ("driver", "Driver"), "Trust score drivers",
                      "Points taken off the overall score, summed over datasets. Job health is scored separately "
                      "as job trust."), 0, 9, 12, 6),
        _place(_table("t_trust", ds["ds_trust"], [
            ("file_day", "Day"), ("dataset", "Dataset"), ("trust_score", "Overall"), ("trust_status", "Status"),
            ("source_trust", "Source"), ("data_trust", "Data"), ("job_trust", "Jobs"),
            ("quarantined_rows", "Quarantined"), ("alert_count", "Alerts"), ("top_issue", "Top issue"),
        ], "Trust by layer, dataset and day"), 0, 15, 12, 7),
    ]

    pages = [
        ("page_executive", "Executive overview", executive),
        ("page_quality", "Data quality", quality),
        ("page_alerts", "Alerts and anomalies", alerts),
        ("page_sources", "Source monitoring", files),
        ("page_jobs", "Job monitoring", jobs),
        ("page_revenue", "Revenue impact", revenue),
        ("page_trust", "Trust score", trust),
    ]
    return {
        "datasets": [
            {"name": d["name"], "displayName": d["displayName"], "queryLines": _query_lines(d["sql"])}
            for d in ds_list
        ],
        "pages": [
            {"name": name, "displayName": title, "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1",
             "layout": layout}
            for name, title, layout in pages
        ],
    }


def _query_lines(sql: str) -> List[str]:
    lines = sql.strip().split("\n")
    return [line + "\n" for line in lines[:-1]] + [lines[-1]]


def write_definition(settings: Settings, path: str) -> str:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(build_definition(settings), fh, indent=2)
        fh.write("\n")
    return path


# ---------------------------------------------------------------------------------
# Deploy to a workspace
# ---------------------------------------------------------------------------------
def _pick_warehouse(client) -> str:
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError(
            "No SQL warehouse is visible to you. Ask for access to one, or pass warehouse_id explicitly."
        )

    def rank(w):
        running = str(getattr(w, "state", "")).upper().endswith("RUNNING")
        serverless = bool(getattr(w, "enable_serverless_compute", False))
        return (not running, not serverless, getattr(w, "name", "") or "")

    return sorted(warehouses, key=rank)[0].id


def deploy(settings: Settings, warehouse_id: Optional[str] = None, parent_path: Optional[str] = None,
           display_name: str = DEFAULT_NAME, publish: bool = True, client=None) -> dict:
    """Create the dashboard, or update it if one with the same name is already in ``parent_path``.

    Runs inside a Databricks notebook with no setup. Returns the dashboard id, path and URL.
    """
    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
    api = client.api_client
    warehouse_id = warehouse_id or _pick_warehouse(client)
    if not parent_path:
        parent_path = f"/Users/{client.current_user.me().user_name}"
    parent_path = parent_path.rstrip("/")
    if parent_path.startswith("/Workspace/"):
        parent_path = parent_path[len("/Workspace"):]
    path = f"{parent_path}/{display_name}.lvdash.json"
    serialized = json.dumps(build_definition(settings))

    existing_id = None
    try:
        status = api.do("GET", "/api/2.0/workspace/get-status", query={"path": path})
        if status.get("object_type") == "DASHBOARD":
            existing_id = status.get("resource_id")
    except Exception:  # noqa: BLE001 - not found means create a new one
        existing_id = None

    body = {"display_name": display_name, "serialized_dashboard": serialized, "warehouse_id": warehouse_id}
    if existing_id:
        result = api.do("PATCH", f"/api/2.0/lakeview/dashboards/{existing_id}", body=body)
        action = "updated"
    else:
        result = api.do("POST", "/api/2.0/lakeview/dashboards", body={**body, "parent_path": parent_path})
        action = "created"
    dashboard_id = result.get("dashboard_id") or existing_id

    if publish:
        api.do(
            "POST",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
            body={"embed_credentials": True, "warehouse_id": warehouse_id},
        )

    host = (client.config.host or "").rstrip("/")
    return {
        "action": action,
        "dashboard_id": dashboard_id,
        "path": path,
        "warehouse_id": warehouse_id,
        "published": publish,
        "url": f"{host}/dashboardsv3/{dashboard_id}" + ("/published" if publish else ""),
    }
