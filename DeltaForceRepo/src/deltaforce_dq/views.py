"""Stage 7 - dashboard views. Every dashboard tile and the Genie space read from these."""

from __future__ import annotations

from .config import Settings, write_table


DIMENSIONS = {
    "not_null": "Completeness",
    "unique": "Uniqueness",
    "duplicate_row": "Uniqueness",
    "regex": "Validity",
    "valid_format": "Validity",
    "not_future": "Validity",
    "expression": "Validity",
    "outlier_vs_median": "Validity",
    "ref_exists": "Integrity",
    "ref_expression": "Integrity",
    "key_disappeared": "Consistency",
    "metric_change": "Volume and anomalies",
}


def run(spark, settings: Settings) -> None:
    t = settings.table

    # Rule catalogue: the column each rule checks and its data quality dimension
    # (an optional "dimension" column in dq_rules.csv overrides the default).
    catalogue = [
        (r["rule_id"], r["rule_name"], r["dataset"], r.get("column"), r["rule_type"],
         r.get("dimension") or DIMENSIONS.get(r["rule_type"], "Other"), r.get("description"))
        for r in settings.rules()
    ]
    write_table(
        spark.createDataFrame(catalogue, "rule_id string, rule_name string, dataset string, column_name string, "
                                         "rule_type string, dimension string, description string"),
        settings, "dq_rule_catalog")

    # Source inventory: what each source is, where it lives and when it is due.
    expected = {c["dataset"]: c for c in settings.load("file_monitor")}
    sources = []
    for d in settings.datasets():
        from_files = not d.get("source_table") or (d.get("load_from_files") or "N").upper() == "Y"
        location = d.get("source_location") or (
            f"{settings.landing_path} ({d['dataset']} folder or {d['file_pattern']})" if from_files
            else d["source_table"])
        kind = d.get("source_type") or ("Files" if from_files else "Unity Catalog table")
        cfg = expected.get(d["dataset"], {})
        sources.append((d["dataset"], kind, location, cfg.get("expected_time"),
                        int(cfg.get("grace_minutes") or 0)))
    write_table(
        spark.createDataFrame(sources, "dataset string, source_type string, source_location string, "
                                       "expected_time string, grace_minutes int"),
        settings, "source_catalog")

    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_trust_score_daily')} AS
        SELECT file_date, dataset, trust_score, trust_status, row_quality_pct, row_penalty_points, alert_penalty_points,
               source_trust, data_trust, job_trust,
               total_rows, bad_rows, quarantined_rows, alert_count, critical_alerts, arrival_status, top_issue
        FROM {t('trust_score')}
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_trust_overall_daily')} AS
        SELECT file_date,
               MIN(trust_score) AS overall_trust_score,
               CASE WHEN MIN(trust_score) >= 90 THEN 'SAFE TO REPORT'
                    WHEN MIN(trust_score) >= 70 THEN 'USE WITH CAUTION'
                    ELSE 'DO NOT REPORT' END AS overall_status,
               SUM(alert_count) AS alerts,
               SUM(critical_alerts) AS critical_alerts,
               MIN_BY(dataset, trust_score) AS weakest_dataset,
               MIN(source_trust) AS source_trust,
               MIN(data_trust) AS data_trust,
               MIN(job_trust) AS job_trust
        FROM {t('trust_score')}
        GROUP BY file_date
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_alerts')} AS
        SELECT alert_id, file_date, dataset, category, severity, rule, message,
               failed_count, observed_value, expected_value, status, `timestamp`,
               CASE severity WHEN 'Critical' THEN 4 WHEN 'High' THEN 3 WHEN 'Medium' THEN 2 ELSE 1 END AS severity_rank
        FROM {t('alert_log')}
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_file_arrivals')} AS
        SELECT file_date, dataset, file_name, expected_ts, actual_ts, delay_minutes, row_count,
               arrival_status, identical_to_previous
        FROM {t('file_monitor_results')}
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_dq_rule_results')} AS
        SELECT r.file_date, r.dataset, r.category, r.rule_id, r.rule_name, r.severity, r.action, r.status,
               r.total_rows, r.failed_count, r.failed_pct, r.observed_value, r.expected_value, r.message,
               r.rule_type, c.column_name, COALESCE(c.dimension, 'Other') AS dimension
        FROM {t('dq_results')} r
        LEFT JOIN {t('dq_rule_catalog')} c ON r.rule_id = c.rule_id
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_job_health')} AS
        SELECT r.run_date, r.job_name, r.dataset, r.scheduled_ts, r.start_ts, r.end_ts, r.duration_min, r.status,
               r.error_message, r.source,
               COALESCE(i.issues, 'OK') AS issues
        FROM {t('job_runs')} r
        LEFT JOIN (
            SELECT run_id, CONCAT_WS(', ', COLLECT_SET(issue)) AS issues
            FROM {t('job_monitor_results')} WHERE run_id IS NOT NULL GROUP BY run_id
        ) i ON r.run_id = i.run_id
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_revenue_before_after')} AS
        SELECT file_date, dataset, measure,
               raw_measure       AS reported_without_hub,
               certified_measure AS certified_by_hub,
               quarantined_measure, held_measure,
               raw_rows, certified_rows, quarantined_rows, held_rows
        FROM {t('certification_summary')}
        WHERE measure IS NOT NULL
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_detection_scorecard')} AS
        SELECT issue_id, file_date, dataset, description, rule_name, record_key,
               official_scenario, caught, alert_id, severity
        FROM {t('detection_scorecard')}
    """)

    # ---- review queue: every quarantined record, as JSON, with its latest decision -------
    from .review import DECISIONS, ensure_decisions_table

    ensure_decisions_table(spark, settings)
    parts = []
    for ds in settings.datasets():
        fields = ", ".join(f"'{c}', `{c}`" for c, _ in ds["columns"])
        parts.append(f"""
        SELECT '{ds['dataset']}' AS dataset, _row_id AS row_id, `{ds['key_column']}` AS record_key,
               _file_date AS file_date, _file_name AS file_name,
               to_json(named_struct({fields}), map('ignoreNullFields', 'false')) AS record_json,
               concat_ws(', ', _dq_rules_failed) AS reasons, _review_status AS review_status,
               _reviewed_by AS reviewed_by, _review_comment AS review_comment,
               concat_ws(', ', _recheck_failed) AS recheck_failed, _quarantined_at AS last_hub_run
        FROM {t('quarantine_' + ds['table_suffix'])}""")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_quarantine_queue')} AS
        WITH latest AS (
            SELECT dataset, row_id, decision, reviewer, decided_at,
                   ROW_NUMBER() OVER (PARTITION BY dataset, row_id ORDER BY decided_at DESC, decision_id DESC) AS rn
            FROM {t(DECISIONS)}
        ),
        q AS ({" UNION ALL ".join(parts)})
        SELECT q.*, l.decision AS latest_decision, l.reviewer AS decision_by, l.decided_at AS decision_at,
               COALESCE(l.decided_at > q.last_hub_run, false) AS awaiting_hub_run
        FROM q LEFT JOIN latest l ON q.dataset = l.dataset AND q.row_id = l.row_id AND l.rn = 1
    """)

    spark.sql(f"""
        CREATE OR REPLACE VIEW {t('vw_source_inventory')} AS
        WITH arrivals AS (
            SELECT dataset,
                   COUNT(*) AS deliveries_expected,
                   SUM(CASE WHEN arrival_status <> 'MISSING' THEN 1 ELSE 0 END) AS deliveries_received,
                   SUM(CASE WHEN arrival_status = 'LATE' THEN 1 ELSE 0 END) AS deliveries_late,
                   SUM(CASE WHEN arrival_status = 'MISSING' THEN 1 ELSE 0 END) AS deliveries_missing,
                   ROUND(100 * SUM(CASE WHEN arrival_status = 'ON_TIME' THEN 1 ELSE 0 END) / COUNT(*), 1)
                     AS on_time_pct,
                   MAX_BY(arrival_status, file_date) AS latest_status,
                   MAX(file_date) AS latest_file_date
            FROM {t('file_monitor_results')}
            GROUP BY dataset
        ),
        latest_trust AS (
            SELECT dataset, MAX_BY(source_trust, file_date) AS source_trust_latest,
                   ROUND(AVG(source_trust), 1) AS source_trust_avg
            FROM {t('trust_score')}
            GROUP BY dataset
        )
        SELECT s.dataset, s.source_type, s.source_location, s.expected_time, s.grace_minutes,
               a.deliveries_expected, a.deliveries_received, a.deliveries_late, a.deliveries_missing,
               a.on_time_pct, a.latest_status, a.latest_file_date,
               l.source_trust_latest, l.source_trust_avg
        FROM {t('source_catalog')} s
        LEFT JOIN arrivals a ON s.dataset = a.dataset
        LEFT JOIN latest_trust l ON s.dataset = l.dataset
    """)
