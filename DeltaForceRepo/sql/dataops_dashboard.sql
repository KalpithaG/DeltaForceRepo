-- =====================================================================================
-- DataOps dashboard: one query per tile.
-- Written for catalog team_9, schema default. Find and replace team_9.default if yours differ.
-- The page and tile layout is described in sql/README.md.
-- =====================================================================================

-- ---------- PAGE 1: OVERVIEW ---------------------------------------------------------

-- [T1] trust_today  (counter: overall_trust_score, with overall_status as the label)
SELECT file_date, overall_trust_score, overall_status, alerts, critical_alerts, weakest_dataset
FROM team_9.default.vw_trust_overall_daily
ORDER BY file_date DESC
LIMIT 1;

-- [T2] trust_trend  (line: x = file_date, y = trust_score, series = dataset)
SELECT file_date, dataset, trust_score, trust_status
FROM team_9.default.vw_trust_score_daily
ORDER BY file_date, dataset;

-- [T3] safe_to_report_grid  (pivot/heatmap: rows = dataset, columns = file_date, value = trust_status)
SELECT dataset, file_date, trust_status, trust_score, top_issue
FROM team_9.default.vw_trust_score_daily
ORDER BY dataset, file_date;

-- [T4] alerts_by_day_and_severity  (stacked bar: x = file_date, y = alerts, colour = severity)
SELECT file_date, severity, COUNT(*) AS alerts
FROM team_9.default.vw_alerts
GROUP BY file_date, severity
ORDER BY file_date;

-- [T5] revenue_before_after  (grouped bar: x = file_date, y = both measures)
SELECT file_date, reported_without_hub, certified_by_hub, quarantined_measure, held_measure
FROM team_9.default.vw_revenue_before_after
ORDER BY file_date;

-- ---------- PAGE 2: FILE ARRIVALS ----------------------------------------------------

-- [T6] file_arrival_grid  (pivot: rows = dataset, columns = file_date, value = arrival_status)
SELECT dataset, file_date, arrival_status, delay_minutes, row_count, file_name
FROM team_9.default.vw_file_arrivals
ORDER BY dataset, file_date;

-- [T7] file_delay_minutes  (bar: x = file_date, y = delay_minutes, colour = dataset)
SELECT file_date, dataset, COALESCE(delay_minutes, 0) AS delay_minutes
FROM team_9.default.vw_file_arrivals
ORDER BY file_date;

-- [T8] rows_per_file  (line: x = file_date, y = row_count, series = dataset)
SELECT file_date, dataset, row_count
FROM team_9.default.vw_file_arrivals
WHERE row_count IS NOT NULL
ORDER BY file_date;

-- ---------- PAGE 3: DATA QUALITY -----------------------------------------------------

-- [T9] failures_by_rule  (horizontal bar: y = rule_name, x = failed_rows, colour = category)
SELECT rule_name, category, severity, SUM(failed_count) AS failed_rows
FROM team_9.default.vw_dq_rule_results
WHERE status = 'FAIL'
GROUP BY rule_name, category, severity
ORDER BY failed_rows DESC;

-- [T10] failures_by_layer  (bar: x = file_date, y = failed_rules, colour = category)
SELECT file_date, category, COUNT(*) AS failed_rules
FROM team_9.default.vw_dq_rule_results
WHERE status = 'FAIL'
GROUP BY file_date, category
ORDER BY file_date;

-- [T11] trend_checks  (table)
SELECT file_date, dataset, rule_name, status, observed_value, expected_value AS healthy_baseline, message
FROM team_9.default.vw_dq_rule_results
WHERE category = 'trend' AND rule_name NOT LIKE '%disappeared'
ORDER BY file_date, rule_name;

-- [T12] failed_records  (table)
SELECT file_date, dataset, rule_name, severity, action, record_key, failed_value, file_name
FROM team_9.default.dq_failed_rows
ORDER BY file_date, dataset, rule_name;

-- ---------- PAGE 4: JOBS -------------------------------------------------------------

-- [T13] job_status_grid  (pivot: rows = job_name, columns = run_date, value = status)
SELECT job_name, run_date, status, duration_min, issues
FROM team_9.default.vw_job_health
ORDER BY job_name, run_date;

-- [T14] job_duration_trend  (line: x = run_date, y = duration_min, series = job_name)
SELECT run_date, job_name, duration_min
FROM team_9.default.vw_job_health
ORDER BY run_date;

-- [T15] job_issues  (table)
SELECT run_date, job_name, issue, severity, observed_value, expected_value, message
FROM team_9.default.job_monitor_results
ORDER BY run_date, job_name;

-- ---------- PAGE 5: ALERTS AND SELF-HEALING ------------------------------------------

-- [T16] alert_log  (table, sorted by severity)
SELECT alert_id, file_date, dataset, category, severity, rule, message, observed_value, expected_value, status
FROM team_9.default.vw_alerts
ORDER BY file_date, severity_rank DESC, alert_id;

-- [T17] quarantined_sales  (table)
SELECT _file_date AS file_date, Sales_ID, Sale_Date, Customer_ID, Product_ID, Quantity, Revenue,
       CONCAT_WS(', ', _dq_rules_failed) AS reasons
FROM team_9.default.quarantine_sales
ORDER BY _file_date, Sales_ID;

-- [T18] certification_summary  (table)
SELECT file_date, dataset, raw_rows, certified_rows, quarantined_rows, held_rows
FROM team_9.default.certification_summary
ORDER BY file_date, dataset;

-- ---------- PAGE 6: DETECTION SCORECARD ----------------------------------------------

-- [T19] scorecard_total  (counter: "caught / issues")
SELECT COUNT(*) AS issues,
       SUM(CASE WHEN caught THEN 1 ELSE 0 END) AS caught,
       ROUND(100.0 * SUM(CASE WHEN caught THEN 1 ELSE 0 END) / COUNT(*), 0) AS caught_pct
FROM team_9.default.vw_detection_scorecard;

-- [T20] scorecard_detail  (table)
SELECT issue_id, file_date, dataset, description, rule_name, record_key,
       CASE WHEN official_scenario = 'Y' THEN 'Official scenario' ELSE 'Extra finding' END AS source,
       CASE WHEN caught THEN 'Caught' ELSE 'Missed' END AS result, alert_id, severity
FROM team_9.default.vw_detection_scorecard
ORDER BY issue_id;
