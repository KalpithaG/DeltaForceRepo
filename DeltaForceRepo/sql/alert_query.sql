-- =====================================================================================
-- Databricks SQL alerts: six alerts, one query each.
-- For each: create a query from the SQL below, then create an alert on it with the
-- condition shown, and add a notification destination (email, Microsoft Teams webhook,
-- or PagerDuty). Written for team_9.default; find and replace if yours differ.
-- "Latest day" means the latest file date the hub has processed.
-- =====================================================================================

-- [A1] Critical alerts open                       Condition: critical_open > 0
SELECT COUNT(*) AS critical_open,
       CONCAT_WS(' | ', SLICE(COLLECT_LIST(message), 1, 5)) AS messages
FROM team_9.default.vw_alerts
WHERE severity = 'Critical' AND status = 'OPEN';

-- [A2] Trust score below 90 on the latest day     Condition: overall_trust_score < 90
SELECT overall_trust_score, overall_status, weakest_dataset, file_date
FROM team_9.default.vw_trust_overall_daily
ORDER BY file_date DESC
LIMIT 1;

-- [A3] Failed jobs on the latest day              Condition: failed_jobs > 0
SELECT COUNT(*) AS failed_jobs,
       CONCAT_WS(', ', COLLECT_LIST(job_name)) AS jobs
FROM team_9.default.vw_job_health
WHERE status = 'FAILED'
  AND run_date = (SELECT MAX(run_date) FROM team_9.default.vw_job_health);

-- [A4] Missing files on the latest day            Condition: missing_files > 0
SELECT COUNT(*) AS missing_files,
       CONCAT_WS(', ', COLLECT_LIST(dataset)) AS datasets
FROM team_9.default.vw_file_arrivals
WHERE arrival_status = 'MISSING'
  AND file_date = (SELECT MAX(file_date) FROM team_9.default.vw_file_arrivals);

-- [A5] Revenue variance over 10% on the latest day  Condition: revenue_variance_pct > 10
--      Share of received revenue kept out of reports (quarantined or held).
SELECT ROUND(100 * (reported_without_hub - certified_by_hub) / NULLIF(reported_without_hub, 0), 1)
         AS revenue_variance_pct,
       reported_without_hub, certified_by_hub, file_date
FROM team_9.default.vw_revenue_before_after
WHERE file_date = (SELECT MAX(file_date) FROM team_9.default.vw_revenue_before_after);

-- [A6] High-severity data quality failures on the latest day   Condition: high_dq_failures > 0
SELECT COUNT(*) AS high_dq_failures,
       CONCAT_WS(', ', COLLECT_LIST(rule_name)) AS rules
FROM team_9.default.vw_dq_rule_results
WHERE status = 'FAIL'
  AND severity IN ('Critical', 'High')
  AND category IN ('row', 'cross_table')
  AND file_date = (SELECT MAX(file_date) FROM team_9.default.vw_dq_rule_results);
