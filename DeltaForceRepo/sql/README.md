# The DataOps dashboard

## Create it automatically (recommended)

1. Run `notebooks/99_run_all` so the hub's tables and views exist.
2. Run `notebooks/08_create_dashboard`. It builds the dashboard from the views, creates it in your home
   folder, publishes it, and prints a link. Running it again updates the same dashboard.

It needs a SQL warehouse you can use. Leave widget 6 blank to pick one automatically, or paste a warehouse ID.

## If automatic creation is blocked: import the file

`DeltaForce_DataOps_Hub.lvdash.json` in this folder is the same dashboard, written for `team_9.default`.
In Databricks, open **Dashboards**, choose to import a dashboard from a file, and select it. Then pick a
SQL warehouse and publish. (Menu names may differ slightly by workspace version.)

To regenerate the file for another catalog or schema:

```python
from deltaforce_dq import Settings
from deltaforce_dq.dashboard import write_definition
write_definition(Settings(landing_path="", catalog="team_9", schema="default"), "DeltaForce_DataOps_Hub.lvdash.json")
```

## What is on it

Seven pages. "Latest day" means the latest file date the hub has processed.

| Page | KPIs | Visuals |
|---|---|---|
| Executive overview | Overall trust score (latest day), open alerts today, critical alerts, DQ pass rate, jobs failed today, files missing, revenue impact | Trust score trend, open alerts by severity, daily health summary with source, data and job trust |
| Data quality | Rules executed, passed, failed, pass %, critical failures | Pass rate by dataset, pass rate by data quality dimension, failed rules trend, quality by column, top failed rules, failed-records drill-down |
| Alerts and anomalies | Active alerts, critical alerts, new anomalies today, seeded issues caught | Severity split, anomaly trend (volume, revenue, quality), top alert categories, alert log, trend checks, detection scorecard |
| Source monitoring | Deliveries expected, received, delayed, missing, on-time %, source trust today | Source inventory (type, location, schedule, on-time %, source trust), delivery status by source, minutes late, delayed deliveries, all deliveries |
| Job monitoring | Total runs, successful, failed, average runtime, job trust today | Job health distribution, runtime by day, failed jobs, long-running and late jobs, all runs |
| Revenue impact | Revenue before, after, difference, impact % | Before vs after, revenue trend, impact by region, where the difference went, certification summary, quarantined rows |
| Trust score | Overall, source, data and job trust today; change vs previous day | Trust by layer, overall trust by dataset, drivers, trust by layer, dataset and day |

Domains are the datasets in `resources/monitored_datasets.csv` (Customer, Product, Sales today); adding a dataset there
adds it to every page. Running jobs are not shown: only finished runs are recorded.

Worth adding by hand after creation: a filter on Day across pages, and green / amber / red colours for the
status values.

## Writing your own tiles

`dataops_dashboard.sql` has one query per tile (written for `team_9.default`) if you'd rather build or extend
the dashboard by hand.

## Alerts

`alert_query.sql` holds six alert queries: critical alerts open, trust score below 90, failed jobs, missing files,
revenue variance over 10%, and high-severity quality failures. Create one Databricks SQL alert per query with the
condition written above it, and add an email, Microsoft Teams or PagerDuty destination.
