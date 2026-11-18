# Delta Force - Data Quality & Monitoring Hub

A config-driven, self-healing data reliability hub for Databricks. It watches file arrivals,
data quality and job health, scores every dataset every day, and keeps bad data out of reporting.

## What it does

| Layer | Checks | Sample scenario it catches |
|---|---|---|
| File | missing, late, identical re-sent files | 6 Nov: Customer file late |
| Row | nulls, duplicates, bad formats, negatives, outliers | 5 Nov: bad data |
| Cross-table | unknown customer / product IDs, revenue not equal to quantity x price | 5 Nov: invalid references |
| Trend | volume drop, revenue drop, revenue spike, records vanishing from a snapshot | 7 Nov drop, 8 Nov spike |
| Job | failed, SLA breach, long-running, missed | simulated history and real runs from system tables |

On top of the checks:

- **Alert log**: one table with every finding (`alert_log`).
- **Trust score**: 0-100 per dataset per day, with SAFE TO REPORT / USE WITH CAUTION / DO NOT REPORT.
- **Self-healing**: failing rows go to `quarantine_<dataset>`; clean rows go to `certified_<dataset>`;
  a day with a critical trend alert is HELD out of reporting.
- **Detection scorecard**: every known seeded issue and whether the hub caught it.

## Results on the 12 sample files

| Day | Official scenario | Hub result | Sales revenue: raw -> certified |
|---|---|---|---|
| 5 Nov | Bad data, duplicates, nulls, invalid references | 14 alerts, trust 0, 7 of 10 sales rows quarantined | 256,900 -> 9,950 |
| 6 Nov | Customer file delayed | file_late alert, Customer trust 80 | 52,400 -> 52,400 |
| 7 Nov | Sales volume drops | volume and revenue drop alerts, Sales trust 40 | 15,900 -> 15,900 (flagged) |
| 8 Nov | Massive revenue spike | critical spike alert, Sales trust 50, day held | 2,789,500 -> 0 (held for review) |

Scorecard: 18 of 18 official seeded issues caught, plus 5 extra findings.

## Quick start on Databricks (notebooks)

1. Open the repo as a Git folder in your workspace.
2. Put the Sales, Customer and Product CSV files in `/Volumes/team_9/default/inputdata/`, either directly
   or in one subfolder per dataset (`Sales/`, `Customers/`, `Product/`).
3. Open `notebooks/00_ingest` and run it. The defaults are catalog `team_9`, schema `default` and that folder;
   change the widgets if yours differ.
4. Run `notebooks/99_run_all`.
5. Run `notebooks/08_create_dashboard` to create and publish the dashboard (see `sql/README.md`).
6. Create the alert from `sql/alert_query.sql` and the Genie space from `genie/genie_setup.md`.
7. Follow `docs/day_plan.md` for the hackathon day.

The notebooks import the package straight from `src/`, so no install is needed. Notebooks `01`-`07`
run one stage each, for walking judges through the logic.

## How the data flows

```
/Volumes/team_9/default/inputdata/<Sales|Customers|Product>/*.csv
   -> team_9.default.sales, customers, product     (source tables, created by the hub)
   -> raw_*                                        (raw layer, with file lineage)
   -> quarantine_* and certified_*                 (cleaned layer)
   -> alert_log, trust_score, vw_*                 (ready layer: dashboard, alerts, Genie)
```

The first step replaces the three source tables on every run. Each keeps the CSV values as text and adds
`source_file`, `file_arrival_ts` and `load_ts`. A file belongs to a dataset if it sits in a folder named after
the dataset (singular or plural, any letter case) or its own name matches the dataset's file pattern.
File names must end in `DDMMYYYY_HHMMSS.csv`. Table names and file patterns are set in
`resources/monitored_datasets.csv`; `{catalog}` there is filled from the catalog setting.

## Reviewing quarantined records

Quarantined records can be approved, corrected or rejected:

- **Review app** (`apps/quarantine_review`): a Databricks App for reviewers. Setup in its README.
- **Notebook** (`notebooks/09_review_quarantine`): the same decisions from widgets, applied straight away.

Decisions are appended to `quarantine_decisions` (never overwritten) with the reviewer and a comment.
On each run the hub applies the latest decision per record: approved records are certified as received;
corrected records are re-checked against every rule and certified only if they pass; rejected records stay
quarantined. Alerts, the scorecard and the trust score always describe the data as it arrived.

## Repo layout

| Folder | Contents |
|---|---|
| `notebooks/` | `00`-`07` one stage each, `99_run_all`, `08_create_dashboard`, `09_review_quarantine`, shared `_setup` |
| `apps/quarantine_review/` | Databricks App for reviewing quarantined records |
| `sql/` | Dashboard import file, tile queries, alert query |
| `genie/` | Genie space tables, instructions and sample questions |
| `resources/` | The five config files |
| `src/deltaforce_dq/` | The package behind the notebooks |
| `docs/` | Architecture, data dictionary, demo script, slide content, day plan |
| `demo/` | Screenshots and backup video link (added on the day) |
| `sample_data/` | The 12 synthetic sample files |
| `tests/`, `dist/`, `databricks.yml` | Tests, built wheel, optional bundle |

## Job history: simulated, real, or both

The `job_source` widget (or `--job-source`) chooses where job runs come from:

- `simulated` (default): a repeatable history generated from `jobs_config.csv`. Works anywhere.
- `system_tables`: real runs from `system.lakeflow.job_run_timeline`, named through `system.lakeflow.jobs`.
- `both`: the simulated four-day story plus the real runs. Falls back to simulated, with a warning, if the
  system tables cannot be read.

Real jobs listed in `jobs_config.csv` are checked against their schedule and SLA. Real jobs that are not
listed are still checked for failures, and for long runs against their own median once they have three runs.
New runs can take up to about an hour to appear in system tables.

## Other ways to source the data

The hub is not tied to CSV. Each dataset reads either files from a path or a Unity Catalog table:

| Source | How to configure it |
|---|---|
| Files in a Unity Catalog volume, Amazon S3 or ADLS Gen2 | Set the landing path to the volume, `s3://...` or `abfss://...` path (through a Unity Catalog external location) |
| Delta Sharing | Mount the share as a catalog; set `source_table` to the shared table and `load_from_files` to `N` |
| External database (Lakehouse Federation) | Create a foreign catalog; set `source_table` to the foreign table |
| Another workspace | Use the table through the shared Unity Catalog metastore, or Delta Sharing |

Set `source_type` and `source_location` in `monitored_datasets.csv` to describe each source on the Source
monitoring page. Tested here: CSV files from a folder, and pre-loaded tables with and without a file-name column.

All set per dataset in `resources/monitored_datasets.csv`:

| Column | Meaning |
|---|---|
| `source_table` | Table the hub reads from |
| `load_from_files` | `Y`: the hub creates `source_table` from the CSV files. `N`: the table already exists and is left alone |
| `file_name_column` | Column holding the original file name. Date and time are read from it |
| `file_date_expr` | SQL expression for the business date, used when there is no file name column |
| `arrival_ts_expr` | SQL expression for the arrival time (optional) |

Leave `source_table` blank to skip the source tables and read the CSV files directly into the raw layer.
With a table that has no file name column, missing days and all quality and trend checks still work,
but late-file detection needs an arrival time.

## Run it as a wheel (any workspace)

```bash
scripts/build.sh                      # builds dist/deltaforce_dq-0.1.0-py3-none-any.whl
```

- **As a job task**: upload the wheel, add a *Python wheel* task with package `deltaforce_dq`,
  entry point `dataops_hub`, and parameters
  `["--catalog","team_9","--schema","default","--landing-path","/Volumes/team_9/default/inputdata"]`.
- **With a bundle**: `databricks bundle deploy -t dev` then
  `databricks bundle run dataops_hub -t dev` (see `databricks.yml`).
- **In a notebook**: `%pip install /path/to/deltaforce_dq-0.1.0-py3-none-any.whl`, then
  `from deltaforce_dq.pipeline import run_all`.

The wheel carries a copy of the config files. Pass `--config-path` to use your own folder.

## Adapting it to new data

Everything is driven by five CSV files in `resources/`:

| File | Purpose |
|---|---|
| `monitored_datasets.csv` | datasets, file name pattern, key column, column types, measure to reconcile |
| `file_monitor_config.csv` | expected arrival time, grace minutes, severities |
| `dq_rules.csv` | one row per check: type, column, parameters, severity, action |
| `jobs_config.csv` | jobs, schedules, SLAs, and which days the simulator fails or slows |
| `expected_issues.csv` | optional answer key for the detection scorecard |

Rule types: `not_null`, `unique`, `regex`, `valid_format`, `not_future`, `expression`, `duplicate_row`,
`outlier_vs_median`, `ref_exists`, `ref_expression`, `key_disappeared`, `metric_change`.
Actions: `quarantine` (remove the row), `warn` (keep and flag), `hold` (keep the whole day out of reporting).

## How the two less obvious parts work

**Trend baseline.** Volume and revenue are compared with the median of recent *healthy* days.
A day with more than 5% of rows quarantined, or with a trend alert of its own, never becomes a baseline.
That is why the bad 5 Nov data does not distort the checks on 7 and 8 Nov.

**Trust score.** Start at 100. Remove 5 points for every 1% of rows failing a quality rule.
Remove 50 / 30 / 15 / 5 points for each Critical / High / Medium / Low file or trend alert.
90+ is safe, 70-89 is caution, below 70 is do not report.

**Trust by layer.** The same penalties, split by where the problem is: **source trust** (late, missing or
re-sent deliveries), **data trust** (bad rows and anomalies in the processed data) and **job trust** (failed,
late or slow jobs, scored the same way). The overall score covers source and data; job trust is shown beside it,
because a failed job does not make the data itself wrong.

## Tables

`raw_<dataset>`, `file_inventory`, `file_monitor_results`, `dq_results`, `dq_failed_rows`,
`dq_daily_metrics`, `job_runs`, `job_monitor_results`, `alert_log`, `trust_score`,
`detection_scorecard`, `quarantine_<dataset>`, `certified_<dataset>`, `certification_summary`,
`hub_run_log`, and the `vw_*` dashboard views.

## Extended synthetic data (optional)

```bash
dataops_datagen --out /Volumes/team_9/default/generated --days 12 --sales-per-day 2000
dataops_hub --catalog team_9 --schema dataops_demo \
            --landing-path /Volumes/team_9/default/generated/landing --config-path /Volumes/team_9/default/generated/config
```

Use a separate schema, as here, so the generated run does not overwrite the hub tables of the real run.
Its source tables are named `generated_sales`, `generated_customers` and `generated_product` in that schema.

Generates 12 days and about 22,000 sales rows with bad data (day 3), a 90-minute late file (day 5),
a missing file (day 6), a volume drop (day 8) and a revenue spike (day 10), plus its own answer key.

## Testing

```bash
pip install pyspark pytest
pytest tests/                 # end-to-end assertions on the sample files
python tests/run_local.py     # prints the main outputs
```
