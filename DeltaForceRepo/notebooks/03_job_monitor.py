# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Job monitoring
# MAGIC Flags failed jobs, SLA breaches, long runs and missed runs. The `job_source` widget chooses the history: `simulated` (generated from `jobs_config.csv`), `system_tables` (real runs from `system.lakeflow`), or `both`.
# COMMAND ----------

# MAGIC %run ./_setup
# COMMAND ----------

from deltaforce_dq import job_monitor

job_monitor.run(spark, settings)
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.job_runs ORDER BY run_date, job_name"""))
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.job_monitor_results ORDER BY run_date, job_name"""))
