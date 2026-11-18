# Databricks notebook source
# MAGIC %md
# MAGIC # Run the whole hub end to end
# MAGIC Runs all nine stages in order, starting with the CSV files in the input volume, and logs each stage in `hub_run_log`.
# COMMAND ----------

# MAGIC %run ./_setup
# COMMAND ----------

from deltaforce_dq.pipeline import run_all

run_all(spark, settings)
# COMMAND ----------

display(spark.sql(f"SELECT * FROM {ns}.vw_trust_overall_daily ORDER BY file_date"))
# COMMAND ----------

display(spark.sql(f"SELECT alert_id, file_date, dataset, category, severity, rule, message FROM {ns}.alert_log ORDER BY alert_id"))
# COMMAND ----------

display(spark.sql(f"SELECT * FROM {ns}.vw_revenue_before_after ORDER BY file_date"))
# COMMAND ----------

# MAGIC %md
# MAGIC To create or refresh the dashboard, run `08_create_dashboard` after this notebook.
