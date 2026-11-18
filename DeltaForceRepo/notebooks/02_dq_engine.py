# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Data quality engine
# MAGIC Runs every rule in `dq_rules.csv`: row checks, cross-table checks and trend checks. Add a check by adding a row to that file.
# COMMAND ----------

# MAGIC %run ./_setup
# COMMAND ----------

from deltaforce_dq import dq_engine

dq_engine.run(spark, settings)
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.dq_results WHERE status <> 'PASS' ORDER BY file_date, dataset, rule_id"""))
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.dq_failed_rows ORDER BY file_date, dataset, rule_id"""))
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.dq_daily_metrics ORDER BY dataset, file_date"""))
