# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - File monitoring
# MAGIC Checks every expected file against `file_monitor_config.csv`: missing, late, or identical to the previous file.
# COMMAND ----------

# MAGIC %run ./_setup
# COMMAND ----------

from deltaforce_dq import file_monitor

file_monitor.run(spark, settings)
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.file_monitor_results ORDER BY file_date, dataset"""))
