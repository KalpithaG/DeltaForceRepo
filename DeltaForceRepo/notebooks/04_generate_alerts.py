# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Generate alerts
# MAGIC Writes every finding from the three monitors into one alert log.
# COMMAND ----------

# MAGIC %run ./_setup
# COMMAND ----------

from deltaforce_dq import alerts

alerts.run(spark, settings)
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.alert_log ORDER BY alert_id"""))
