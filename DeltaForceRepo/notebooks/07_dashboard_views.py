# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Dashboard views
# MAGIC Creates the views that the dashboard and the Genie space read from.
# COMMAND ----------

# MAGIC %run ./_setup
# COMMAND ----------

from deltaforce_dq import views

views.run(spark, settings)
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.vw_trust_overall_daily ORDER BY file_date"""))
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.vw_revenue_before_after ORDER BY file_date"""))
