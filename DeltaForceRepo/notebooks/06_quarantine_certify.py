# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Quarantine and certify
# MAGIC Moves failing rows to quarantine, publishes clean certified tables, and holds any day with a critical trend alert out of reporting.
# COMMAND ----------

# MAGIC %run ./_setup
# COMMAND ----------

from deltaforce_dq import certify

certify.run(spark, settings)
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.certification_summary ORDER BY dataset, file_date"""))
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.quarantine_sales ORDER BY _file_date, Sales_ID"""))
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.certified_sales ORDER BY _file_date, Sales_ID"""))
