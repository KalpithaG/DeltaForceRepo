# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Trust score and detection scorecard
# MAGIC Scores each dataset each day from 0 to 100 and checks the hub against the known seeded issues in `expected_issues.csv`.
# COMMAND ----------

# MAGIC %run ./_setup
# COMMAND ----------

from deltaforce_dq import trust

trust.run(spark, settings)
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.trust_score ORDER BY file_date, dataset"""))
# COMMAND ----------

display(spark.sql(f"""SELECT official_scenario, COUNT(*) AS issues, SUM(CASE WHEN caught THEN 1 ELSE 0 END) AS caught FROM {ns}.detection_scorecard GROUP BY official_scenario"""))
# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {ns}.detection_scorecard ORDER BY issue_id"""))
