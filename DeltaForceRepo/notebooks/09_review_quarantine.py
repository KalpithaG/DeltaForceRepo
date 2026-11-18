# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 09 - Review quarantined records (notebook version of the review app)
# MAGIC Approve, correct or reject a quarantined record, then apply the decision straight away.
# MAGIC
# MAGIC 1. Run the first two cells to see the queue.
# MAGIC 2. Fill in widgets 6 to 10 and run the rest of the notebook.
# MAGIC
# MAGIC - **APPROVE**: the record is released into the certified table as received.
# MAGIC - **MODIFY**: give the corrected values in widget 9, as `Field=value; Field=value`. The record is re-checked against every rule and released only if it now passes. An empty value (`Sale_Date=`) clears a field.
# MAGIC - **REJECT**: the record stays quarantined, marked as reviewed.
# MAGIC
# MAGIC Alerts, the scorecard and the trust score are not changed: they always describe the data as it arrived.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

display(spark.sql(f"""
    SELECT dataset, record_key, file_date, reasons, review_status, latest_decision, decision_by,
           awaiting_hub_run, record_json, row_id
    FROM {ns}.vw_quarantine_queue
    ORDER BY dataset, file_date, record_key
"""))

# COMMAND ----------

dbutils.widgets.dropdown("review_dataset", "Sales", [d["dataset"] for d in settings.datasets()], "6. Dataset")
dbutils.widgets.text("review_key", "", "7. Record key (e.g. S004)")
dbutils.widgets.dropdown("review_decision", "APPROVE", ["APPROVE", "MODIFY", "REJECT"], "8. Decision")
dbutils.widgets.text("review_corrections", "", "9. Corrections (MODIFY only): Field=value; Field=value")
dbutils.widgets.text("review_comment", "", "10. Comment")

# COMMAND ----------

from deltaforce_dq import review

dataset = dbutils.widgets.get("review_dataset")
key = dbutils.widgets.get("review_key").strip()
decision = dbutils.widgets.get("review_decision")
comment = dbutils.widgets.get("review_comment").strip()
corrections = {}
for part in dbutils.widgets.get("review_corrections").split(";"):
    if "=" in part:
        field, value = part.split("=", 1)
        corrections[field.strip()] = value.strip()

if not key:
    raise ValueError("Fill in widget 7 with the record key, for example S004.")
matches = spark.sql(f"SELECT row_id FROM {ns}.vw_quarantine_queue WHERE dataset = '{dataset}' AND record_key = '{key}'").collect()
if len(matches) != 1:
    raise ValueError(f"Found {len(matches)} quarantined {dataset} records with key {key}; expected exactly one.")
if decision != "MODIFY":
    corrections = {}
elif not corrections:
    raise ValueError("For MODIFY, give the corrected values in widget 9, for example: Quantity=5; Revenue=1250")
if decision != "MODIFY" and not comment:
    raise ValueError("Add a short comment in widget 10 so the decision can be audited.")

decision_id = review.record_decision(spark, settings, dataset, matches[0][0], decision,
                                     corrections=corrections, comment=comment, record_key=key)
print(f"Saved {decision} for {dataset} {key} (decision {decision_id}).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply the decision now
# MAGIC Re-runs only the certify and view stages (a few seconds), so the dashboard reflects the decision.

# COMMAND ----------

from deltaforce_dq.pipeline import run_all

run_all(spark, settings, stages=["quarantine_certify", "dashboard_views"])
display(spark.sql(f"""
    SELECT dataset, record_key, review_status, recheck_failed, reviewed_by, review_comment
    FROM {ns}.vw_quarantine_queue WHERE dataset = '{dataset}' AND record_key = '{key}'
"""))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {ns}.certification_summary ORDER BY file_date, dataset"))