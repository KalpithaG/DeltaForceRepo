# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08 - Create the dashboard
# MAGIC Builds the AI/BI dashboard from the hub's views and creates it in this workspace, then publishes it.
# MAGIC Run `99_run_all` first so the views exist. Running this again updates the same dashboard.
# MAGIC
# MAGIC - **SQL warehouse:** blank picks one you can use (a running one first). Paste an ID to choose.
# MAGIC - **Folder:** blank puts the dashboard in your home folder.
# MAGIC - If creation is blocked, import `sql/DeltaForce_DataOps_Hub.lvdash.json` by hand instead (see `sql/README.md`).

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

dbutils.widgets.text("warehouse_id", "", "6. SQL warehouse ID (blank = pick one)")
dbutils.widgets.text("dashboard_folder", "", "7. Dashboard folder (blank = your home folder)")
dbutils.widgets.text("dashboard_name", "Delta Force DataOps Hub", "8. Dashboard name")
dbutils.widgets.dropdown("publish", "yes", ["yes", "no"], "9. Publish")

# COMMAND ----------

from deltaforce_dq.dashboard import deploy

result = deploy(
    settings,
    warehouse_id=dbutils.widgets.get("warehouse_id").strip() or None,
    parent_path=dbutils.widgets.get("dashboard_folder").strip() or None,
    display_name=dbutils.widgets.get("dashboard_name").strip() or "Delta Force DataOps Hub",
    publish=dbutils.widgets.get("publish") == "yes",
)
for k, v in result.items():
    print(f"{k:>13}: {v}")
displayHTML(f'<a href="{result["url"]}" target="_blank">Open the dashboard</a>')