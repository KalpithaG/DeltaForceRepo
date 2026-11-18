# Databricks notebook source
# MAGIC %md
# MAGIC ## Shared setup
# MAGIC Defines the widgets, makes the `deltaforce_dq` package importable and builds `settings`.
# MAGIC Every other notebook starts with `%run ./_setup`. Change the widget values once, at the top of any notebook.
# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "team_9", "1. Catalog")
dbutils.widgets.text("schema", "default", "2. Schema for hub tables")
dbutils.widgets.text("landing_path", "", "3. CSV folder (blank = /Volumes/<catalog>/default/inputdata)")
dbutils.widgets.text("config_path", "", "4. Config path (blank = ../resources)")
dbutils.widgets.dropdown("job_source", "simulated", ["simulated", "both", "system_tables"], "5. Job history source")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
landing_path = dbutils.widgets.get("landing_path").strip() or f"/Volumes/{catalog}/default/inputdata"
config_path = dbutils.widgets.get("config_path").strip() or os.path.abspath("../resources")
# COMMAND ----------

# Use the installed wheel if there is one; otherwise import straight from the repo's src folder.
try:
    import deltaforce_dq
except ImportError:
    sys.path.insert(0, os.path.abspath("../src"))
    import deltaforce_dq

from deltaforce_dq import Settings

settings = Settings(
    landing_path=landing_path,
    catalog=catalog or None,
    schema=schema,
    config_path=config_path,
    job_source=dbutils.widgets.get("job_source"),
)
ns = settings.namespace
print(f"deltaforce_dq {deltaforce_dq.__version__} | tables: {ns} | landing: {landing_path} | config: {config_path}")
