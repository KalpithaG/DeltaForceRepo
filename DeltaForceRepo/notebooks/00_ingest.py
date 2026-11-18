# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Load source tables and ingest
# MAGIC 1. Reads the Sales, Customer and Product CSV files from the input volume, including its subfolders (`inputdata/Sales/`, `inputdata/Customers/`, `inputdata/Product/`).
# MAGIC 2. Creates the source tables `<catalog>.default.sales`, `customers` and `product` from them (replacing them if they exist).
# MAGIC 3. Loads those tables into the hub's raw tables with file lineage on each row.
# MAGIC
# MAGIC Table names and file patterns come from `resources/monitored_datasets.csv`.
# COMMAND ----------

# MAGIC %run ./_setup
# COMMAND ----------

# Check the CSV files are where we expect them (the folder itself and any subfolders).
csv_files = sorted(
    os.path.relpath(os.path.join(folder, f), landing_path)
    for folder, _, names in os.walk(landing_path)
    for f in names
    if f.lower().endswith(".csv")
)
print(f"{len(csv_files)} CSV files under {landing_path}")
for f in csv_files:
    print("  ", f)
if not csv_files:
    raise FileNotFoundError(f"No CSV files found under {landing_path}. Check widget 3 (CSV folder).")
# COMMAND ----------

from deltaforce_dq import load_tables

load_tables.run(spark, settings)
for ds in settings.datasets():
    n = spark.table(ds["source_table"]).count()
    print(f"{ds['source_table']}: {n} rows")
# COMMAND ----------

display(spark.table(settings.dataset("Sales")["source_table"]).orderBy("source_file", "Sales_ID"))
# COMMAND ----------

from deltaforce_dq import ingest

ingest.run(spark, settings)
display(spark.table(f"{ns}.file_inventory").orderBy("dataset", "file_date"))
