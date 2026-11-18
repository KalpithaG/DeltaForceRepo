"""Stage 2 - the config-driven data quality engine.

Every check is one row in dq_rules.csv. Supported rule types:

  row          not_null, unique, regex, valid_format, not_future, expression,
               duplicate_row, outlier_vs_median
  cross_table  ref_exists, ref_expression
  trend        key_disappeared, metric_change

Outputs
-------
dq_failed_rows    one row per failing record per rule
dq_results        one row per rule per file date (PASS / FAIL / NO_BASELINE)
dq_daily_metrics  per dataset per day: row counts, bad-row share, healthy-baseline flag
"""

from __future__ import annotations

from statistics import median
from typing import Dict, List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .config import Settings, read_table, write_table

FAILED_SCHEMA = (
    "rule_id string, rule_name string, dataset string, category string, severity string, action string, "
    "file_date date, file_name string, row_id string, record_key string, failed_value string"
)
RESULT_SCHEMA = (
    "rule_id string, rule_name string, dataset string, category string, rule_type string, severity string, "
    "action string, file_date date, total_rows long, failed_count long, failed_pct double, "
    "observed_value string, expected_value string, status string, message string"
)
METRIC_SCHEMA = (
    "dataset string, file_date date, total_rows long, bad_rows long, quarantined_rows long, "
    "bad_share double, quarantine_share double, is_healthy_baseline boolean"
)

RAW = "__raw"


# ---------------------------------------------------------------------------------
# Typed view of a raw table: <col> is the typed value, <col>__raw the original text
# ---------------------------------------------------------------------------------
def typed_view(spark, settings: Settings, ds: dict) -> DataFrame:
    return typed_from_raw(read_table(spark, settings, f"raw_{ds['table_suffix']}"), ds)


def typed_from_raw(raw: DataFrame, ds: dict) -> DataFrame:
    cols = []
    for name, dtype in ds["columns"]:
        cols.append(F.col(name).alias(name + RAW))
        if dtype == "string":
            cols.append(F.col(name))
        else:
            cols.append(F.expr(f"try_cast(`{name}` as {dtype})").alias(name))
    return raw.select(*cols, "_file_name", "_file_date", "_row_num", "_row_id")


def _fails(df: DataFrame, key_col: str, value) -> DataFrame:
    return df.select(
        F.col("_file_date").alias("file_date"),
        F.col("_file_name").alias("file_name"),
        F.col("_row_id").alias("row_id"),
        F.col(key_col + RAW).alias("record_key"),
        value.cast("string").alias("failed_value"),
    )


def _ref_snapshot_map(df: DataFrame, ref: DataFrame) -> DataFrame:
    """For each file date in df, the reference snapshot to validate against:
    the latest reference file on or before that date (else the earliest one)."""
    dates = sorted(r[0] for r in df.select("_file_date").distinct().collect() if r[0])
    ref_dates = sorted(r[0] for r in ref.select("_file_date").distinct().collect() if r[0])
    pairs = []
    for d in dates:
        earlier = [r for r in ref_dates if r <= d]
        if earlier or ref_dates:
            pairs.append((d, earlier[-1] if earlier else ref_dates[0]))
    return df.sparkSession.createDataFrame(pairs, "_file_date date, _ref_date date")


# ---------------------------------------------------------------------------------
# Rule implementations: each returns the failing records
# ---------------------------------------------------------------------------------
def apply_rule(rule: dict, ds: dict, views: Dict[str, DataFrame], settings: Settings) -> DataFrame:
    """Public wrapper: the records failing one rule, given typed views of every dataset."""
    return _apply_rule(rule, ds, views, settings)


def _apply_rule(rule: dict, ds: dict, views: Dict[str, DataFrame], settings: Settings) -> DataFrame:
    df = views[ds["dataset"]]
    key = ds["key_column"]
    col = rule.get("column")
    p = rule["params"]
    kind = rule["rule_type"]

    if kind == "not_null":
        return _fails(df.filter(F.col(col + RAW).isNull()), key, F.lit(None))

    if kind == "unique":
        w = Window.partitionBy("_file_name", col + RAW).orderBy("_row_num")
        dup = df.filter(F.col(col + RAW).isNotNull()).withColumn("_n", F.row_number().over(w)).filter("_n > 1")
        return _fails(dup, key, F.col(col + RAW))

    if kind == "regex":
        bad = df.filter(F.col(col + RAW).isNotNull() & ~F.col(col + RAW).rlike(p["pattern"]))
        return _fails(bad, key, F.col(col + RAW))

    if kind == "valid_format":
        bad = df.filter(F.col(col + RAW).isNotNull() & F.col(col).isNull())
        return _fails(bad, key, F.col(col + RAW))

    if kind == "not_future":
        return _fails(df.filter(F.col(col) > F.col("_file_date")), key, F.col(col + RAW))

    if kind == "expression":
        return _fails(df.filter(~F.expr(p["expr"])), key, F.col(col + RAW) if col else F.lit(None))

    if kind == "duplicate_row":
        parts = [c.strip() + RAW for c in p["columns"].split("|")]
        w = Window.partitionBy("_file_name", *parts).orderBy("_row_num")
        dup = df.withColumn("_n", F.row_number().over(w)).filter("_n > 1")
        return _fails(dup, key, F.concat_ws(" | ", *[F.coalesce(F.col(c), F.lit("")) for c in parts]))

    if kind == "outlier_vs_median":
        factor = float(p.get("factor", 20))
        med = (
            df.filter(F.col(col) > 0)
            .groupBy("_file_name")
            .agg(F.percentile_approx(col, 0.5).alias("_median"))
        )
        bad = df.join(med, "_file_name").filter(F.col(col) > F.col("_median") * F.lit(factor))
        return _fails(bad, key, F.col(col + RAW))

    if kind in ("ref_exists", "ref_expression"):
        ref_ds = settings.dataset(p["ref_dataset"])
        ref = views[ref_ds["dataset"]]
        ref_col = p["ref_column"]
        mapped = df.join(_ref_snapshot_map(df, ref), "_file_date", "left")

        if kind == "ref_exists":
            ref_keys = (
                ref.filter(F.col(ref_col + RAW).isNotNull())
                .select(F.col("_file_date").alias("_ref_date"), F.col(ref_col + RAW).alias("_ref_key"))
                .distinct()
            )
            bad = mapped.filter(F.col(col + RAW).isNotNull()).join(
                ref_keys,
                (mapped["_ref_date"] == ref_keys["_ref_date"]) & (mapped[col + RAW] == ref_keys["_ref_key"]),
                "left_anti",
            )
            return _fails(bad, key, F.col(col + RAW))

        # ref_expression: join the reference row in as ref_<column>, then test the expression
        join_key = p.get("key", ref_col)
        w = Window.partitionBy("_file_date", ref_col + RAW).orderBy("_row_num")
        ref_rows = (
            ref.filter(F.col(ref_col + RAW).isNotNull())
            .withColumn("_n", F.row_number().over(w))
            .filter("_n = 1")
            .select(
                F.col("_file_date").alias("_ref_date2"),
                F.col(ref_col + RAW).alias("_ref_key"),
                *[F.col(name).alias("ref_" + name) for name, _ in ref_ds["columns"]],
            )
        )
        joined = mapped.join(
            ref_rows,
            (mapped["_ref_date"] == ref_rows["_ref_date2"]) & (mapped[join_key + RAW] == ref_rows["_ref_key"]),
            "inner",
        )
        return _fails(joined.filter(~F.expr(p["expr"])), key, F.col(col + RAW) if col else F.lit(None))

    if kind == "key_disappeared":
        files = df.select("_file_date", "_file_name").distinct()
        files = files.withColumn("_prev_date", F.lag("_file_date").over(Window.orderBy("_file_date")))
        keys = df.filter(F.col(col + RAW).isNotNull()).select("_file_date", F.col(col + RAW).alias("_k")).distinct()
        prev_keys = keys.select(F.col("_file_date").alias("_prev_date"), "_k")
        gone = files.join(prev_keys, "_prev_date").join(keys, ["_file_date", "_k"], "left_anti")
        return gone.select(
            F.col("_file_date").alias("file_date"),
            F.col("_file_name").alias("file_name"),
            F.lit(None).cast("string").alias("row_id"),
            F.col("_k").alias("record_key"),
            F.col("_k").alias("failed_value"),
        )

    raise ValueError(f"Rule {rule['rule_id']}: unknown rule_type '{kind}'")


def _fmt(x) -> str:
    if x is None:
        return ""
    return f"{x:,.0f}" if abs(x) >= 100 or float(x).is_integer() else f"{x:,.2f}"


# ---------------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------------
def run(spark, settings: Settings) -> None:
    datasets = {d["dataset"]: d for d in settings.datasets()}
    views = {name: typed_view(spark, settings, d) for name, d in datasets.items()}
    rules = settings.rules()
    record_rules = [r for r in rules if r["rule_type"] != "metric_change"]
    metric_rules = [r for r in rules if r["rule_type"] == "metric_change"]

    # ---- 1. record-level rules -> dq_failed_rows -------------------------------
    failed = spark.createDataFrame([], FAILED_SCHEMA)
    for rule in record_rules:
        ds = datasets[rule["dataset"]]
        hits = _apply_rule(rule, ds, views, settings).select(
            F.lit(rule["rule_id"]).alias("rule_id"),
            F.lit(rule["rule_name"]).alias("rule_name"),
            F.lit(rule["dataset"]).alias("dataset"),
            F.lit(rule["category"]).alias("category"),
            F.lit(rule["severity"]).alias("severity"),
            F.lit(rule["action"]).alias("action"),
            "file_date", "file_name", "row_id", "record_key", "failed_value",
        )
        failed = failed.unionByName(hits)
    write_table(failed, settings, "dq_failed_rows")
    failed = read_table(spark, settings, "dq_failed_rows")

    # ---- 2. totals and per-rule failure counts ---------------------------------
    totals: Dict[tuple, int] = {}
    for name, view in views.items():
        for r in view.groupBy("_file_date").count().collect():
            if r["_file_date"] is not None:
                totals[(name, r["_file_date"])] = r["count"]

    counts = {
        (r["rule_id"], r["file_date"]): (r["n"], r["samples"])
        for r in failed.groupBy("rule_id", "file_date")
        .agg(F.count(F.lit(1)).alias("n"), F.slice(F.sort_array(F.collect_set("record_key")), 1, 3).alias("samples"))
        .collect()
    }

    results: List[tuple] = []
    for rule in record_rules:
        for (name, day), total in sorted(totals.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            if name != rule["dataset"]:
                continue
            n, samples = counts.get((rule["rule_id"], day), (0, []))
            status = "FAIL" if n else "PASS"
            if n:
                eg = ", ".join(s for s in samples if s)
                unit = "key(s)" if rule["rule_type"] == "key_disappeared" else f"of {total} rows"
                message = f"{n} {unit}: {rule['description']}" + (f" (e.g. {eg})" if eg else "")
            else:
                message = f"Passed: {rule['rule_name']}"
            results.append(
                (rule["rule_id"], rule["rule_name"], name, rule["category"], rule["rule_type"], rule["severity"],
                 rule["action"], day, total, n, round(100.0 * n / total, 2) if total else None,
                 str(n), "0", status, message)
            )

    # ---- 3. daily metrics on clean rows ----------------------------------------
    row_fails = failed.filter(F.col("row_id").isNotNull())
    bad = {
        (r["dataset"], r["file_date"]): (r["bad"], r["quarantined"])
        for r in row_fails.groupBy("dataset", "file_date")
        .agg(
            F.countDistinct("row_id").alias("bad"),
            F.countDistinct(F.when(F.col("action") == "quarantine", F.col("row_id"))).alias("quarantined"),
        )
        .collect()
    }
    quarantined_ids = row_fails.filter("action = 'quarantine'").select(F.col("row_id").alias("_row_id")).distinct()

    metric_values: Dict[tuple, float] = {}  # (rule_id, day) -> value
    for rule in metric_rules:
        view = views[rule["dataset"]].join(quarantined_ids, "_row_id", "left_anti")
        metric = rule["params"].get("metric", "row_count")
        agg = F.count(F.lit(1)) if metric == "row_count" else F.sum(F.col(rule["column"]).cast("double"))
        for r in view.groupBy("_file_date").agg(agg.alias("v")).collect():
            if r["_file_date"] is not None:
                metric_values[(rule["rule_id"], r["_file_date"])] = float(r["v"] or 0)

    # ---- 4. trend rules, walked day by day, learning only from healthy days ----
    daily: List[tuple] = []
    for name in datasets:
        days = sorted(day for (ds_name, day) in totals if ds_name == name)
        ds_rules = [r for r in metric_rules if r["dataset"] == name]
        healthy_history: Dict[str, List[float]] = {r["rule_id"]: [] for r in ds_rules}

        for day in days:
            total = totals[(name, day)]
            n_bad, n_quar = bad.get((name, day), (0, 0))
            quarantine_share = n_quar / total if total else 0.0
            day_anomaly = False
            todays: Dict[str, float] = {}

            for rule in ds_rules:
                p = rule["params"]
                # Rows quarantined on a fully-bad day leave no clean rows: treat as zero.
                value = metric_values.get((rule["rule_id"], day), 0.0)
                todays[rule["rule_id"]] = value
                history = healthy_history[rule["rule_id"]][-settings.baseline_window:]
                label = "row count" if p.get("metric", "row_count") == "row_count" else f"{rule['column']} total"

                if not history or median(history) == 0:
                    results.append(
                        (rule["rule_id"], rule["rule_name"], name, rule["category"], rule["rule_type"],
                         rule["severity"], rule["action"], day, total, 0, None, _fmt(value), None, "NO_BASELINE",
                         f"{name} {label} {_fmt(value)}: no healthy baseline yet")
                    )
                    continue

                baseline = median(history)
                pct = (value - baseline) / baseline * 100.0
                drop = float(p["drop_pct"]) if p.get("drop_pct") else None
                spike = float(p["spike_pct"]) if p.get("spike_pct") else None
                is_fail = (drop is not None and pct <= -drop) or (spike is not None and pct >= spike)
                limit = f"-{drop:.0f}%" if drop is not None else f"+{spike:.0f}%"
                day_anomaly = day_anomaly or is_fail
                results.append(
                    (rule["rule_id"], rule["rule_name"], name, rule["category"], rule["rule_type"],
                     rule["severity"], rule["action"], day, total, 1 if is_fail else 0, None,
                     _fmt(value), _fmt(baseline), "FAIL" if is_fail else "PASS",
                     f"{name} {label} {_fmt(value)} vs healthy baseline {_fmt(baseline)} "
                     f"({pct:+,.0f}%, alert at {limit})")
                )

            healthy = quarantine_share <= settings.healthy_max_bad_share and not day_anomaly
            if healthy:
                for rule_id, value in todays.items():
                    healthy_history[rule_id].append(value)
            daily.append(
                (name, day, total, n_bad, n_quar, round(n_bad / total, 4) if total else 0.0,
                 round(quarantine_share, 4), healthy)
            )

    write_table(spark.createDataFrame(results, RESULT_SCHEMA), settings, "dq_results")
    write_table(spark.createDataFrame(daily, METRIC_SCHEMA), settings, "dq_daily_metrics")
