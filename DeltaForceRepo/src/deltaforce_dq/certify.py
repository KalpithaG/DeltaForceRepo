"""Stage 6 - self-healing: quarantine bad rows, apply review decisions, publish certified tables.

Rule actions (dq_rules.csv, column ``action``):
  quarantine  the failing row is removed from the certified table and kept, with reasons, in quarantine
  warn        the row stays certified and carries the warning in ``_dq_warnings``
  hold        (trend rules) the whole day stays out of reporting until someone reviews it

Review decisions (quarantine_decisions, see review.py) on quarantined rows:
  APPROVE -> released as received; MODIFY -> corrected, re-checked, released only if it passes;
  REJECT -> stays quarantined. Rows with no decision are PENDING.

Outputs
-------
quarantine_<dataset>    every row that failed a quarantine rule, original text, reasons and review status
certified_<dataset>     typed rows fit for reporting; certification_status = CERTIFIED or HELD;
                        review_status = APPROVED or CORRECTED for released rows
certification_summary   per dataset per day: raw vs certified vs quarantined vs held vs released
"""

from __future__ import annotations

from pyspark.sql import functions as F

from . import review
from .config import Settings, read_table, write_table


def run(spark, settings: Settings) -> None:
    failed = read_table(spark, settings, "dq_failed_rows").filter(F.col("row_id").isNotNull())
    results = read_table(spark, settings, "dq_results")
    decisions = review.latest_decisions(spark, settings)

    # Only quarantined rows can be reviewed.
    quarantined_ids = (
        failed.filter("action = 'quarantine'").select("dataset", "row_id").distinct()
    )
    decisions = decisions.join(quarantined_ids, ["dataset", "row_id"], "left_semi")
    modify = decisions.filter("decision = 'MODIFY'").select("dataset", "row_id", "corrections")
    rechecked = review.recheck(spark, settings, modify)
    summaries = []

    for ds in settings.datasets():
        name, suffix = ds["dataset"], ds["table_suffix"]
        columns = [c for c, _ in ds["columns"]]
        raw = read_table(spark, settings, f"raw_{suffix}")
        mine = failed.filter(F.col("dataset") == name)

        reasons = (
            mine.filter("action = 'quarantine'")
            .groupBy(F.col("row_id").alias("_row_id"))
            .agg(F.sort_array(F.collect_set("rule_name")).alias("_dq_rules_failed"))
        )
        warnings = (
            mine.filter("action = 'warn'")
            .groupBy(F.col("row_id").alias("_row_id"))
            .agg(F.sort_array(F.collect_set("rule_name")).alias("_dq_warnings"))
        )
        held_days = (
            results.filter((F.col("dataset") == name) & (F.col("status") == "FAIL") & (F.col("action") == "hold"))
            .groupBy(F.col("file_date").alias("_file_date"))
            .agg(F.concat_ws("; ", F.collect_set("message")).alias("_hold_reason"))
        )

        # ---- review status of every quarantined row ------------------------------
        mine_dec = decisions.filter(F.col("dataset") == name).select(
            F.col("row_id").alias("_row_id"), "decision", "corrections", "comment", "reviewer", "decided_at")
        mine_chk = rechecked.filter(F.col("dataset") == name).select(
            F.col("row_id").alias("_row_id"), "still_failing")
        status = (
            reasons.join(mine_dec, "_row_id", "left")
            .join(mine_chk, "_row_id", "left")
            .withColumn(
                "_review_status",
                F.when(F.col("decision") == "APPROVE", "APPROVED")
                .when((F.col("decision") == "MODIFY") & F.col("still_failing").isNull(), "CORRECTED")
                .when(F.col("decision") == "MODIFY", "CORRECTION_FAILED")
                .when(F.col("decision") == "REJECT", "REJECTED")
                .otherwise("PENDING"),
            )
        )
        released = status.filter(F.col("_review_status").isin("APPROVED", "CORRECTED")).select("_row_id", "_review_status")
        blocked = status.filter(~F.col("_review_status").isin("APPROVED", "CORRECTED")).select("_row_id")

        quarantine = raw.join(status, "_row_id", "inner").select(
            *columns, "_dq_rules_failed", "_file_name", "_file_date", "_row_id",
            "_review_status",
            F.col("reviewer").alias("_reviewed_by"),
            F.col("comment").alias("_review_comment"),
            F.col("decided_at").alias("_decided_at"),
            F.col("corrections").alias("_corrections"),
            F.coalesce(F.col("still_failing"), F.array().cast("array<string>")).alias("_recheck_failed"),
            F.current_timestamp().alias("_quarantined_at"),
        )
        write_table(quarantine, settings, f"quarantine_{suffix}")

        # Corrected values replace the originals only for rows that passed the re-check.
        corrected_ok = modify.filter(F.col("dataset") == name).join(
            status.filter("_review_status = 'CORRECTED'").select(F.col("_row_id").alias("row_id")), "row_id", "left_semi")
        effective = review.apply_corrections(raw, ds, corrected_ok)

        typed = [
            F.col(c) if t == "string" else F.expr(f"try_cast(`{c}` as {t})").alias(c) for c, t in ds["columns"]
        ]
        certified = (
            effective.join(blocked, "_row_id", "left_anti")
            .join(released, "_row_id", "left")
            .join(warnings, "_row_id", "left")
            .join(held_days, "_file_date", "left")
            .select(
                *typed,
                F.when(F.col("_hold_reason").isNotNull(), "HELD").otherwise("CERTIFIED").alias("certification_status"),
                F.col("_hold_reason").alias("hold_reason"),
                F.col("_review_status").alias("review_status"),
                F.coalesce(F.col("_dq_warnings"), F.array().cast("array<string>")).alias("_dq_warnings"),
                "_file_name", "_file_date", "_row_id",
            )
        )
        write_table(certified, settings, f"certified_{suffix}")

        # ---- summary: what reporting would have shown vs what is certified -------
        measure = ds.get("measure_column")
        raw_value = F.expr(f"try_cast(`{measure}` as double)") if measure else F.lit(None).cast("double")
        flagged = (
            raw.join(blocked.select("_row_id", F.lit(True).alias("_q")), "_row_id", "left")
            .join(released.select("_row_id", F.lit(True).alias("_r")), "_row_id", "left")
            .join(held_days.select("_file_date", F.lit(True).alias("_h")), "_file_date", "left")
            .join(effective.select("_row_id", (F.expr(f"try_cast(`{measure}` as double)") if measure
                                               else F.lit(None).cast("double")).alias("_eff")), "_row_id", "left")
            .withColumn("_v", raw_value)
            .withColumn(
                "_bucket",
                F.when(F.col("_q"), "quarantined").when(F.col("_h"), "held").otherwise("certified"),
            )
            # certified and held amounts use corrected values where a correction was accepted
            .withColumn("_bv", F.when(F.col("_bucket") == "quarantined", F.col("_v")).otherwise(F.col("_eff")))
        )

        def rows(bucket):
            return F.sum(F.when(F.col("_bucket") == bucket, 1).otherwise(0))

        def total(bucket):
            return F.round(F.coalesce(F.sum(F.when(F.col("_bucket") == bucket, F.col("_bv"))), F.lit(0.0)), 2)

        summaries.append(
            flagged.groupBy(F.col("_file_date").alias("file_date")).agg(
                F.count(F.lit(1)).alias("raw_rows"),
                rows("certified").alias("certified_rows"),
                rows("quarantined").alias("quarantined_rows"),
                rows("held").alias("held_rows"),
                F.sum(F.when(F.col("_r"), 1).otherwise(0)).alias("released_rows"),
                F.round(F.coalesce(F.sum("_v"), F.lit(0.0)), 2).alias("raw_measure"),
                total("certified").alias("certified_measure"),
                total("quarantined").alias("quarantined_measure"),
                total("held").alias("held_measure"),
            ).select(
                F.lit(name).alias("dataset"),
                F.lit(measure).cast("string").alias("measure"),
                "*",
            )
        )

    summary = summaries[0]
    for other in summaries[1:]:
        summary = summary.unionByName(other)
    write_table(summary, settings, "certification_summary")
