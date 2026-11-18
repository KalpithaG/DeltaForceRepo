# Quarantine review app

A Databricks App where reviewers approve, correct or reject the records the hub quarantined.
Decisions go into `team_9.default.quarantine_decisions`; the hub applies them on its next run.

| Decision | Effect on the next hub run |
|---|---|
| Approve as received | The record moves into the certified table unchanged, marked APPROVED |
| Correct the values | The corrected record is re-checked against every rule. If it passes, it is certified with the new values (CORRECTED); if not, it stays quarantined and the app shows which rules still fail |
| Reject | The record stays quarantined, marked REJECTED |

Alerts, the scorecard and the trust score always describe the data as it arrived; only certification follows the review.

## Set it up (about 10 minutes)

1. Run `notebooks/99_run_all` once with the updated code. It creates `quarantine_decisions` and the view
   `vw_quarantine_queue` the app reads.
2. In the workspace, go to **Compute > Apps > Create app** and choose a custom app. Name it, for example
   `quarantine-review`.
3. Under **App resources**, add a **SQL warehouse** with the key `sql-warehouse` and permission *Can use*.
4. Point the app at this folder (`apps/quarantine_review` in your Git folder) and deploy.
5. Give the app's service principal access to the data. Its name is on the app's page; run:

```sql
GRANT USE CATALOG ON CATALOG team_9 TO `<app service principal>`;
GRANT USE SCHEMA ON SCHEMA team_9.default TO `<app service principal>`;
GRANT SELECT ON TABLE team_9.default.vw_quarantine_queue TO `<app service principal>`;
GRANT SELECT, MODIFY ON TABLE team_9.default.quarantine_decisions TO `<app service principal>`;
```

6. Optional: to get a **Run the hub now** button, set `HUB_JOB_ID` in `app.yaml` to the hub job's ID and give the
   service principal *Can manage run* on that job.

If your catalog or schema is not `team_9.default`, change `HUB_CATALOG` and `HUB_SCHEMA` in `app.yaml`.

## No Apps in the workspace?

Use `notebooks/09_review_quarantine`: the same decisions from notebook widgets, applied straight away.
