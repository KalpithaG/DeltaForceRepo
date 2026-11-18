"""Quarantine review app for the Delta Force DataOps Hub.

Lists every record the hub quarantined, with the rules it broke, and lets a reviewer:
  * approve it as received,
  * correct its values (the hub re-checks them against every rule), or
  * reject it.
Each decision is appended to <catalog>.<schema>.quarantine_decisions with the reviewer's identity.
The hub applies decisions on its next run: approved and successfully corrected records move
into the certified tables; rejected ones stay quarantined.
"""

import json
import os
import uuid

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

CATALOG = os.getenv("HUB_CATALOG", "team_9")
SCHEMA = os.getenv("HUB_SCHEMA", "default")
NS = f"`{CATALOG}`.`{SCHEMA}`"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
HUB_JOB_ID = os.getenv("HUB_JOB_ID", "").strip()

STATUS_LABELS = {
    "PENDING": "Pending review",
    "APPROVED": "Approved (released)",
    "CORRECTED": "Corrected (released)",
    "CORRECTION_FAILED": "Correction still fails",
    "REJECTED": "Rejected",
}

st.set_page_config(page_title="Quarantine review · DataOps Hub", layout="wide")
cfg = Config()


@st.cache_resource
def connection():
    return sql.connect(
        server_hostname=cfg.host.replace("https://", "").rstrip("/"),
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


def query(statement, params=None):
    with connection().cursor() as cur:
        cur.execute(statement, params or {})
        if cur.description is None:
            return pd.DataFrame()
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


@st.cache_data(ttl=30)
def load_queue():
    df = query(f"SELECT * FROM {NS}.vw_quarantine_queue ORDER BY dataset, file_date, record_key")
    if not df.empty:
        df["file_date"] = df["file_date"].astype(str)
    return df


def reviewer():
    headers = getattr(st.context, "headers", {}) or {}
    return (headers.get("X-Forwarded-Email") or headers.get("X-Forwarded-Preferred-Username")
            or headers.get("X-Forwarded-User") or "unknown")


def save_decision(row, decision, corrections, comment):
    query(
        f"INSERT INTO {NS}.quarantine_decisions "
        "(decision_id, dataset, row_id, record_key, decision, corrections, comment, reviewer, decided_at) "
        "VALUES (:decision_id, :dataset, :row_id, :record_key, :decision, :corrections, :comment, :reviewer, "
        "current_timestamp())",
        {
            "decision_id": str(uuid.uuid4()),
            "dataset": row["dataset"],
            "row_id": row["row_id"],
            "record_key": row["record_key"],
            "decision": decision,
            "corrections": json.dumps(corrections) if corrections else None,
            "comment": comment or None,
            "reviewer": reviewer(),
        },
    )
    load_queue.clear()


# ---------------------------------------------------------------------------------------
st.title("Quarantine review")
st.caption(
    f"Delta Force DataOps Hub · `{CATALOG}.{SCHEMA}` · signed in as {reviewer()}. "
    "Decisions take effect on the next hub run: approved and successfully corrected records move into "
    "the certified tables; corrected values are re-checked against every rule first."
)

if not WAREHOUSE_ID:
    st.error("No SQL warehouse is configured. Add a SQL warehouse resource with the key `sql-warehouse` to this app.")
    st.stop()

try:
    queue = load_queue()
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not read `{CATALOG}.{SCHEMA}.vw_quarantine_queue`. Has the hub run, and does this app's "
             f"service principal have SELECT on it?\n\n{exc}")
    st.stop()

if queue.empty:
    st.success("Nothing is quarantined. Every record passed its checks.")
    st.stop()

pending = int((queue["review_status"] == "PENDING").sum())
awaiting = int(queue["awaiting_hub_run"].fillna(False).astype(bool).sum())
released = int(queue["review_status"].isin(["APPROVED", "CORRECTED"]).sum())
c1, c2, c3, c4 = st.columns(4)
c1.metric("Quarantined records", len(queue))
c2.metric("Pending review", pending)
c3.metric("Released by review", released)
c4.metric("Decisions awaiting a hub run", awaiting)

with st.sidebar:
    st.header("Filter")
    datasets = ["All"] + sorted(queue["dataset"].unique().tolist())
    pick_ds = st.selectbox("Dataset", datasets)
    statuses = ["All"] + [s for s in STATUS_LABELS if s in set(queue["review_status"])]
    pick_status = st.selectbox("Review status", statuses, format_func=lambda s: STATUS_LABELS.get(s, s),
                               index=statuses.index("PENDING") if "PENDING" in statuses else 0)
    if HUB_JOB_ID:
        st.divider()
        st.header("Apply decisions")
        if st.button("Run the hub now", width="stretch"):
            from databricks.sdk import WorkspaceClient

            run = WorkspaceClient().jobs.run_now(job_id=int(HUB_JOB_ID))
            st.success(f"Hub job started (run {run.run_id}). Refresh this page when it finishes.")
    if st.button("Refresh", width="stretch"):
        load_queue.clear()
        st.rerun()

view = queue
if pick_ds != "All":
    view = view[view["dataset"] == pick_ds]
if pick_status != "All":
    view = view[view["review_status"] == pick_status]

table = view.assign(status=view["review_status"].map(STATUS_LABELS).fillna(view["review_status"]))[
    ["dataset", "file_date", "record_key", "reasons", "status", "latest_decision", "decision_by", "awaiting_hub_run"]
].rename(columns={
    "dataset": "Dataset", "file_date": "Day", "record_key": "Record", "reasons": "Why quarantined",
    "status": "Status", "latest_decision": "Latest decision", "decision_by": "Decided by",
    "awaiting_hub_run": "Awaiting hub run",
})
st.dataframe(table, width="stretch", hide_index=True)

if view.empty:
    st.info("No records match the filter.")
    st.stop()

# ---------------------------------------------------------------------------------------
st.subheader("Review a record")
options = view.index.tolist()
pick = st.selectbox(
    "Record",
    options,
    format_func=lambda i: f"{view.at[i, 'dataset']} · {view.at[i, 'record_key']} · {view.at[i, 'file_date']} · "
                          f"{view.at[i, 'reasons']}",
)
row = view.loc[pick]
record = json.loads(row["record_json"])

left, right = st.columns([3, 2])
with left:
    st.markdown("**As received**")
    st.dataframe(pd.DataFrame([{k: ("" if v is None else v) for k, v in record.items()}]),
                 width="stretch", hide_index=True)
    st.markdown(f"**Why quarantined:** {row['reasons']}")
    if row.get("recheck_failed"):
        st.warning(f"The last correction still failed: {row['recheck_failed']}")
    if row.get("latest_decision"):
        st.caption(f"Latest decision: {row['latest_decision']} by {row['decision_by']} at {row['decision_at']}"
                   + (" (not applied yet; it takes effect on the next hub run)" if row["awaiting_hub_run"] else ""))
    file_name = row["file_name"]
    st.caption(f"Source file: {file_name} · record id {row['row_id']}")

with right:
    action = st.radio("Decision", ["Approve as received", "Correct the values", "Reject"], horizontal=True)
    with st.form("decision", clear_on_submit=False):
        corrections = {}
        if action == "Correct the values":
            st.caption("Change only the fields that are wrong. Leave a field blank to clear it.")
            for field, value in record.items():
                new = st.text_input(field, value="" if value is None else str(value), key=f"{pick}-{field}")
                if new != ("" if value is None else str(value)):
                    corrections[field] = new
        comment = st.text_area("Comment", placeholder="Why? For example: confirmed with the data owner")
        submitted = st.form_submit_button("Save decision", type="primary", width="stretch")

    if submitted:
        if action == "Correct the values" and not corrections:
            st.error("Change at least one value, or choose Approve.")
        elif action != "Correct the values" and not comment.strip():
            st.error("Add a short comment so the decision can be audited.")
        else:
            decision = {"Approve as received": "APPROVE", "Correct the values": "MODIFY", "Reject": "REJECT"}[action]
            try:
                save_decision(row, decision, corrections, comment.strip())
                st.success(f"Saved: {decision} for {row['record_key']}. It takes effect on the next hub run.")
                st.rerun()
            except Exception as exc:  # noqa: BLE001
                st.error(f"Could not save the decision. Does this app's service principal have MODIFY on "
                         f"`{CATALOG}.{SCHEMA}.quarantine_decisions`?\n\n{exc}")

with st.expander("Decision history"):
    history = query(
        f"SELECT decided_at, dataset, record_key, decision, corrections, comment, reviewer "
        f"FROM {NS}.quarantine_decisions ORDER BY decided_at DESC LIMIT 100"
    )
    st.dataframe(history, width="stretch", hide_index=True)
