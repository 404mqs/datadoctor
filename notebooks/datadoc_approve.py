# Databricks notebook source
# MAGIC %md
# MAGIC # Data Doctor Approve — Approve / reject / rollback a proposal
# MAGIC
# MAGIC Usage:
# MAGIC 1. Paste the `proposal_id` into the widget.
# MAGIC 2. Choose the action: `approve` (applies v2), `reject` (discards), `rollback` (reverts
# MAGIC    an already-applied change).
# MAGIC 3. Run the notebook.
# MAGIC
# MAGIC The backup of the original notebook is saved to `{workspace_path}/backups/{task_key}_{ts}`.
# MAGIC History is tracked in `datadoc.applied_changes`.

# COMMAND ----------

# DBTITLE 1,Widgets
try:
    dbutils.widgets.text("proposal_id", "", "Proposal ID to act on")
    dbutils.widgets.dropdown("accion", "approve", ["approve", "reject", "rollback"], "Action")
    dbutils.widgets.text("notes", "", "Notes (reason for rejection or confirmation)")
except Exception:
    pass

PROPOSAL_ID = dbutils.widgets.get("proposal_id").strip()
ACTION      = dbutils.widgets.get("accion")
NOTES       = dbutils.widgets.get("notes").strip()

if not PROPOSAL_ID:
    dbutils.notebook.exit("ERROR: proposal_id is empty. Paste it into the widget and re-run.")

# COMMAND ----------

# DBTITLE 1,Load config
import sys, yaml

_CONFIG_PATH = "/Workspace/Shared/DataDoctor/datadoctor_config.yml"
with open(_CONFIG_PATH, "r", encoding="utf-8") as _f:
    CFG = yaml.safe_load(_f)

WORKSPACE_PATH = CFG["databricks"]["workspace_path"]
DELTA_SCHEMA   = CFG["agent"]["delta_schema"]
BACKUP_DIR     = f"{WORKSPACE_PATH}/backups"

sys.path.insert(0, WORKSPACE_PATH)

# Databricks connection
from databricks.sdk import WorkspaceClient
w     = WorkspaceClient()
ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST  = w.config.host
TOKEN = ctx.apiToken().get()

try:
    USER = ctx.userName().get()
except Exception:
    USER = "unknown"

print(f"User: {USER}")

# COMMAND ----------

# DBTITLE 1,Imports
from datadoc_swap import apply_proposal, reject_proposal, rollback_applied_change

# COMMAND ----------

# DBTITLE 1,Show proposal details
detalle = spark.sql(f"""
    SELECT
        proposal_id, created_ts, task_key, original_path, v2_path,
        duration_original_s, tier, target_tables, side_effects,
        validation_status, llm_analysis, status, status_updated_ts
    FROM {DELTA_SCHEMA}.proposals
    WHERE proposal_id = '{PROPOSAL_ID}'
""")

rows = detalle.collect()
if not rows:
    dbutils.notebook.exit(f"ERROR: proposal_id '{PROPOSAL_ID}' not found in {DELTA_SCHEMA}.proposals")

r = rows[0]
task_key = r["task_key"]

print("=" * 60)
print(f"Task:              {task_key}")
print(f"Original:          {r['original_path']}")
print(f"v2:                {r['v2_path']}")
print(f"Duration:          {r['duration_original_s']/60:.1f} min")
print(f"Tier:              {r['tier']}")
print(f"Output tables:     {r['target_tables']}")
print(f"Side effects:      {r['side_effects']}")
print(f"Validation:        {r['validation_status']}")
print(f"Current status:    {r['status']}")
print(f"Created:           {r['created_ts']}")
print("=" * 60)
print("\nLLM Analysis:")
print(r["llm_analysis"])

# COMMAND ----------

# DBTITLE 1,Execute action
if ACTION == "approve":
    if r["status"] != "proposed":
        dbutils.notebook.exit(f"ERROR: current status is '{r['status']}', cannot approve.")

    if r["validation_status"] not in ("passed", "skipped") and r["tier"] == "green":
        print(f"⚠️ WARNING: validation is '{r['validation_status']}' but tier=green. "
              "Make sure you have reviewed v2 manually.")

    print(f"\n🔧 Applying proposal {PROPOSAL_ID} ...")
    result = apply_proposal(HOST, TOKEN, spark, PROPOSAL_ID, USER,
                            backup_dir=BACKUP_DIR, notes=NOTES)
    print(f"✅ Applied")
    print(f"   Backup at: {result['backup_path']}")
    print(f"   To revert: run this notebook with action='rollback' and the same proposal_id.")

elif ACTION == "reject":
    if r["status"] != "proposed":
        dbutils.notebook.exit(f"ERROR: current status is '{r['status']}', cannot reject.")

    reject_proposal(spark=spark, proposal_id=PROPOSAL_ID,
                    rejected_by=USER, reason=NOTES)
    print(f"✅ Proposal {PROPOSAL_ID} rejected.")

elif ACTION == "rollback":
    if r["status"] != "applied":
        dbutils.notebook.exit(
            f"ERROR: can only rollback a proposal with status='applied'. "
            f"Current status: '{r['status']}'"
        )

    print(f"\n↩️ Rolling back proposal {PROPOSAL_ID} ...")
    result = rollback_applied_change(HOST, TOKEN, spark, PROPOSAL_ID,
                                     rolled_back_by=USER,
                                     backup_dir=BACKUP_DIR)
    print(f"✅ Rollback complete. Notebook {result['notebook_path']} restored from backup.")

else:
    dbutils.notebook.exit(f"ERROR: invalid action '{ACTION}'")

# COMMAND ----------

# DBTITLE 1,Notify audit channel
# Notify audit channel
_notif_type = CFG.get("notifications", {}).get("type", "none")
if _notif_type == "slack":
    try:
        from notifiers.slack import SlackNotifier
        _notifier = SlackNotifier(CFG, dbutils)
        _notifier.send_result(action=ACTION, task_key=task_key, proposal_id=PROPOSAL_ID, notes=NOTES)
    except Exception as _e:
        print(f"[slack] could not notify: {_e}")

# COMMAND ----------

dbutils.notebook.exit(f"OK: action={ACTION} proposal_id={PROPOSAL_ID}")
