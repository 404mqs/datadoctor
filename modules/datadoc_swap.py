"""
datadoc_swap — Apply an approved v2 to the production notebook.

Swap flow:
  1. Verifies that the proposal exists and has status='approved' (or marks it as
     approved if it comes from the datadoc_approve notebook with manual confirmation).
  2. Exports the current production notebook to a backup path with timestamp.
  3. Exports the v2 and uploads it OVER the production path (overwrite=true).
  4. Inserts a row into datadoc.applied_changes.
  5. Updates datadoc.proposals with status='applied'.

Rollback:
  To revert, run `rollback_applied_change(applied_ts, proposal_id)`. It takes the
  stored backup_path and restores it over the production path. Updates the row
  with rollback_ts.
"""

import base64
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, Optional


# ============================================================
# HTTP helpers (intentionally duplicated to keep this module self-contained)
# ============================================================

def _http(host: str, token: str, method: str, path: str,
          body: Optional[dict] = None, params: Optional[dict] = None) -> dict:
    url = f"{host}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode("utf-8"))


def export_notebook_source(host: str, token: str, path: str) -> str:
    resp = _http(host, token, "GET", "/api/2.0/workspace/export",
                 params={"path": path, "format": "SOURCE"})
    return base64.b64decode(resp["content"]).decode("utf-8")


def import_notebook_source(host: str, token: str, path: str, source: str,
                           overwrite: bool = False) -> None:
    encoded = base64.b64encode(source.encode("utf-8")).decode("ascii")
    _http(host, token, "POST", "/api/2.0/workspace/import", body={
        "path": path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded,
        "overwrite": overwrite,
    })


def mkdirs(host: str, token: str, path: str) -> None:
    try:
        _http(host, token, "POST", "/api/2.0/workspace/mkdirs", body={"path": path})
    except Exception:
        pass  # already exists


# ============================================================
# Swap
# ============================================================

def apply_proposal(host: str, token: str, spark,
                   proposal_id: str, applied_by: str,
                   backup_dir: str = "/Workspace/Shared/DataDoctor/backups",
                   delta_schema: str = "datadoc",
                   notes: str = "") -> dict:
    """
    Applies a proposal: backup of the original + overwrite with the v2.
    Prior validation (if passed) must be done before calling this.

    Does not perform authorization checks — trusts that the caller (datadoc_approve notebook)
    has already done so.
    """
    # 1. Read the proposal
    proposals = spark.sql(f"""
        SELECT original_path, v2_path, status, task_key
        FROM {delta_schema}.proposals
        WHERE proposal_id = '{proposal_id}'
    """).collect()

    if not proposals:
        raise ValueError(f"proposal_id '{proposal_id}' does not exist")

    prop = proposals[0]

    if prop["status"] not in ("proposed", "approved"):
        raise ValueError(
            f"proposal {proposal_id} has status='{prop['status']}', cannot be applied. "
            "Only 'proposed' or 'approved' are valid."
        )

    original_path = prop["original_path"]
    v2_path       = prop["v2_path"]
    task_key      = prop["task_key"]

    # 2. Backup the original
    mkdirs(host, token, backup_dir)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{task_key}_{ts}"
    backup_path = f"{backup_dir}/{backup_name}"

    original_source = export_notebook_source(host, token, original_path)
    import_notebook_source(host, token, backup_path, original_source, overwrite=False)
    print(f"[swap] backup saved at {backup_path}")

    # 3. Read v2 and overwrite the production notebook
    v2_source = export_notebook_source(host, token, v2_path)
    import_notebook_source(host, token, original_path, v2_source, overwrite=True)
    print(f"[swap] {original_path} replaced with v2")

    # 4. Insert into applied_changes
    applied_ts = datetime.utcnow()
    spark.sql(f"""
        INSERT INTO {delta_schema}.applied_changes
        VALUES (
            timestamp '{applied_ts.isoformat()}',
            '{proposal_id}',
            '{original_path}',
            '{backup_path}',
            '{applied_by}',
            NULL,
            NULL,
            NULL
        )
    """)

    # 5. Update proposals
    notes_escaped = notes.replace("'", "''")
    spark.sql(f"""
        UPDATE {delta_schema}.proposals
        SET status = 'applied',
            status_updated_ts = timestamp '{applied_ts.isoformat()}',
            status_updated_by = '{applied_by}',
            notes = '{notes_escaped}'
        WHERE proposal_id = '{proposal_id}'
    """)

    return {
        "status": "applied",
        "proposal_id": proposal_id,
        "backup_path": backup_path,
        "applied_ts": applied_ts.isoformat(),
    }


def reject_proposal(spark, proposal_id: str, rejected_by: str,
                    delta_schema: str = "datadoc", reason: str = "") -> Dict:
    """Marks a proposal as rejected (nothing is applied to the production notebook)."""
    reason_escaped = reason.replace("'", "''")
    spark.sql(f"""
        UPDATE {delta_schema}.proposals
        SET status = 'rejected',
            status_updated_ts = current_timestamp(),
            status_updated_by = '{rejected_by}',
            notes = '{reason_escaped}'
        WHERE proposal_id = '{proposal_id}'
    """)
    return {"status": "rejected", "proposal_id": proposal_id}


def rollback_applied_change(host: str, token: str, spark,
                            proposal_id: str,
                            rolled_back_by: str = "unknown",
                            backup_dir: str = "/Workspace/Shared/DataDoctor/backups",
                            delta_schema: str = "datadoc") -> dict:
    """
    Reverts an applied change: takes the backup and restores it over the production path.
    """
    rows = spark.sql(f"""
        SELECT notebook_path, backup_path, rollback_ts
        FROM {delta_schema}.applied_changes
        WHERE proposal_id = '{proposal_id}'
        ORDER BY applied_ts DESC
        LIMIT 1
    """).collect()

    if not rows:
        raise ValueError(f"No applied_change found for proposal_id '{proposal_id}'")

    row = rows[0]
    if row["rollback_ts"] is not None:
        raise ValueError(f"proposal_id '{proposal_id}' has already been rolled back")

    notebook_path = row["notebook_path"]
    backup_path   = row["backup_path"]

    backup_source = export_notebook_source(host, token, backup_path)
    import_notebook_source(host, token, notebook_path, backup_source, overwrite=True)

    rollback_ts = datetime.utcnow().isoformat()
    spark.sql(f"""
        UPDATE {delta_schema}.applied_changes
        SET rollback_ts = timestamp '{rollback_ts}',
            rollback_by = '{rolled_back_by}'
        WHERE proposal_id = '{proposal_id}'
    """)

    spark.sql(f"""
        UPDATE {delta_schema}.proposals
        SET status = 'rejected',
            status_updated_ts = current_timestamp(),
            status_updated_by = '{rolled_back_by}',
            notes = concat(coalesce(notes, ''), '\n[ROLLBACK] ')
        WHERE proposal_id = '{proposal_id}'
    """)

    return {
        "status": "rolled_back",
        "proposal_id": proposal_id,
        "notebook_path": notebook_path,
        "rollback_ts": rollback_ts,
    }
