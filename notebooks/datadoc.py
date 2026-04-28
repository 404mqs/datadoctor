# Databricks notebook source
# MAGIC %md
# MAGIC # Data Doctor — Continuous Optimization Agent
# MAGIC
# MAGIC Runs at the end of your orchestrator job. For the N slowest notebooks it
# MAGIC generates an **optimization proposal (v2)**, validates it automatically
# MAGIC for safe notebooks, and notifies via Slack (or your configured notifier).
# MAGIC
# MAGIC **Flow:**
# MAGIC 1. Jobs API → top N slowest tasks in the run
# MAGIC 2. For each: export source + classify → tier (green/yellow)
# MAGIC 3. LLM (via Databricks serving endpoint) → v2 diffs + analysis
# MAGIC 4. Save v2 to `/Workspace/Shared/DataDoctor/proposals/...`
# MAGIC 5. If tier=green and validate=on → run validator (test tables vs prod)
# MAGIC 6. Insert row in `datadoc.proposals`
# MAGIC 7. Send report via configured notifier

# COMMAND ----------

# DBTITLE 1,Widgets
try:
    dbutils.widgets.text("run_id",            "", "Orchestrator Run ID (empty = latest)")
    dbutils.widgets.text("job_id",            "", "Orchestrator Job ID (empty = from config)")
    dbutils.widgets.dropdown("validate_mode", "auto", ["auto", "on", "off"], "Run v2 validation")
    dbutils.widgets.text("top_n",             "", "How many notebooks to analyze (empty = from config)")
    dbutils.widgets.text("cooldown_days",     "", "Cooldown days post-apply (empty = from config)")
    dbutils.widgets.text("validation_cluster","", "Cluster ID for validation (empty = from config)")
    dbutils.widgets.dropdown("force_run",     "false", ["false", "true"], "Force run (ignore odd-day check)")
except Exception:
    pass

# COMMAND ----------

# DBTITLE 1,Load config
import yaml

_CONFIG_PATH = "/Workspace/Shared/DataDoctor/datadoctor_config.yml"
with open(_CONFIG_PATH, "r", encoding="utf-8") as _f:
    CFG = yaml.safe_load(_f)

# Databricks connection
HOST_CFG          = CFG["databricks"]["host"].rstrip("/")
WORKSPACE_PATH    = CFG["databricks"]["workspace_path"]
BTN_CLUSTER_ID    = CFG["databricks"]["cluster_id"]
BTN_APPROVERS     = CFG.get("approvers", [])

# Agent settings — widgets override config when provided
_w_job_id     = dbutils.widgets.get("job_id").strip()
_w_top_n      = dbutils.widgets.get("top_n").strip()
_w_cooldown   = dbutils.widgets.get("cooldown_days").strip()

JOB_ID        = int(_w_job_id)      if _w_job_id    else int(CFG["databricks"]["job_id"])
TOP_N         = int(_w_top_n)       if _w_top_n     else int(CFG["agent"]["top_n"])
COOLDOWN_DAYS = int(_w_cooldown)    if _w_cooldown  else int(CFG["agent"]["cooldown_days"])
ODD_DAYS_ONLY = CFG["agent"].get("odd_days_only", True)
DELTA_SCHEMA  = CFG["agent"]["delta_schema"]
QA_TASK_KEY   = CFG["agent"]["self_task_key"]

# LLM
LLM_MODEL         = CFG["llm"]["endpoint_name"]
LLM_MAX_TOKENS    = int(CFG["llm"].get("max_tokens", 16000))

# Paths derived from workspace_path
PROPOSALS_DIR  = f"{WORKSPACE_PATH}/proposals"
BACKUPS_DIR    = f"{WORKSPACE_PATH}/backups"
SANDBOX_DIR    = f"{WORKSPACE_PATH}/validation_sandbox"
PROMPT_PATH    = f"{WORKSPACE_PATH}/prompt_optimization.txt"

# Widgets
RUN_ID_WIDGET      = dbutils.widgets.get("run_id").strip()
VALIDATE_MODE      = dbutils.widgets.get("validate_mode")
VALIDATION_CLUSTER = dbutils.widgets.get("validation_cluster").strip() or BTN_CLUSTER_ID
FORCE_RUN          = dbutils.widgets.get("force_run") == "true"

if VALIDATE_MODE == "auto":
    VALIDATE_MODE = "on"
print(f"Config loaded. LLM: {LLM_MODEL} | top_n: {TOP_N} | schema: {DELTA_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Odd-day check
from datetime import datetime, timezone, timedelta as _td
_LOCAL_TZ = timezone(_td(hours=int(CFG["agent"].get("timezone_offset_hours", -3))))
_today    = datetime.now(_LOCAL_TZ)
if _today.day % 2 == 0 and ODD_DAYS_ONLY and not FORCE_RUN:
    dbutils.notebook.exit(
        f"Even day ({_today.day}/{_today.month}) — skipping (token savings). "
        f"Next run: tomorrow. Set force_run=true to override."
    )
if FORCE_RUN and _today.day % 2 == 0:
    print(f"⚠️ Even day ({_today.day}/{_today.month}) — forced by force_run=true.")
else:
    print(f"✓ Odd day ({_today.day}/{_today.month}) — running.")

# COMMAND ----------

# DBTITLE 1,Imports + runtime constants
import base64
import json
import re
import sys
import time
import uuid
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional

from databricks.sdk import WorkspaceClient

sys.path.insert(0, WORKSPACE_PATH)
from datadoc_tier_classifier import classify_notebook
from datadoc_validator       import validate_proposal, upload_notebook
from datadoc_swap            import export_notebook_source
from datadoc_table_sizer     import estimate_sizes, format_size_context

# Import notifier based on config
_notif_type = CFG.get("notifications", {}).get("type", "none")
if _notif_type == "slack":
    from notifiers.slack import SlackNotifier
    notifier = SlackNotifier(CFG, dbutils)
else:
    from notifiers.base import NullNotifier
    notifier = NullNotifier()

w   = WorkspaceClient()
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST  = w.config.host
TOKEN = ctx.apiToken().get()

# WORKSPACE_URL: canonical browser URL (may differ from HOST in Azure).
# w.config.host can return the regional endpoint (e.g. eastus2.azuredatabricks.net)
# which works for REST but not for UI links. Build it from the workspace ID instead.
try:
    _WORKSPACE_ID = ctx.workspaceId().get()
    WORKSPACE_URL = f"https://adb-{_WORKSPACE_ID}.2.azuredatabricks.net"
except Exception:
    _WORKSPACE_ID = ""
    WORKSPACE_URL = HOST

MAX_LLM_SOURCE_CHARS = 160_000
EXECUTION_TS = datetime.utcnow()

# COMMAND ----------

# DBTITLE 1,HTTP helper (Databricks REST)
def http(method: str, path: str, body: Optional[dict] = None,
         params: Optional[dict] = None) -> dict:
    url = f"{HOST}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,Fetch top N slowest tasks from the run
def fetch_run_tasks(run_id: int) -> List[dict]:
    resp = http("GET", "/api/2.1/jobs/runs/get", params={"run_id": run_id})
    return resp.get("tasks", [])


def fetch_notebook_paths_from_job(job_id: int) -> Dict[str, str]:
    """Returns mapping task_key -> notebook_path taken from the job definition."""
    resp = http("GET", "/api/2.1/jobs/get", params={"job_id": job_id})
    tasks = resp.get("settings", {}).get("tasks", [])
    out = {}
    for t in tasks:
        nb = t.get("notebook_task") or {}
        if "notebook_path" in nb:
            out[t["task_key"]] = nb["notebook_path"]
    return out


def fetch_all_task_durations(run_id: int) -> Dict[str, float]:
    """Returns {task_key: duration_s} for all successful tasks in the run."""
    result = {}
    for t in fetch_run_tasks(run_id):
        if t.get("state", {}).get("result_state") != "SUCCESS":
            continue
        start, end = t.get("start_time"), t.get("end_time")
        if start and end:
            result[t["task_key"]] = (end - start) / 1000.0
    return result


def resolve_two_run_ids() -> List[int]:
    """Returns [current_run_id, previous_run_id] to average durations.

    - current_run_id:  the orchestrator run currently in progress (today, DataDoctor runs inside it)
    - previous_run_id: last COMPLETED orchestrator run (yesterday)
    If there is no active run (manual execution), returns only the last completed one.
    """
    run_ids = []

    resp_active = http("GET", "/api/2.1/jobs/runs/list",
                       params={"job_id": JOB_ID, "limit": 3, "active_only": "true"})
    active = resp_active.get("runs", [])
    if active:
        run_ids.append(active[0]["run_id"])
        print(f"Active run (today):      {active[0]['run_id']}")

    resp_done = http("GET", "/api/2.1/jobs/runs/list",
                     params={"job_id": JOB_ID, "limit": 1, "completed_only": "true"})
    done = resp_done.get("runs", [])
    if done:
        run_ids.append(done[0]["run_id"])
        print(f"Completed run (yesterday): {done[0]['run_id']}")

    if not run_ids:
        raise RuntimeError(f"No runs found for job_id={JOB_ID}")
    return run_ids


def fetch_avg_task_durations(run_ids: List[int]) -> Dict[str, float]:
    """Averages duration_s per task_key across the list of runs. Excludes QA_TASK_KEY."""
    from collections import defaultdict
    sums: Dict[str, List[float]] = defaultdict(list)
    for rid in run_ids:
        for tk, dur in fetch_all_task_durations(rid).items():
            if tk == QA_TASK_KEY:
                continue
            sums[tk].append(dur)
    return {tk: sum(v) / len(v) for tk, v in sums.items()}


def get_cooled_tasks(cooldown_days: int) -> Dict[str, datetime]:
    """Returns {task_key: applied_ts} for tasks applied within the last cooldown_days days."""
    if cooldown_days <= 0:
        return {}
    cutoff = datetime.utcnow() - __import__("datetime").timedelta(days=cooldown_days)
    rows = spark.sql(f"""
        SELECT p.task_key, MAX(ac.applied_ts) AS last_applied
        FROM {DELTA_SCHEMA}.applied_changes ac
        JOIN {DELTA_SCHEMA}.proposals p ON ac.proposal_id = p.proposal_id
        WHERE ac.applied_ts >= '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'
          AND ac.rollback_ts IS NULL
        GROUP BY p.task_key
    """).collect()
    return {r.task_key: r.last_applied for r in rows}


def apply_cooldown(ranked_all: List[dict], cooled: Dict[str, datetime], top_n: int) -> List[dict]:
    """Filters cooled-down tasks from the full ranked list and returns the first top_n valid ones."""
    result, skipped = [], []
    for t in ranked_all:
        if t["task_key"] in cooled:
            days_ago = (datetime.utcnow() - cooled[t["task_key"]].replace(tzinfo=None)).days
            skipped.append((t["task_key"], days_ago))
        else:
            result.append(t)
        if len(result) == top_n:
            break
    if skipped:
        for tk, days_ago in skipped:
            print(f"  [cooldown] {tk} — applied {days_ago}d ago, skipping until {COOLDOWN_DAYS}d")
    return result


if RUN_ID_WIDGET:
    run_ids = [int(RUN_ID_WIDGET)]
    print(f"Manual run: {run_ids[0]}")
else:
    run_ids = resolve_two_run_ids()

run_id        = run_ids[0]  # primary reference for notifications and proposal inserts
n_runs        = len(run_ids)
avg_durations = fetch_avg_task_durations(run_ids)

# Get full ranking (no limit) so we can replace cooled-down tasks
notebook_paths_all = fetch_notebook_paths_from_job(JOB_ID)
ranked_all = [
    {"task_key": tk, "duration_s": avg_s, "notebook_path": notebook_paths_all[tk]}
    for tk, avg_s in avg_durations.items()
    if tk in notebook_paths_all
]
ranked_all.sort(key=lambda x: x["duration_s"], reverse=True)

cooled_tasks = get_cooled_tasks(COOLDOWN_DAYS)
top_tasks    = apply_cooldown(ranked_all, cooled_tasks, TOP_N)
candidates   = apply_cooldown(ranked_all, cooled_tasks, len(ranked_all))  # full pool for fallback

label = f"avg. {n_runs} run{'s' if n_runs > 1 else ''}"
print(f"\nTop {TOP_N} slowest ({label}) — cooldown={COOLDOWN_DAYS}d ({len(candidates)} candidates total):")
for i, t in enumerate(top_tasks, 1):
    mins = t["duration_s"] / 60
    print(f"  {i}. {t['task_key']:<50} {mins:6.1f} min")

# COMMAND ----------

# DBTITLE 1,Load prompt template
with open(PROMPT_PATH, "r", encoding="utf-8") as f:
    prompt_template = f.read()

system_block, user_block = prompt_template.split("===USER===")
system_prompt_template = system_block.replace("===SYSTEM===", "").strip()
user_prompt_template   = user_block.strip()

# COMMAND ----------

# DBTITLE 1,LLM call helpers
def _normalize(s: str) -> str:
    """Normalizes whitespace and case for comparing cell IDs."""
    import re as _re
    return _re.sub(r"\s+", " ", s).strip().lower()


def _apply_changes(original: str, changes: List[dict]) -> str:
    """Reconstructs v2 by applying only the modified cells onto the original."""
    cells = original.split("# COMMAND ----------")
    result = list(cells)
    norm_cells = [_normalize(c) for c in cells]

    for change in changes:
        cell_id   = (change.get("celda_id") or "").strip()
        new_code  = (change.get("codigo_nuevo") or "").strip()
        if not cell_id or not new_code:
            continue

        norm_id = _normalize(cell_id)
        matched = False

        # Strategy 0: idx_N direct — LLM used the cell index
        import re as _re2
        _idx_m = _re2.match(r'^idx_(\d+)', cell_id.strip(), _re2.IGNORECASE)
        if _idx_m:
            idx = int(_idx_m.group(1))
            if 0 <= idx < len(result):
                result[idx] = "\n\n" + new_code + "\n\n"
                matched = True

        # Strategy 1: exact substring match (normalized)
        if not matched:
            for i, norm_cell in enumerate(norm_cells):
                if norm_id in norm_cell:
                    result[i] = "\n\n" + new_code + "\n\n"
                    matched = True
                    break

        # Strategy 2: at least 60% of words in cell_id appear in the cell
        if not matched:
            words = set(norm_id.split())
            if words:
                for i, norm_cell in enumerate(norm_cells):
                    overlap = sum(1 for w in words if w in norm_cell)
                    if overlap / len(words) >= 0.6:
                        result[i] = "\n\n" + new_code + "\n\n"
                        matched = True
                        break

        if not matched:
            print(f"  [change] cell not found — id: {cell_id[:80]!r}")

    return "# COMMAND ----------".join(result)


def _enumerate_cells(source: str) -> List[tuple]:
    """Returns [(idx, id_string)] for each cell in the notebook."""
    import re as _re
    cells = source.split("# COMMAND ----------")
    result = []
    for i, cell in enumerate(cells):
        m = _re.search(r"#\s*DBTITLE\s+\d+\s*,\s*(.+)", cell)
        if m:
            cell_id = f"# DBTITLE 1,{m.group(1).strip()}"
        else:
            first_code = next((l.strip() for l in cell.splitlines() if l.strip() and not l.strip().startswith("#")), "")
            cell_id = f"(unnamed — preview: {first_code[:50]})" if first_code else "(empty cell)"
        result.append((i, cell_id))
    return result


def _build_cell_index(cells_enum: List[tuple]) -> str:
    return "\n".join(f"  idx_{i}: {cid}" for i, cid in cells_enum)


def _smart_truncate(source: str, max_chars: int) -> str:
    """If source exceeds max_chars, keeps skeleton of early cells + full code of the last ones."""
    if len(source) <= max_chars:
        return source

    import re as _re
    cells = source.split("# COMMAND ----------")
    sep = "# COMMAND ----------"

    def _skeleton(cell: str) -> str:
        m = _re.search(r"#\s*DBTITLE\s+\d+\s*,\s*(.+)", cell)
        name = m.group(1).strip() if m else "(unnamed)"
        return f"\n# DBTITLE 1,{name}\n# [cell omitted by smart truncation]\n"

    skeletons  = [_skeleton(c) for c in cells]
    skel_total = sum(len(s) + len(sep) for s in skeletons)
    budget     = max_chars - skel_total

    # Fill with full code from the end backwards
    full_idx = set()
    for i in range(len(cells) - 1, -1, -1):
        if budget >= len(cells[i]):
            full_idx.add(i)
            budget -= len(cells[i])
        else:
            break

    parts = [cells[i] if i in full_idx else skeletons[i] for i in range(len(cells))]
    result = sep.join(parts)
    n_full = len(full_idx)
    result += (f"\n# [SMART TRUNCATION: {n_full}/{len(cells)} cells full "
               f"(last ones), {len(cells)-n_full} summarized. "
               f"Total original: {len(source):,} chars]")
    return result


def _parse_llm_response(raw: str) -> dict:
    """Extracts JSON from the LLM response, tolerating text before/after the JSON block."""
    text = raw.strip()

    # Strategy 1: direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Strategy 2: strip code fences at start/end
    stripped = re.sub(r"^```(?:json)?\s*", "", text)
    stripped = re.sub(r"\s*```\s*$", "", stripped).strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    # Strategy 3: find ```json ... ``` block anywhere in the text
    m = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # Strategy 4: first { ... last } (handles preamble + postamble text)
    start = text.find("{")
    end   = text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            pass

    print(f"[llm] full response (first 1000 chars):\n{text[:1000]}")
    raise json.JSONDecodeError("No valid JSON found in LLM response", text, 0)


def call_llm(notebook_path: str, duration_min: float, target_tables: List[str],
             source: str, size_context: str = "") -> Dict:
    """Calls the configured LLM endpoint and parses the JSON response."""
    cells_enum = _enumerate_cells(source)
    cell_index = _build_cell_index(cells_enum)
    source_llm = _smart_truncate(source, MAX_LLM_SOURCE_CHARS)
    if len(source_llm) < len(source):
        print(f"  [llm] smart truncation: {len(source):,} → {len(source_llm):,} chars")
    user_prompt = (
        user_prompt_template
        .replace("{notebook_path}",    notebook_path)
        .replace("{duracion_minutos}", f"{duration_min:.1f}")
        .replace("{target_tables}",    ", ".join(target_tables) or "(none detected)")
        .replace("{table_sizes}",      size_context or "(sizes not available)")
        .replace("{cell_index}",       cell_index)
        .replace("{notebook_source}",  source_llm)
    )

    t0 = time.time()
    # Direct urllib to control timeout (SDK does not expose it in serving_endpoints.query)
    llm_payload = json.dumps({
        "messages": [
            {"role": "system", "content": system_prompt_template},
            {"role": "user",   "content": user_prompt},
        ],
        "max_tokens": LLM_MAX_TOKENS,
    }).encode("utf-8")
    llm_req = urllib.request.Request(
        f"{HOST}/serving-endpoints/{LLM_MODEL}/invocations",
        data=llm_payload, method="POST",
    )
    llm_req.add_header("Authorization", f"Bearer {TOKEN}")
    llm_req.add_header("Content-Type", "application/json; charset=utf-8")
    with urllib.request.urlopen(llm_req, timeout=600) as r:  # 10 min
        llm_resp = json.loads(r.read().decode("utf-8"))
    elapsed = time.time() - t0

    raw = llm_resp["choices"][0]["message"]["content"].strip()
    parsed = _parse_llm_response(raw)

    # New format: "cambios" (per-cell diffs) rebuilds v2 on top of the original.
    # Fallback: if the LLM returns the old "codigo_optimizado" field, use it directly.
    cambios = parsed.get("cambios", [])
    if cambios:
        v2_source = _apply_changes(source, cambios)
    elif parsed.get("codigo_optimizado", "").strip():
        v2_source = parsed["codigo_optimizado"]
    else:
        v2_source = ""  # no changes

    return {
        "analisis":          parsed.get("analisis", {}),
        "codigo_optimizado": v2_source,
        "llm_duration_s":    elapsed,
    }

# COMMAND ----------

# DBTITLE 1,Ensure schema + directories
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DELTA_SCHEMA}")

try:
    http("POST", "/api/2.0/workspace/mkdirs", body={"path": PROPOSALS_DIR})
except Exception:
    pass

# COMMAND ----------

# DBTITLE 1,Track applied optimizations
def check_performance_gains(all_durations: Dict[str, float]) -> List[dict]:
    """
    Returns approvals that:
      - Were applied on a previous day (not today)
      - Have not been reported yet (first_reported_ts IS NULL)
    Shown only once, the day after approval.
    """
    try:
        rows = spark.sql(f"""
            SELECT ac.proposal_id, p.task_key, ac.applied_by, p.duration_original_s
            FROM {DELTA_SCHEMA}.applied_changes ac
            JOIN {DELTA_SCHEMA}.proposals p ON ac.proposal_id = p.proposal_id
            WHERE date(ac.applied_ts) < date(current_timestamp())
              AND ac.rollback_ts IS NULL
              AND ac.first_reported_ts IS NULL
            ORDER BY ac.applied_ts DESC
        """).collect()
    except Exception as e:
        print(f"[gains] error querying: {e}")
        return []

    seen, gains = set(), []
    for r in rows:
        task_key = r["task_key"]
        if task_key in seen:
            continue
        seen.add(task_key)
        original_s = r["duration_original_s"] or 0
        current_s  = all_durations.get(task_key)
        user       = (r["applied_by"] or "").split("@")[0]

        if current_s is None:
            gains.append({"proposal_id": r["proposal_id"], "task_key": task_key,
                          "original_min": original_s / 60, "current_min": None,
                          "delta_pct": None, "applied_by": user, "status": "no_ran"})
        else:
            delta_pct = (original_s - current_s) / original_s * 100 if original_s > 0 else 0
            status = "improved" if delta_pct > 3 else "regressed" if delta_pct < -3 else "neutral"
            gains.append({"proposal_id": r["proposal_id"], "task_key": task_key,
                          "original_min": original_s / 60, "current_min": current_s / 60,
                          "delta_pct": delta_pct, "applied_by": user, "status": status})
    return gains


performance_gains = check_performance_gains(avg_durations)
if performance_gains:
    print(f"Previously applied optimizations to report: {len(performance_gains)}")
    for g in performance_gains:
        if g["current_min"] is not None:
            print(f"  {g['task_key']}: {g['original_min']:.1f} → {g['current_min']:.1f} min ({g['delta_pct']:+.0f}%)")
        else:
            print(f"  {g['task_key']}: did not run in this run")

# COMMAND ----------

# DBTITLE 1,Ephemeral button jobs for Slack
BTN_PREFIX = "zzz_datadoc_btn_"

def cleanup_button_jobs() -> int:
    """Deletes all zzz_datadoc_btn_* jobs created by previous runs."""
    deleted = 0
    has_more = True
    page_token = None
    while has_more:
        params = {"limit": 100}
        if page_token:
            params["page_token"] = page_token
        resp = http("GET", "/api/2.1/jobs/list", params=params)
        for job in resp.get("jobs", []):
            if job.get("settings", {}).get("name", "").startswith(BTN_PREFIX):
                try:
                    http("POST", "/api/2.1/jobs/delete", {"job_id": job["job_id"]})
                    deleted += 1
                except Exception as e:
                    print(f"  [cleanup] could not delete job {job['job_id']}: {e}")
        has_more = resp.get("has_more", False)
        page_token = resp.get("next_page_token")
    return deleted


def create_button_jobs(proposal_id: str, task_key: str) -> tuple:
    """Creates two ephemeral jobs with pre-loaded parameters. Returns (approve_url, reject_url)."""
    cluster_id = VALIDATION_CLUSTER or BTN_CLUSTER_ID
    short_id   = proposal_id[:8]
    safe_key   = task_key[:25].replace(" ", "_")

    def _create(action: str) -> str:
        resp = http("POST", "/api/2.1/jobs/create", {
            "name": f"{BTN_PREFIX}{action}_{short_id}_{safe_key}",
            "existing_cluster_id": cluster_id,
            "notebook_task": {
                "notebook_path": f"{WORKSPACE_PATH}/datadoc_approve",
                "base_parameters": {"proposal_id": proposal_id, "accion": action},
            }
        })
        job_id = resp["job_id"]
        if BTN_APPROVERS:
            try:
                http("PUT", f"/api/2.0/permissions/jobs/{job_id}", {
                    "access_control_list": [
                        {"user_name": u, "permission_level": "CAN_MANAGE_RUN"}
                        for u in BTN_APPROVERS
                    ]
                })
            except Exception as perm_err:
                print(f"  [btn_jobs] could not set permissions on job {job_id}: {perm_err}")
        base = f"{WORKSPACE_URL}/jobs/{job_id}"
        return f"{base}?o={_WORKSPACE_ID}" if _WORKSPACE_ID else base

    try:
        approve_url = _create("approve")
        reject_url  = _create("reject")
        return approve_url, reject_url
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"  [btn_jobs] HTTP {e.code} for {task_key}: {body[:300]}")
        return None, None
    except Exception as e:
        print(f"  [btn_jobs] error for {task_key}: {e}")
        return None, None

# COMMAND ----------

# DBTITLE 1,Process each task in top N
deleted_btn = cleanup_button_jobs()
print(f"Previous button jobs deleted: {deleted_btn}")

proposals_resumen = []  # for the notification report
accepted = 0

for task in candidates:
    if accepted >= TOP_N:
        break
    task_key      = task["task_key"]
    duration_s    = task["duration_s"]
    notebook_path = task["notebook_path"]
    duration_min  = duration_s / 60.0

    print(f"\n=== Processing {task_key} ({duration_min:.1f} min) ===")

    proposal_id = str(uuid.uuid4())
    v2_path = f"{PROPOSALS_DIR}/{task_key}_v2_{EXECUTION_TS.strftime('%Y%m%d')}"

    # 1. Export source
    try:
        source = export_notebook_source(HOST, TOKEN, notebook_path)
    except Exception as e:
        print(f"[error] could not export {notebook_path}: {e}")
        proposals_resumen.append({
            "task_key": task_key, "duration_min": duration_min, "tier": "unknown",
            "proposal_id": None, "v2_path": None, "validation_status": "export_error",
            "analisis": {"error": str(e)},
        })
        continue

    # 2. Classify
    classification = classify_notebook(source)
    tier = classification["tier"]
    target_tables = classification["target_tables"]
    self_reference = classification.get("self_reference", False)
    print(f"  tier: {tier} | tables: {target_tables} | reason: {classification['reason']}")

    # 3. Estimate table sizes to inform the LLM
    sizes = estimate_sizes(spark, dbutils, source)
    size_context = format_size_context(sizes)
    print(f"  sizes: {len(sizes)} tables/parquets estimated")

    # 4. LLM -> v2
    try:
        llm_result = call_llm(notebook_path, duration_min, target_tables, source, size_context)
    except Exception as e:
        print(f"  [llm] error: {e}")
        try:
            spark.sql(f"""
                INSERT INTO {DELTA_SCHEMA}.proposals
                SELECT
                  :pid, :ts, :rid, :tk, :op, NULL, :dur,
                  :tier, :reason, :tables, :sfx,
                  :model, :llm, 0.0,
                  'llm_error', NULL, NULL,
                  'proposed', :ts, 'datadoc', NULL
            """, {
                "pid": proposal_id, "ts": EXECUTION_TS, "rid": run_id, "tk": task_key,
                "op": notebook_path, "dur": duration_s,
                "tier": tier, "reason": classification["reason"],
                "tables": target_tables, "sfx": classification["side_effects"],
                "model": LLM_MODEL,
                "llm": json.dumps({"error": str(e)}, ensure_ascii=False),
            })
        except Exception as db_err:
            print(f"  [db] could not record llm_error: {db_err}")
        proposals_resumen.append({
            "task_key": task_key, "duration_min": duration_min, "tier": tier,
            "proposal_id": proposal_id, "v2_path": None, "validation_status": "llm_error",
            "analisis": {"error": str(e)},
        })
        continue

    v2_source = llm_result["codigo_optimizado"]
    if not v2_source.strip() or v2_source.strip() == source.strip():
        print(f"  [llm] no changes proposed")
        proposals_resumen.append({
            "task_key": task_key, "duration_min": duration_min, "tier": tier,
            "proposal_id": None, "v2_path": None, "validation_status": "no_changes",
            "analisis": llm_result["analisis"],
        })
        continue

    # 5. Save v2 to sandbox
    try:
        upload_notebook(HOST, TOKEN, v2_path, v2_source)
        print(f"  v2 saved to {v2_path}")
    except Exception as e:
        print(f"  [v2] error saving v2: {e}")
        continue

    # 6. Validate if applicable
    validation = {"validation_status": "skipped", "details": None, "run_id": None}
    if tier == "green" and VALIDATE_MODE == "on":
        cluster_id = VALIDATION_CLUSTER
        try:
            val = validate_proposal(
                host=HOST, token=TOKEN, spark=spark,
                v2_source=v2_source,
                target_tables=target_tables,
                cluster_id=cluster_id,
                sandbox_dir=SANDBOX_DIR,
            )
            validation = {
                "validation_status": val.get("validation_status"),
                "details":           val,
                "run_id":            val.get("run_id"),
            }
            print(f"  validation: {validation['validation_status']}")
        except Exception as e:
            validation = {"validation_status": "failed",
                          "details": {"error": str(e)}, "run_id": None}
            print(f"  [validator] error: {e}")
    elif tier == "yellow":
        validation["details"] = {"reason": classification["reason"]}

    # If validation found output differences, discard this proposal and try the next notebook
    if validation["validation_status"] == "diffs_detected":
        print(f"  [validation] diffs detected — proposal discarded, trying next task")
        continue

    # 7. Insert proposal
    spark.sql(f"""
        INSERT INTO {DELTA_SCHEMA}.proposals
        SELECT
          :pid, :ts, :rid, :tk, :op, :v2p, :dur,
          :tier, :reason, :tables, :sfx,
          :model, :llm, :llm_dur,
          :vstatus, :vdetails, :vrid,
          'proposed', :ts, 'datadoc', NULL
    """, {
        "pid": proposal_id, "ts": EXECUTION_TS, "rid": run_id, "tk": task_key,
        "op": notebook_path, "v2p": v2_path, "dur": duration_s,
        "tier": tier, "reason": classification["reason"],
        "tables": target_tables, "sfx": classification["side_effects"],
        "model": LLM_MODEL, "llm": json.dumps(llm_result["analisis"], ensure_ascii=False),
        "llm_dur": llm_result["llm_duration_s"],
        "vstatus": validation["validation_status"],
        "vdetails": json.dumps(validation["details"], ensure_ascii=False, default=str)
                    if validation["details"] else None,
        "vrid": validation["run_id"],
    })

    # 8. Create ephemeral jobs for Slack buttons
    approve_url, reject_url = create_button_jobs(proposal_id, task_key)
    review_url = f"{WORKSPACE_URL}/#workspace{v2_path}" if v2_path else None

    proposals_resumen.append({
        "task_key": task_key, "duration_min": duration_min, "tier": tier,
        "proposal_id": proposal_id, "v2_path": v2_path,
        "validation_status": validation["validation_status"],
        "analisis": llm_result["analisis"],
        "self_reference": self_reference,
        "approve_url": approve_url,
        "reject_url":  reject_url,
        "review_url":  review_url,
    })
    accepted += 1

# COMMAND ----------

# DBTITLE 1,Send notification
result = notifier.send_proposal(
    proposals=proposals_resumen,
    run_ids=run_ids,
    top_n=TOP_N,
    performance_gains=performance_gains,
)
print(f"Notifier result: {result.get('ok')} {result.get('error', '')}")

if performance_gains and result.get("ok"):
    reported_ids = [g["proposal_id"] for g in performance_gains]
    ids_sql = ", ".join(f"'{pid}'" for pid in reported_ids)
    try:
        spark.sql(f"""
            UPDATE {DELTA_SCHEMA}.applied_changes
            SET first_reported_ts = current_timestamp()
            WHERE proposal_id IN ({ids_sql})
              AND first_reported_ts IS NULL
        """)
        print(f"[gains] {len(reported_ids)} record(s) marked as reported")
    except Exception as e:
        print(f"[gains] could not mark as reported: {e}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "run_analyzed":  run_id,
    "proposals":     len(proposals_resumen),
    "notifier_ok":   result.get("ok", False),
    "notifier_error": result.get("error", ""),
}))
