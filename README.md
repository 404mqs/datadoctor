<p align="center">
  <img src="docs/logo.png" width="120" alt="Data Doctor logo" />
</p>

<h1 align="center">Data Doctor</h1>

<p align="center">
  <strong>Automated notebook optimization for Databricks.</strong><br/>
  Finds your slowest notebooks, generates optimized versions with an LLM, validates them, and asks for your approval — all on autopilot.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Claude%20Opus-7C3AED?style=flat" />
  <img src="https://img.shields.io/badge/Slack-4A154B?style=flat&logo=slack&logoColor=white" />
  <img src="https://img.shields.io/badge/license-MIT-blue?style=flat" />
</p>

---

## How it works

```
Orchestrator job finishes
        │
        ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  datadoc  (runs as the last task in your job)                 │
  │                                                               │
  │  1. Jobs API  ──▶  average of N runs  ──▶  top K slowest     │
  │  2. Export source + compute SHA-256 hash                      │
  │  3. Classify  ──▶  🟢 green / 🟡 yellow  (tier + side-effects)│
  │  4. Estimate table sizes  ──▶  broadcast safety map           │
  │  5. Fetch last 5 rejected proposals for this notebook         │
  │  6. LLM call  ──▶  JSON with per-cell optimization diffs      │
  │     └─ rejected history injected as negative context          │
  │  7. Reconstruct v2 by applying diffs to original source       │
  │  8. Save v2 to  {workspace_path}/proposals/                   │
  │  9. If 🟢 green  ──▶  validate output equivalence (T1/T2/T3) │
  │ 10. Insert into  datadoc.proposals  (with source hash)        │
  │ 11. Notify team  ──▶  Slack with approve/reject buttons       │
  └──────────────────────────────────────────────────────────────┘
        │
        ▼  (when you decide)
  ┌──────────────────────────────────────────────────────────────┐
  │  datadoc_approve                                              │
  │  ✅ approve  ──▶  verify source hash  ──▶  backup + apply v2  │
  │                    (if hash mismatch → reject as 'stale')     │
  │  ❌ reject   ──▶  mark as rejected (feeds future LLM context) │
  │  ↩️  rollback ──▶  restore from backup                        │
  └──────────────────────────────────────────────────────────────┘
```

## Requirements

- Databricks workspace with an all-purpose cluster
- A Databricks serving endpoint for your LLM — **recommended: `databricks-claude-opus-4-7`**
- A Slack app with `chat:write` scope + bot token in a Databricks secret
  *(or set `notifications.type: none` to skip Slack)*

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/404mqs/datadoctor
cd datadoctor
pip install pyyaml
```

### 2. Fill in `datadoctor_config.yml`

Open `datadoctor_config.yml` and set:

```yaml
databricks:
  host: "https://<your-workspace>.azuredatabricks.net"
  job_id: 123456789        # your orchestrator job ID
  cluster_id: "xxxx-yyy"   # all-purpose cluster

llm:
  endpoint_name: "databricks-claude-opus-4-7"

notifications:
  type: "slack"
  slack:
    channel_proposals: "C..."
    channel_audit: "C..."
    secret_scope: "datadoctor"
    secret_key: "slack_bot_token"
```

### 3. Upload to your workspace

```bash
python scripts/upload_all.py
```

### 4. Create the Delta tables

Run `schema/datadoc.sql` in a Databricks notebook or `%sql` cell.

### 5. Add Data Doctor to your job

```bash
python scripts/add_to_job.py          # preview
python scripts/add_to_job.py --apply  # apply
```

That's it. Data Doctor runs automatically at the end of your next orchestrator job.

---

## Approval flow

When Data Doctor finds optimizations it posts to Slack:

| Button | Action |
|---|---|
| 🔍 **Review v2** | Opens the proposed notebook in your workspace |
| ✅ **Approve** | Verifies the notebook wasn't edited since proposal; backs up + overwrites with v2 |
| ❌ **Reject** | Marks as rejected; description is fed back to LLM on the next proposal for this notebook |

**Stale protection**: if the notebook was manually edited after the proposal was generated, the approve action is blocked and the proposal is marked `stale`. Data Doctor will generate a fresh proposal the next day incorporating the manual changes.

To approve **without Slack**, run `datadoc_approve` manually and set the `proposal_id` widget.
To **rollback** after approving, run `datadoc_approve` with `action=rollback`.

---

## Continuous improvement & cooldown

Data Doctor is designed for **continuous, incremental optimization** — not a one-shot pass. Each run it targets the slowest notebooks that haven't been recently touched, applies one improvement, and moves on to the next bottleneck on the following run.

To support this, every task that receives an approved optimization enters a **cooldown period** (default: 5 days). During cooldown, that task is skipped from the slow-task ranking so Data Doctor can focus on other notebooks instead of repeatedly re-analyzing the same one.

This also prevents a subtle feedback loop: a freshly optimized notebook may run slower for the first few executions (Spark JIT warm-up, Delta cache cold start) and would otherwise keep appearing as a candidate — generating redundant proposals before the improvement has had time to prove itself.

To disable cooldown entirely, set `agent.cooldown_days: 0` in the config.

---

## Tiers

| Tier | Meaning | What Data Doctor does |
|---|---|---|
| 🟢 **Green** | Writes Delta tables, no external side effects | Generates v2 + auto-validates + shows approve button |
| 🟡 **Yellow** | Has side effects (Sheets, Slack, APIs) or self-referencing tables | Generates v2 but skips auto-validation — requires manual review |

---

## Notifiers

**Built-in:**
- `slack` — Block Kit message with approve/reject buttons
- `none` — logs to stdout; proposals saved in `datadoc.proposals`

**Custom notifier** (Teams, email, etc.):
1. Create `modules/notifiers/your_notifier.py` extending `BaseNotifier`
2. Implement `send_proposal()` and `send_result()`
3. Set `notifications.type: your_notifier` in the config

---

## Configuration reference

| Key | Default | Description |
|---|---|---|
| `databricks.host` | — | Workspace URL *(required)* |
| `databricks.job_id` | — | Orchestrator job ID *(required)* |
| `databricks.cluster_id` | — | All-purpose cluster ID *(required)* |
| `databricks.workspace_path` | `/Workspace/Shared/DataDoctor` | Where files are uploaded |
| `agent.top_n` | `3` | Notebooks to analyze per run |
| `agent.cooldown_days` | `5` | Days to skip a task after applying an optimization |
| `agent.odd_days_only` | `true` | Run every other day to save LLM tokens |
| `agent.delta_schema` | `datadoc` | Delta schema for proposals and applied changes |
| `agent.self_task_key` | `DATADOCTOR` | Task key to exclude from slow-task ranking |
| `agent.runs_to_average` | `2` | Past runs to average when ranking slow notebooks — higher values smooth out outliers |
| `llm.endpoint_name` | `databricks-claude-opus-4-7` | LLM serving endpoint |
| `llm.max_tokens` | `16000` | Max tokens for LLM response |
| `notifications.type` | `slack` | `slack` or `none` |
| `notifications.slack.channel_proposals` | — | Channel for optimization proposals |
| `notifications.slack.channel_audit` | — | Channel for approve/reject confirmations |
| `notifications.slack.secret_scope` | `datadoctor` | Databricks secret scope name |
| `notifications.slack.secret_key` | `slack_bot_token` | Key within the scope |

---

## Multiple orchestrators

If your team runs several orchestrator jobs, deploy a separate Data Doctor instance per job.
Each instance needs its own config file (`datadoctor_config.yml`) pointing to the correct `job_id`, and a distinct `agent.delta_schema` (e.g. `datadoc_pipelines`, `datadoc_ml`).

The proposals and applied-changes tables are scoped to that schema, so histories and rejection context stay isolated per orchestrator.

```yaml
# config for the ML orchestrator
databricks:
  job_id: 987654321
agent:
  delta_schema: "datadoc_ml"
  self_task_key: "DATADOCTOR_ML"
```

Run `scripts/upload_all.py` once per instance (it reads the path from your config).

---

## Recommended LLM

Data Doctor works with any OpenAI-compatible Databricks serving endpoint.
We recommend **`databricks-claude-opus-4-7`** — it produces the most reliable diff-based JSON and handles complex PySpark notebooks well.

## Optional: 1-click webhook

See [`webhook/README.md`](webhook/README.md) for setting up an Azure Function that enables true 1-click
approval from Slack — no Databricks UI required.
