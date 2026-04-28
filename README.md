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
  ┌─────────────────────────────────────────────────────┐
  │  datadoc  (runs as the last task in your job)        │
  │                                                      │
  │  1. Jobs API  ──▶  top N slowest notebooks           │
  │  2. Export source + classify  ──▶  🟢 green / 🟡 yellow │
  │  3. Estimate table sizes  ──▶  broadcast safety check │
  │  4. LLM  ──▶  JSON with per-cell optimization diffs  │
  │  5. Reconstruct v2 applying diffs to original        │
  │  6. Save v2 to  /proposals/                          │
  │  7. If 🟢 green  ──▶  validate equivalence           │
  │  8. Insert into  datadoc.proposals                   │
  │  9. Notify team  ──▶  Slack with approve/reject      │
  └─────────────────────────────────────────────────────┘
        │
        ▼  (when you decide)
  ┌──────────────────────┐
  │  datadoc_approve      │
  │  ✅ approve           │  ──▶  backup original + apply v2
  │  ❌ reject            │  ──▶  mark as rejected
  │  ↩️  rollback         │  ──▶  restore from backup
  └──────────────────────┘
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
| ✅ **Approve** | Backs up original + overwrites with v2 |
| ❌ **Reject** | Marks the proposal as rejected |

To approve **without Slack**, run `datadoc_approve` manually and set the `proposal_id` widget.
To **rollback** after approving, run `datadoc_approve` with `accion=rollback`.

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
| `llm.endpoint_name` | `databricks-claude-opus-4-7` | LLM serving endpoint |
| `llm.max_tokens` | `16000` | Max tokens for LLM response |
| `notifications.type` | `slack` | `slack` or `none` |
| `notifications.slack.channel_proposals` | — | Channel for optimization proposals |
| `notifications.slack.channel_audit` | — | Channel for approve/reject confirmations |
| `notifications.slack.secret_scope` | `datadoctor` | Databricks secret scope name |
| `notifications.slack.secret_key` | `slack_bot_token` | Key within the scope |

---

## Recommended LLM

Data Doctor works with any OpenAI-compatible Databricks serving endpoint.
We recommend **`databricks-claude-opus-4-7`** — it produces the most reliable diff-based JSON and handles complex PySpark notebooks well.

## Optional: 1-click webhook

See [`webhook/README.md`](webhook/README.md) for setting up an Azure Function that enables true 1-click
approval from Slack — no Databricks UI required.
