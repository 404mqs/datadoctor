# Data Doctor

**Automated notebook optimization for Databricks.**

Data Doctor runs at the end of your orchestrator job, analyzes the N slowest notebooks,
uses an LLM to generate diff-based optimization proposals, validates them automatically,
and notifies your team via Slack (or your preferred channel) with one-click approve/reject.

```
Orchestrator job finishes
        │
        ▼
  datadoc (notebook)
        │
        ├─ 1. Jobs API → top N slowest tasks
        ├─ 2. Export source + classify (green / yellow tier)
        ├─ 3. Estimate table sizes (prevents broadcast OOM)
        ├─ 4. LLM → JSON with per-cell diffs
        ├─ 5. Reconstruct v2 applying diffs to original
        ├─ 6. Save v2 to /proposals/
        ├─ 7. If tier=green → validate equivalence (row count + numeric sums)
        ├─ 8. Insert into datadoc.proposals
        └─ 9. Notify team (Slack with approve/reject buttons)

[when you decide]

  datadoc_approve (notebook)
        ├─ approve  → backup original + overwrite with v2
        ├─ reject   → mark proposal as rejected
        └─ rollback → restore backup if something breaks post-apply
```

## Requirements

- Databricks workspace with a running all-purpose cluster
- A Databricks serving endpoint for your LLM (recommended: `databricks-claude-opus-4-7`)
- A Slack app with `chat:write` scope and a bot token stored in a Databricks secret
  (or set `notifications.type: none` to skip Slack)

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/yourusername/datadoctor
cd datadoctor
pip install pyyaml
```

### 2. Fill in `datadoctor_config.yml`

Open `datadoctor_config.yml` and fill in:
- `databricks.host` — your workspace URL
- `databricks.job_id` — the job ID of your orchestrator
- `databricks.cluster_id` — an all-purpose cluster ID
- `llm.endpoint_name` — your LLM serving endpoint
- `notifications.slack.*` — your Slack channel IDs and secret info

### 3. Upload files to your workspace

```bash
python scripts/upload_all.py
```

### 4. Create the Delta tables

Run `schema/datadoc.sql` in a Databricks notebook (or `%sql` cell).

### 5. Add Data Doctor to your job

```bash
python scripts/add_to_job.py           # preview
python scripts/add_to_job.py --apply   # apply
```

That's it. Data Doctor will run automatically at the end of your next orchestrator run.

## Approval flow

When Data Doctor finds optimizations, it posts a Slack message with:
- A summary of proposed changes per notebook
- A **Review v2** button (opens the notebook in your workspace)
- An **Approve** button (runs `datadoc_approve` → applies v2 to production)
- A **Reject** button (marks the proposal as rejected)

To approve manually (without Slack buttons):
1. Open `datadoc_approve` in your workspace
2. Set `proposal_id` widget to the ID from the Slack message or `datadoc.proposals` table
3. Set `accion` to `approve`
4. Run

To rollback after applying:
1. Run `datadoc_approve` with the same `proposal_id` and `accion=rollback`

## Tiers

| Tier | What it means | What Data Doctor does |
|---|---|---|
| 🟢 Green | Writes Delta tables, no external side effects | Generates v2 + validates automatically + shows approve button |
| 🟡 Yellow | Has side effects (Sheets, Slack, APIs), self-reference, or no detected output tables | Generates v2 but skips automatic validation — requires manual review |

## Notifiers

**Built-in:**
- `slack` — posts Block Kit proposals to a channel with approve/reject buttons
- `none` — logs to stdout; proposals saved in `datadoc.proposals`

**Custom notifier:**
1. Create `modules/notifiers/your_notifier.py` implementing `BaseNotifier` from `base.py`
2. Set `notifications.type: your_notifier` in the config
3. Instantiate it in `notebooks/datadoc.py`

## Configuration reference

| Key | Default | Description |
|---|---|---|
| `databricks.host` | — | Workspace URL (required) |
| `databricks.job_id` | — | Orchestrator job ID (required) |
| `databricks.cluster_id` | — | All-purpose cluster ID (required) |
| `databricks.workspace_path` | `/Workspace/Shared/DataDoctor` | Where files are uploaded |
| `agent.top_n` | `3` | Notebooks to analyze per run |
| `agent.cooldown_days` | `5` | Days to skip a task after apply |
| `agent.odd_days_only` | `true` | Run every other day (saves LLM tokens) |
| `agent.delta_schema` | `datadoc` | Delta schema for proposals/changes |
| `agent.self_task_key` | `DATADOCTOR` | Task key to exclude from slow ranking |
| `llm.endpoint_name` | `databricks-claude-opus-4-7` | LLM serving endpoint |
| `llm.max_tokens` | `16000` | LLM response token limit |
| `notifications.type` | `slack` | `slack` or `none` |
| `notifications.slack.channel_proposals` | — | Channel for proposals |
| `notifications.slack.channel_audit` | — | Channel for approve/reject log |
| `notifications.slack.secret_scope` | `datadoctor` | Databricks secret scope |
| `notifications.slack.secret_key` | `slack_bot_token` | Key within the scope |

## Recommended LLM

Data Doctor works with any OpenAI-compatible Databricks serving endpoint.
We recommend **`databricks-claude-opus-4-7`** — it produces the most reliable
diff-based JSON output and handles complex PySpark notebooks well.

## Optional: 1-click webhook

See `webhook/README.md` for setting up an Azure Function that enables true 1-click
approval directly from Slack (no Databricks UI required).
