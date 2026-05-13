# P11 — ROI-Based Ranking Design

**Date:** 2026-05-13  
**Status:** Approved  
**Scope:** `datadoctor-oss` only (Brubank Orquestador uses a single homogeneous cluster type — no ranking impact)

---

## Problem

Data Doctor ranks tasks by average duration (minutes). This ignores cluster cost: a 10-minute task on a 16-worker memory-optimized cluster is significantly more expensive than a 20-minute task on a single-worker baseline node, yet the latter gets analyzed first.

---

## Design

### Cost Weight Formula

```
cost_weight = node_weight × (num_workers + 1) × photon_multiplier
```

- `node_weight` — relative cost per node, from `cluster_cost_weights` config. Default: `1.0` for unknown types.
- `(num_workers + 1)` — driver counts as one node. If `num_workers` is `null` (autoscale or single-node), treated as `1` → total `2`.
- `photon_multiplier` — applied only when the cluster has `runtime_engine: PHOTON`. Default: `1.5`.

### Ranking Score

```
score = duration_s × cost_weight
```

Tasks are sorted by `score` descending. The top N by score are analyzed instead of the top N by duration.

### Slack Display

The `cost_weight` is shown next to duration **only when it differs from `1.0`**. If all tasks in the job share the same cluster config, nothing extra is shown (no noise).

```
# When cost_weight = 3.0:
*32.7 min  ·  3.0×*

# When cost_weight = 1.0:
*32.7 min*
```

Header context line changes from `"Top N slowest notebooks"` to `"Top N by cost score"` when any task has `cost_weight ≠ 1.0`.

---

## Files Changed

### `datadoctor_config.yml`
New top-level section after `approvers`:

```yaml
cluster_cost_weights:
  Standard_DS3_v2:  1.0   # 4 cores, 14 GB — baseline
  Standard_DS4_v2:  2.0   # 8 cores, 28 GB
  Standard_DS5_v2:  4.0   # 16 cores, 56 GB
  Standard_E4d_v4:  1.5   # 4 cores, 32 GB (memory-optimized)
  Standard_E8d_v4:  3.0   # 8 cores, 64 GB
  Standard_E16d_v4: 6.0   # 16 cores, 128 GB
  Standard_E32d_v4: 12.0  # 32 cores, 256 GB
  # Any node_type_id not listed defaults to 1.0

photon_cost_multiplier: 1.5
```

Both keys are optional — if absent, cost weighting is effectively disabled (all weights = 1.0, ranking identical to current behavior).

### `notebooks/datadoc.py`

New function `fetch_task_cost_weights() -> Dict[str, float]` added in the **"Fetch top N slowest tasks"** cell block, called once after `avg_durations` is computed.

Logic:
1. Call `/api/2.1/jobs/get` for `JOB_ID` (same endpoint as `fetch_notebook_paths_from_job` — independent call, lightweight GET).
2. Build a `job_cluster_key → {node_type, num_workers, photon}` dict from `settings.job_clusters`.
3. For tasks with `existing_cluster_id`, query `/api/2.0/clusters/get` with an in-process cache (one API call per unique cluster ID).
4. Apply formula, default `1.0` for unknowns.

Ranking change — replace the two lines that build and sort `ranked_all`:

```python
# Before
ranked_all.sort(key=lambda x: x["duration_s"], reverse=True)

# After
cost_weights = fetch_task_cost_weights()
for t in ranked_all:
    t["cost_weight"] = cost_weights.get(t["task_key"], 1.0)
    t["score"]       = t["duration_s"] * t["cost_weight"]
ranked_all.sort(key=lambda x: x["score"], reverse=True)
```

Print block updated to show score when weights are non-uniform:

```
Top 3 by cost score (avg. 2 runs) — cooldown=5d:
  1. dl_diario_genera_tablero      32.7 min  ×3.0  →  98.1 score
  2. MQS_MAIN_CTA_MAX              20.1 min  ×1.0  →  20.1 score
  3. ultra_report_daily_gsheets    18.4 min  ×1.0  →  18.4 score
```

`cost_weight` is added to each item in `proposals_resumen` so the notifier can display it.

### `modules/notifiers/slack.py`

In `_build_proposal_payload()`, the proposal title block (line ~205):

```python
# Before
f"*{i}. `{p['task_key']}`*  {tier_emoji} {val_emoji}\n*{p['duration_min']:.1f} min*"

# After
cw = p.get("cost_weight", 1.0)
cost_str = f"  ·  *{cw:.1f}×*" if cw != 1.0 else ""
f"*{i}. `{p['task_key']}`*  {tier_emoji} {val_emoji}\n*{p['duration_min']:.1f} min*{cost_str}"
```

Header context line (line ~159):
```python
# Before
f"{run_label}  ·  Top {top_n} slowest notebooks"

# After — only changes label when cost weighting is active
_any_weighted = any(p.get("cost_weight", 1.0) != 1.0 for p in proposals)
rank_label = "by cost score" if _any_weighted else "slowest"
f"{run_label}  ·  Top {top_n} {rank_label} notebooks"
```

---

## What Doesn't Change

- `datadoc_approve.py` — no changes
- `datadoc_swap.py` — no changes
- `datadoc_validator.py` — no changes
- `datadoc.proposals` schema — no changes
- Cooldown logic — still operates on `task_key`, unaffected by score
- `check_performance_gains()` — unaffected (queries applied_changes by task_key)

---

## Edge Cases

| Case | Behavior |
|---|---|
| `cluster_cost_weights` absent from config | All weights default to `1.0` — ranking identical to current |
| Task has no cluster assignment (`task_key` not in job definition) | `cost_weight = 1.0` |
| `num_workers = null` (autoscale or single-node) | Treated as `1` → `(1 + 1) = 2` |
| Cluster API call fails for `existing_cluster_id` | Logged, `cost_weight = 1.0` for that task |
| All tasks have same cost_weight | `×` not shown in Slack, header stays "slowest" |

---

## Non-Goals

- No changes to `datadoc.proposals` or `datadoc.applied_changes` schema
- No FinOps $ translation (that's P17)
- No Brubank implementation (homogeneous cluster, zero ranking impact)
