# Scripts

## `upload_all.py`

Uploads all Data Doctor files to your Databricks workspace.

**Prerequisites:**
- `~/.databrickscfg` with your workspace host and token
- `datadoctor_config.yml` filled in at the repo root
- `pip install pyyaml` (or `conda install pyyaml`)

**Usage:**
```bash
python scripts/upload_all.py
```

## `add_to_job.py`

Adds the `DATADOCTOR` task to your orchestrator job as the final step.
Computes leaf tasks algorithmically — no manual dependency wiring needed.

**Usage:**
```bash
python scripts/add_to_job.py                            # dry run
python scripts/add_to_job.py --apply                    # apply
python scripts/add_to_job.py --apply --exclude FLAG_START,FLAG_END  # exclude control-flow flags
```
