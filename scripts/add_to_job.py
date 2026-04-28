"""
Add the DATADOCTOR task to your Databricks orchestrator job.

The task is added as the last step: it depends on all leaf tasks (tasks that no
other task depends on) and runs only when all of them succeed.

Usage:
    python scripts/add_to_job.py                         # dry run (shows what would change)
    python scripts/add_to_job.py --apply                 # applies the change
    python scripts/add_to_job.py --exclude FLAG_A,FLAG_B # exclude control-flow tasks from leaf detection
"""

import argparse
import configparser
import json
import pathlib
import urllib.request
import urllib.parse
import yaml

REPO_ROOT = pathlib.Path(__file__).parent.parent


def _load_databricks_cfg():
    cfg = configparser.ConfigParser()
    cfg.read(pathlib.Path.home() / ".databrickscfg")
    return cfg["DEFAULT"]["host"].rstrip("/"), cfg["DEFAULT"]["token"]


def _load_config():
    with open(REPO_ROOT / "datadoctor_config.yml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _http(host: str, token: str, method: str, path: str,
          body: dict = None, params: dict = None) -> dict:
    url = f"{host}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode("utf-8"))


def compute_leaf_tasks(tasks: list, exclude: set) -> list:
    """
    Returns task_keys that no other task depends on (leaves of the DAG),
    excluding any keys in `exclude`.
    """
    all_keys = {t["task_key"] for t in tasks}
    depended_on = set()
    for t in tasks:
        for dep in t.get("depends_on", []):
            depended_on.add(dep["task_key"])
    leaves = [k for k in all_keys if k not in depended_on and k not in exclude]
    return sorted(leaves)


def main():
    parser = argparse.ArgumentParser(description="Add DATADOCTOR task to your orchestrator job.")
    parser.add_argument("--apply", action="store_true",
                        help="Apply changes (default: dry run)")
    parser.add_argument("--exclude", default="",
                        help="Comma-separated task keys to exclude from leaf detection "
                             "(e.g. control-flow flags like FLAG_START,FLAG_END)")
    args = parser.parse_args()

    host, token = _load_databricks_cfg()
    cfg = _load_config()

    job_id         = int(cfg["databricks"]["job_id"])
    cluster_id     = cfg["databricks"]["cluster_id"]
    workspace_path = cfg["databricks"]["workspace_path"]
    task_key       = cfg["agent"]["self_task_key"]
    exclude_keys   = set(k.strip() for k in args.exclude.split(",") if k.strip())

    # Fetch current job definition
    job = _http(host, token, "GET", "/api/2.1/jobs/get", params={"job_id": job_id})
    settings = job["settings"]
    existing_tasks = settings.get("tasks", [])
    existing_keys = {t["task_key"] for t in existing_tasks}

    if task_key in existing_keys:
        print(f"Task '{task_key}' already exists in job {job_id}. Nothing to do.")
        return

    leaves = compute_leaf_tasks(existing_tasks, exclude=exclude_keys)
    print(f"Job {job_id}: {len(existing_tasks)} existing tasks")
    print(f"Exclude keys: {exclude_keys or '(none)'}")
    print(f"Leaf tasks ({len(leaves)}): {leaves}")

    new_task = {
        "task_key": task_key,
        "notebook_task": {
            "notebook_path": f"{workspace_path}/datadoc",
            "base_parameters": {
                "validate_mode": "auto",
                "force_run": "false",
            },
        },
        "existing_cluster_id": cluster_id,
        "depends_on": [{"task_key": k} for k in leaves],
        "run_if": "ALL_SUCCESS",
    }

    print(f"\nNew task to add:")
    print(json.dumps(new_task, indent=2))

    if not args.apply:
        print("\n[DRY RUN] No changes made. Pass --apply to execute.")
        return

    updated_tasks = existing_tasks + [new_task]
    _http(host, token, "POST", "/api/2.1/jobs/reset", {
        "job_id": job_id,
        "new_settings": {**settings, "tasks": updated_tasks},
    })
    print(f"\n✓ Task '{task_key}' added to job {job_id} with {len(leaves)} dependencies.")


if __name__ == "__main__":
    main()
