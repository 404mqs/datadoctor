"""
Upload all Data Doctor files to your Databricks workspace.

Usage:
    python scripts/upload_all.py

Reads:
    - datadoctor_config.yml  (workspace_path)
    - ~/.databrickscfg        (host + token)
"""

import base64
import configparser
import json
import pathlib
import urllib.request
import yaml

REPO_ROOT = pathlib.Path(__file__).parent.parent


def _load_databricks_cfg():
    cfg = configparser.ConfigParser()
    cfg_path = pathlib.Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"~/.databrickscfg not found. "
            f"Run 'databricks configure' or create the file manually."
        )
    cfg.read(cfg_path)
    section = "DEFAULT"
    return cfg[section]["host"].rstrip("/"), cfg[section]["token"]


def _load_config():
    config_path = REPO_ROOT / "datadoctor_config.yml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _api(host: str, token: str, path: str, body: dict) -> dict:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(f"{host}{path}", data=data, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode("utf-8"))


def upload_workspace_file(host: str, token: str, local_path: pathlib.Path,
                          remote_path: str) -> None:
    content = local_path.read_text(encoding="utf-8")
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    _api(host, token, "/api/2.0/workspace/import", {
        "path": remote_path,
        "format": "AUTO",
        "content": encoded,
        "overwrite": True,
    })
    print(f"  ✓ {remote_path}  ({len(content):,} chars)")


def upload_notebook(host: str, token: str, local_path: pathlib.Path,
                    remote_path: str) -> None:
    # Delete existing Workspace File first (if it was previously uploaded as AUTO format)
    try:
        _api(host, token, "/api/2.0/workspace/delete", {"path": remote_path, "recursive": False})
    except Exception:
        pass  # file may not exist yet

    content = local_path.read_text(encoding="utf-8")
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    _api(host, token, "/api/2.0/workspace/import", {
        "path": remote_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded,
        "overwrite": True,
    })
    print(f"  ✓ {remote_path}  (notebook, {len(content):,} chars)")


def main():
    host, token = _load_databricks_cfg()
    cfg = _load_config()
    base = cfg["databricks"]["workspace_path"]

    print(f"Uploading to: {host}{base}\n")

    # Ensure workspace directory exists
    try:
        _api(host, token, "/api/2.0/workspace/mkdirs", {"path": base})
        _api(host, token, "/api/2.0/workspace/mkdirs", {"path": f"{base}/proposals"})
        _api(host, token, "/api/2.0/workspace/mkdirs", {"path": f"{base}/backups"})
        _api(host, token, "/api/2.0/workspace/mkdirs", {"path": f"{base}/validation_sandbox"})
        _api(host, token, "/api/2.0/workspace/mkdirs", {"path": f"{base}/notifiers"})
    except Exception as e:
        print(f"  [mkdirs] {e}")

    # Config
    upload_workspace_file(host, token, REPO_ROOT / "datadoctor_config.yml",
                          f"{base}/datadoctor_config.yml")

    # Prompt
    upload_workspace_file(host, token, REPO_ROOT / "prompt/prompt_optimization.txt",
                          f"{base}/prompt_optimization.txt")

    # Modules
    for module in ["datadoc_tier_classifier.py", "datadoc_table_sizer.py",
                   "datadoc_validator.py", "datadoc_swap.py"]:
        upload_workspace_file(host, token, REPO_ROOT / "modules" / module,
                              f"{base}/{module}")

    # Notifiers
    for nf in ["base.py", "slack.py"]:
        upload_workspace_file(host, token, REPO_ROOT / "modules/notifiers" / nf,
                              f"{base}/notifiers/{nf}")

    # Notebooks (must be uploaded as SOURCE notebooks, not Workspace Files)
    upload_notebook(host, token, REPO_ROOT / "notebooks/datadoc.py",
                    f"{base}/datadoc")
    upload_notebook(host, token, REPO_ROOT / "notebooks/datadoc_approve.py",
                    f"{base}/datadoc_approve")

    print("\nAll files uploaded successfully.")
    print(f"\nNext steps:")
    print(f"  1. Run schema/datadoc.sql in a Databricks notebook to create the Delta tables")
    print(f"  2. Run: python scripts/add_to_job.py  (dry run)")
    print(f"  3. Run: python scripts/add_to_job.py --apply  (to add the task to your job)")


if __name__ == "__main__":
    main()
