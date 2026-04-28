"""
datadoc_validator — Validates equivalence between an optimized v2 notebook and the
production notebook.

Validation tiers:
  T1 — Smoke:  v2 ran without errors (applies to all notebooks)
  T2 — Schema: output tables have the same columns and types as in prod
  T3 — Data:   row count + numeric column sums match prod

For yellow notebooks without external side effects, only T1 (smoke_only=True).
For green notebooks, T1 → T2 → T3 in sequence (stops at first failure).

Limitations:
- Does not work if the notebook READS and WRITES to the same table (self-reference).
- Requires the cluster to be running or creatable on-demand.
- Takes as long as the original notebook (v2 runs in its entirety).
"""

import base64
import json
import re
import time
import urllib.parse
import urllib.request
from typing import Dict, List, Optional


TEST_SCHEMA = "datadoc"  # all test tables are created here, never in prod


# ============================================================
# 1. Suffix injection into the source
# ============================================================

def inject_test_suffix(source: str, target_tables: List[str], suffix: str) -> str:
    """
    Replaces target tables with their test version across all common
    syntactic variants (quoted, unquoted, in SQL, in DataFrame API).
    """
    modified = source
    for table in target_tables:
        _, _, name_part = table.partition(".")
        new_table = f"{TEST_SCHEMA}.{name_part}_test_{suffix}"
        modified = modified.replace(f"'{table}'", f"'{new_table}'")
        modified = modified.replace(f'"{table}"', f'"{new_table}"')
        pattern = r"\b" + re.escape(table) + r"\b"
        modified = re.sub(pattern, new_table, modified)
    return modified


# ============================================================
# 2. Databricks HTTP helpers
# ============================================================

def _http_request(host: str, token: str, method: str, path: str,
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


def upload_notebook(host: str, token: str, path: str, source: str) -> None:
    encoded = base64.b64encode(source.encode("utf-8")).decode("ascii")
    _http_request(host, token, "POST", "/api/2.0/workspace/import", body={
        "path": path, "format": "SOURCE", "language": "PYTHON",
        "content": encoded, "overwrite": True,
    })


def submit_one_time_run(host: str, token: str, notebook_path: str,
                        cluster_id: str, run_name: str) -> int:
    resp = _http_request(host, token, "POST", "/api/2.1/jobs/runs/submit", body={
        "run_name": run_name,
        "tasks": [{
            "task_key": "qa_validation",
            "existing_cluster_id": cluster_id,
            "notebook_task": {"notebook_path": notebook_path},
        }],
    })
    return resp["run_id"]


def wait_for_run(host: str, token: str, run_id: int,
                 timeout_s: int = 3600, poll_interval_s: int = 30) -> str:
    start = time.time()
    while time.time() - start < timeout_s:
        resp = _http_request(host, token, "GET", "/api/2.1/jobs/runs/get",
                             params={"run_id": run_id})
        life = resp.get("state", {}).get("life_cycle_state")
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return resp.get("state", {}).get("result_state", "UNKNOWN")
        time.sleep(poll_interval_s)
    return "TIMEOUT"


# ============================================================
# 3. Comparison checks (T2 and T3)
# ============================================================

NUMERIC_TYPES = {"integer", "long", "short", "byte", "double", "float", "decimal"}


def compare_schemas(spark, prod_table: str, test_table: str) -> Dict:
    """T2 — Compares column names and types between prod and test."""
    prod_fields = {f.name: f.dataType.simpleString()
                   for f in spark.read.table(prod_table).schema.fields}
    test_fields = {f.name: f.dataType.simpleString()
                   for f in spark.read.table(test_table).schema.fields}
    missing = sorted(set(prod_fields) - set(test_fields))
    added   = sorted(set(test_fields) - set(prod_fields))
    changed = sorted(c for c in set(prod_fields) & set(test_fields)
                     if prod_fields[c] != test_fields[c])
    return {
        "prod_table": prod_table,
        "test_table": test_table,
        "match": not missing and not added and not changed,
        "missing_columns": missing,
        "added_columns":   added,
        "changed_types":   {c: {"prod": prod_fields[c], "test": test_fields[c]}
                            for c in changed},
    }


def compare_tables(spark, prod_table: str, test_table: str) -> Dict:
    """T3 — Compares row count + numeric column sums between prod and test."""
    from pyspark.sql import functions as F
    prod = spark.read.table(prod_table)
    test = spark.read.table(test_table)
    prod_count = prod.count()
    test_count = test.count()
    prod_numeric = {f.name for f in prod.schema.fields
                    if f.dataType.typeName() in NUMERIC_TYPES}
    test_numeric = {f.name for f in test.schema.fields
                    if f.dataType.typeName() in NUMERIC_TYPES}
    common_numeric = sorted(prod_numeric & test_numeric)
    col_diffs = []
    if common_numeric:
        prod_sums = prod.agg(*[F.sum(c).alias(c) for c in common_numeric]).first()
        test_sums = test.agg(*[F.sum(c).alias(c) for c in common_numeric]).first()
        for c in common_numeric:
            ps, ts = prod_sums[c], test_sums[c]
            col_diffs.append({
                "column": c,
                "prod_sum": float(ps) if ps is not None else None,
                "test_sum": float(ts) if ts is not None else None,
                "match": (ps is None and ts is None) or (ps == ts),
            })
    overall = (prod_count == test_count) and all(c["match"] for c in col_diffs)
    return {
        "prod_table": prod_table, "test_table": test_table,
        "prod_count": prod_count, "test_count": test_count,
        "count_match": prod_count == test_count,
        "column_diffs": col_diffs, "overall_match": overall,
    }


# ============================================================
# 4. Internal helpers
# ============================================================

def _cleanup_test_tables(spark, target_tables: List[str], suffix: str) -> None:
    for prod_table in target_tables:
        _, _, name_part = prod_table.partition(".")
        test_table = f"{TEST_SCHEMA}.{name_part}_test_{suffix}"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {test_table}")
        except Exception as e:
            print(f"[validator] warning: could not drop {test_table}: {e}")


def _safe_delete_workspace_path(host: str, token: str, path: str) -> None:
    try:
        _http_request(host, token, "POST", "/api/2.0/workspace/delete",
                      body={"path": path, "recursive": False})
    except Exception as e:
        print(f"[validator] warning: could not delete {path}: {e}")


# ============================================================
# 5. Validation orchestrator
# ============================================================

def validate_proposal(
    host: str,
    token: str,
    spark,
    v2_source: str,
    target_tables: List[str],
    cluster_id: str,
    sandbox_dir: str = "/Workspace/Shared/DataDoctor/validation_sandbox",
    smoke_only: bool = False,
) -> Dict:
    """
    Orchestrates v2 validation.

    smoke_only=False (green):  T1 smoke → T2 schema → T3 data
    smoke_only=True  (yellow): T1 smoke only

    Possible validation_status values:
      passed          — T1+T2+T3 all passed
      smoke_passed    — T1 passed, smoke_only=True
      smoke_failed    — T1 failed (notebook crashed or timed out)
      schema_mismatch — T1 passed, T2 failed (columns/types changed)
      data_mismatch   — T1+T2 passed, T3 failed (row count or sums differ)
      failed          — validator infrastructure error
    """
    suffix = f"{int(time.time())}"
    sandbox_path = f"{sandbox_dir}/test_{suffix}"
    print(f"[validator] suffix={suffix} smoke_only={smoke_only}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
    injected_source = inject_test_suffix(v2_source, target_tables, suffix)

    try:
        _http_request(host, token, "POST", "/api/2.0/workspace/mkdirs",
                      body={"path": sandbox_dir})
    except Exception:
        pass

    upload_notebook(host, token, sandbox_path, injected_source)
    print(f"[validator] sandbox: {sandbox_path}")

    for table in target_tables:
        _, _, name_part = table.partition(".")
        test_table = f"{TEST_SCHEMA}.{name_part}_test_{suffix}"
        try:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {test_table} LIKE {table}")
            print(f"[validator] test table pre-created: {test_table}")
        except Exception as e:
            print(f"[validator] WARNING: could not pre-create {test_table}: {e}")

    run_id = submit_one_time_run(host, token, sandbox_path, cluster_id,
                                 run_name=f"datadoc_validator_{suffix}")
    print(f"[validator] test run_id={run_id}, waiting...")
    result = wait_for_run(host, token, run_id)
    print(f"[validator] result={result}")

    # T1 — Smoke
    if result != "SUCCESS":
        _cleanup_test_tables(spark, target_tables, suffix)
        _safe_delete_workspace_path(host, token, sandbox_path)
        return {"validation_status": "smoke_failed",
                "reason": f"Run ended with state {result}",
                "run_id": run_id, "suffix": suffix}

    if smoke_only:
        _cleanup_test_tables(spark, target_tables, suffix)
        _safe_delete_workspace_path(host, token, sandbox_path)
        return {"validation_status": "smoke_passed", "run_id": run_id, "suffix": suffix}

    # T2 — Schema
    schema_checks, schema_errors = [], []
    for prod_table in target_tables:
        _, _, name_part = prod_table.partition(".")
        test_table = f"{TEST_SCHEMA}.{name_part}_test_{suffix}"
        try:
            schema_checks.append(compare_schemas(spark, prod_table, test_table))
        except Exception as e:
            schema_errors.append({"table": prod_table, "error": str(e)})

    if not all(c["match"] for c in schema_checks) or schema_errors:
        _cleanup_test_tables(spark, target_tables, suffix)
        _safe_delete_workspace_path(host, token, sandbox_path)
        return {"validation_status": "schema_mismatch",
                "run_id": run_id, "suffix": suffix,
                "schema_checks": schema_checks, "schema_errors": schema_errors}

    # T3 — Data
    comparisons, data_errors = [], []
    for prod_table in target_tables:
        _, _, name_part = prod_table.partition(".")
        test_table = f"{TEST_SCHEMA}.{name_part}_test_{suffix}"
        try:
            comparisons.append(compare_tables(spark, prod_table, test_table))
        except Exception as e:
            data_errors.append({"table": prod_table, "error": str(e)})

    _cleanup_test_tables(spark, target_tables, suffix)
    _safe_delete_workspace_path(host, token, sandbox_path)

    all_match = (comparisons and all(c["overall_match"] for c in comparisons)
                 and not data_errors)
    return {
        "validation_status": "passed" if all_match else "data_mismatch",
        "run_id": run_id, "suffix": suffix,
        "schema_checks": schema_checks,
        "comparisons": comparisons, "errors": data_errors,
    }
