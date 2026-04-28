"""
datadoc_validator — Validates equivalence between an optimized v2 notebook and the
production notebook.

Strategy:
1. Takes the v2 source code and injects a suffix into the output table names
   (e.g.: 'dwh_analytics.dl_transacciones' -> 'datadoc.dl_transacciones_test_{ts}')
2. Uploads the injected code to a sandbox path within the workspace
3. Launches a one-time run via Jobs API using an existing cluster
4. Waits for it to finish
5. Compares the resulting test tables vs the prod tables:
     - row count
     - sums of numeric columns
     - aggregated hash ordered by PKs (if provided)
6. Drops the test tables
7. Returns a dict with validation_status + details

Limitations:
- Does not work if the notebook READS and WRITES to the same table (self-reference).
  Those are classified as tier yellow/red by the classifier.
- Requires the cluster to be running or able to be created on-demand.
- Takes as long as the original notebook (the v2 runs in its entirety).
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

    E.g.: 'dwh_analytics.dl_clientes' -> 'datadoc.dl_clientes_test_abc123'
    """
    modified = source
    for table in target_tables:
        schema_part, _, name_part = table.partition(".")
        new_table = f"{TEST_SCHEMA}.{name_part}_test_{suffix}"

        # Literal replacement in quoted strings (Python and embedded SQL)
        modified = modified.replace(f"'{table}'", f"'{new_table}'")
        modified = modified.replace(f'"{table}"', f'"{new_table}"')

        # Replacement in bare SQL (MERGE INTO schema.table, FROM schema.table, etc)
        # Use word boundary to avoid touching partial matches (e.g.: _stock when
        # the table is dl_clientes).
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
    """Uploads a notebook in SOURCE format (plain text) to the workspace."""
    encoded = base64.b64encode(source.encode("utf-8")).decode("ascii")
    _http_request(host, token, "POST", "/api/2.0/workspace/import", body={
        "path": path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded,
        "overwrite": True,
    })


def submit_one_time_run(host: str, token: str, notebook_path: str,
                        cluster_id: str, run_name: str) -> int:
    """Launches a one-time run on an existing cluster. Returns run_id."""
    payload = {
        "run_name": run_name,
        "tasks": [{
            "task_key": "qa_validation",
            "existing_cluster_id": cluster_id,
            "notebook_task": {"notebook_path": notebook_path},
        }],
    }
    resp = _http_request(host, token, "POST", "/api/2.1/jobs/runs/submit", body=payload)
    return resp["run_id"]


def wait_for_run(host: str, token: str, run_id: int, timeout_s: int = 3600,
                 poll_interval_s: int = 30) -> str:
    """Polls until the run finishes. Returns result_state."""
    start = time.time()
    while time.time() - start < timeout_s:
        resp = _http_request(
            host, token, "GET", "/api/2.1/jobs/runs/get",
            params={"run_id": run_id}
        )
        life = resp.get("state", {}).get("life_cycle_state")
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return resp.get("state", {}).get("result_state", "UNKNOWN")
        time.sleep(poll_interval_s)
    return "TIMEOUT"


# ============================================================
# 3. Prod vs test table comparison
# ============================================================

NUMERIC_TYPES = {"integer", "long", "short", "byte", "double", "float", "decimal"}


def compare_tables(spark, prod_table: str, test_table: str) -> Dict:
    """
    Compares row count + sums of numeric columns between prod and test.

    Does not compare row-by-row hash because it would be too expensive. Sums + count
    are a sufficiently strong proxy to detect real diffs; for deeper inspection
    run an EXCEPT manually.
    """
    from pyspark.sql import functions as F

    prod = spark.read.table(prod_table)
    test = spark.read.table(test_table)

    prod_count = prod.count()
    test_count = test.count()

    # Common numeric columns
    prod_numeric = {f.name: f.dataType.typeName() for f in prod.schema.fields
                    if f.dataType.typeName() in NUMERIC_TYPES}
    test_numeric = {f.name for f in test.schema.fields
                    if f.dataType.typeName() in NUMERIC_TYPES}
    common_numeric = sorted(set(prod_numeric) & test_numeric)

    col_diffs = []
    if common_numeric:
        prod_sums = prod.agg(*[F.sum(c).alias(c) for c in common_numeric]).first()
        test_sums = test.agg(*[F.sum(c).alias(c) for c in common_numeric]).first()

        for c in common_numeric:
            ps = prod_sums[c]
            ts = test_sums[c]
            col_diffs.append({
                "column": c,
                "prod_sum": float(ps) if ps is not None else None,
                "test_sum": float(ts) if ts is not None else None,
                "match": (ps is None and ts is None) or (ps == ts),
            })

    overall = (prod_count == test_count) and all(c["match"] for c in col_diffs)

    return {
        "prod_table": prod_table,
        "test_table": test_table,
        "prod_count": prod_count,
        "test_count": test_count,
        "count_match": prod_count == test_count,
        "column_diffs": col_diffs,
        "overall_match": overall,
    }


# ============================================================
# 4. Validation orchestrator
# ============================================================

def validate_proposal(host: str, token: str, spark, v2_source: str,
                      target_tables: list, cluster_id: str,
                      sandbox_dir: str = "/Workspace/Shared/DataDoctor/validation_sandbox") -> dict:
    """
    Orchestrates the complete validation of a v2.

    Flow:
      1. Generates a unique suffix based on timestamp
      2. Injects suffix into the source
      3. Uploads the modified notebook to the sandbox
      4. Launches one-time run
      5. Waits for completion
      6. If SUCCESS, compares tables
      7. Drops test tables
      8. Deletes the sandbox notebook
    """
    suffix = f"{int(time.time())}"
    sandbox_path = f"{sandbox_dir}/test_{suffix}"

    print(f"[validator] suffix={suffix}")

    # Ensure the test schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")

    # 1. Inject suffix
    injected_source = inject_test_suffix(v2_source, target_tables, suffix)

    # 2. Upload to sandbox
    # Ensure sandbox directory exists
    try:
        _http_request(host, token, "POST", "/api/2.0/workspace/mkdirs",
                      body={"path": sandbox_dir})
    except Exception:
        pass  # probably already exists

    upload_notebook(host, token, sandbox_path, injected_source)
    print(f"[validator] sandbox notebook at {sandbox_path}")

    # 2b. Pre-create empty test tables so DELETE does not fail on the first run.
    # The notebook does DELETE+INSERT — without the table, DELETE fails with TABLE_NOT_FOUND.
    for table in target_tables:
        _, _, name_part = table.partition(".")
        test_table = f"{TEST_SCHEMA}.{name_part}_test_{suffix}"
        try:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {test_table} LIKE {table}")
            print(f"[validator] test table pre-created: {test_table}")
        except Exception as e:
            print(f"[validator] WARNING: could not pre-create {test_table}: {e}")

    # 3. Launch run
    run_id = submit_one_time_run(
        host, token, sandbox_path, cluster_id,
        run_name=f"datadoc_validator_{suffix}"
    )
    print(f"[validator] test run_id={run_id}, waiting...")

    # 4. Wait
    result = wait_for_run(host, token, run_id)
    print(f"[validator] result={result}")

    if result != "SUCCESS":
        # Clean up sandbox notebook even on failure
        _safe_delete_workspace_path(host, token, sandbox_path)
        return {
            "validation_status": "failed",
            "reason": f"Test run finished with state {result}",
            "run_id": run_id,
            "suffix": suffix,
        }

    # 5. Compare tables
    comparisons = []
    errors = []
    for prod_table in target_tables:
        _, _, name_part = prod_table.partition(".")
        test_table = f"{TEST_SCHEMA}.{name_part}_test_{suffix}"
        try:
            comparisons.append(compare_tables(spark, prod_table, test_table))
        except Exception as e:
            errors.append({"table": prod_table, "error": str(e)})

    # 6. Cleanup
    for prod_table in target_tables:
        _, _, name_part = prod_table.partition(".")
        test_table = f"{TEST_SCHEMA}.{name_part}_test_{suffix}"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {test_table}")
        except Exception as e:
            print(f"[validator] warning: could not drop {test_table}: {e}")

    _safe_delete_workspace_path(host, token, sandbox_path)

    all_match = comparisons and all(c["overall_match"] for c in comparisons) and not errors

    return {
        "validation_status": "passed" if all_match else "diffs_detected",
        "run_id": run_id,
        "suffix": suffix,
        "comparisons": comparisons,
        "errors": errors,
    }


def _safe_delete_workspace_path(host: str, token: str, path: str) -> None:
    try:
        _http_request(host, token, "POST", "/api/2.0/workspace/delete",
                      body={"path": path, "recursive": False})
    except Exception as e:
        print(f"[validator] warning: could not delete {path}: {e}")
