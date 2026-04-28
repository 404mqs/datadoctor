"""
datadoc_table_sizer — Estimates the on-disk size of tables and parquets read in a notebook,
so that the LLM can make an informed decision about whether to propose broadcast hints.

Strategy:
- Delta tables: DESCRIBE DETAIL schema.table → sizeInBytes
- Parquets: dbutils.fs.ls recursive → sum of file sizes
- Derived temp views: broadcast_safe propagation via lineage (see build_derived_view_context)
- Tables without permissions / not found: silently skipped
"""

import re
from typing import Dict, List, Optional

BROADCAST_SAFE_MB = 200  # conservative threshold (on disk); in memory can be 3-10x larger


# ── extraction helpers ───────────────────────────────────────────────────────

def _non_comment(source: str) -> str:
    return "\n".join(
        line for line in source.splitlines()
        if not line.lstrip().startswith("#")
        and not line.lstrip().startswith("--")
        and not re.match(r"\s*(from\s+\S+\s+import|import\s+)", line)
    )


def extract_read_tables(source: str) -> List[str]:
    """Extracts Delta tables read in the notebook (schema.table), ignoring comments."""
    text = _non_comment(source)
    patterns = [
        r"spark\.read\.table\(\s*['\"]([a-zA-Z0-9_.]+)['\"]",
        r"spark\.table\(\s*['\"]([a-zA-Z0-9_.]+)['\"]",
        r"\bFROM\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\b",
        r"\bJOIN\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\b",
    ]
    tables = set()
    for pattern in patterns:
        for match in re.findall(pattern, text, re.IGNORECASE):
            if "." in match:
                tables.add(match.lower())
    return sorted(tables)


def extract_read_parquets(source: str) -> List[str]:
    """Extracts parquet paths read with spark.read.parquet(...), ignoring comments."""
    text = _non_comment(source)
    pattern = r"spark\.read(?:\.[a-z]+)*\.parquet\(\s*['\"]([^'\"]+)['\"]"
    return sorted(set(re.findall(pattern, text, re.IGNORECASE)))


# ── size estimation ──────────────────────────────────────────────────────────

def _delta_size_bytes(spark, table: str) -> int:
    """sizeInBytes of a Delta table via DESCRIBE DETAIL. -1 if it cannot be resolved."""
    try:
        row = spark.sql(f"DESCRIBE DETAIL {table}").first()
        return int(row["sizeInBytes"])
    except Exception:
        return -1


_PARQUET_LS_MAX_FILES = 5_000  # abort early on huge directories → conservatively unsafe


def _parquet_size_bytes(dbutils, path: str) -> int:
    """Recursive sum of file sizes under path via dbutils.fs.ls.
    Returns -1 if it fails or if the directory exceeds _PARQUET_LS_MAX_FILES entries
    (signal that it is a large dataset → conservatively unsafe for broadcast).
    """
    try:
        total = 0
        files_seen = 0
        stack = [path]
        while stack:
            current = stack.pop()
            for item in dbutils.fs.ls(current):
                files_seen += 1
                if files_seen > _PARQUET_LS_MAX_FILES:
                    return -1  # too large to estimate quickly → conservative
                if item.size == 0:  # directory
                    stack.append(item.path)
                else:
                    total += item.size
        return total
    except Exception:
        return -1


# ── temp view lineage ────────────────────────────────────────────────────────

def build_derived_view_context(source: str, sizes: Dict[str, dict]) -> Dict[str, dict]:
    """
    Detects temp views derived from large sources and adds them to the context with
    broadcast_safe=False, propagating the status transitively:

      spark.read.parquet(path) → var → var.createOrReplaceTempView("name")
      spark.sql("... FROM src ...") → var → var.createOrReplaceTempView("name")

    If src is unsafe, var is unsafe; if var is unsafe, name is unsafe.
    Iterates until convergence (max 5 steps) to resolve long chains.
    """
    text = _non_comment(source)

    # Build name → broadcast_safe lookup with already-known sizes
    known: Dict[str, Optional[bool]] = {}  # None = unknown
    for key, info in sizes.items():
        # key can be "schema.table" or "NAME (/path/)"
        simple = key.split("(")[0].strip().lower()
        known[simple] = info["broadcast_safe"]
        if "." in simple:
            known[simple.split(".")[-1]] = info["broadcast_safe"]

    new_entries: Dict[str, dict] = {}

    def _mark_unsafe(name: str, derived_from: str):
        n = name.lower()
        if known.get(n) is False:
            return  # already marked
        known[n] = False
        new_entries[n] = {
            "size_bytes": -1,
            "size_mb": -1.0,
            "broadcast_safe": False,
            "source_type": "derived_view",
            "derived_from": derived_from,
        }

    # ── Step 1: var = spark.read.parquet(path) → var inherits the parquet status ──
    # If the parquet could not be estimated (not in sizes), mark conservatively unsafe:
    # unknown size = do not assume it is small.
    parquet_var_pat = r'(\w+)\s*=\s*spark\.read(?:\.[a-z]+)*\.parquet\(\s*[\'"]([^\'"]+)[\'"]\s*\)'
    for m in re.finditer(parquet_var_pat, text, re.IGNORECASE):
        var_name, path = m.group(1).lower(), m.group(2)
        path_name = path.rstrip("/").split("/")[-1].lower()
        matched = False
        for size_key, info in sizes.items():
            if path in size_key or path_name in size_key.lower():
                known[var_name] = info["broadcast_safe"]
                if not info["broadcast_safe"]:
                    new_entries[var_name] = {
                        "size_bytes": info["size_bytes"],
                        "size_mb": info["size_mb"],
                        "broadcast_safe": False,
                        "source_type": "parquet_var",
                        "derived_from": size_key,
                    }
                matched = True
                break
        if not matched and known.get(var_name) is None:
            # Parquet not estimated → conservatively unsafe
            _mark_unsafe(var_name, f"parquet {path_name} (size not estimated → conservative)")

    # ── Step 2: var = spark.sql("""...""") → collect SQL sources ──
    sql_sources: Dict[str, List[str]] = {}
    sql_pat = r'(\w+)\s*=\s*spark\.sql\(\s*(?:f?""")(.*?)(?:""")\s*\)'
    for m in re.finditer(sql_pat, text, re.DOTALL | re.IGNORECASE):
        var_name = m.group(1).lower()
        sql = m.group(2)
        srcs = set()
        for p in [r'\bFROM\s+(\w+)', r'\bJOIN\s+(\w+)']:
            for match in re.findall(p, sql, re.IGNORECASE):
                srcs.add(match.lower())
        sql_sources[var_name] = sorted(srcs)

    # Propagate unsafe transitively (max 5 iterations)
    for _ in range(5):
        changed = False
        for var_name, srcs in sql_sources.items():
            if known.get(var_name) is False:
                continue
            for src in srcs:
                if known.get(src) is False:
                    _mark_unsafe(var_name, src)
                    changed = True
                    break
        if not changed:
            break

    # ── Step 3: var.createOrReplaceTempView("name") → name inherits var status ──
    tempview_pat = r'(\w+)\.createOrReplaceTempView\(\s*[\'"](\w+)[\'"]\s*\)'
    for m in re.finditer(tempview_pat, text, re.IGNORECASE):
        var_name, view_name = m.group(1).lower(), m.group(2).lower()
        if known.get(var_name) is False:
            _mark_unsafe(view_name, var_name)
        elif known.get(var_name) is True:
            known[view_name] = True

    return new_entries


# ── main orchestrator ────────────────────────────────────────────────────────

def estimate_sizes(spark, dbutils, source: str) -> Dict[str, dict]:
    """
    Estimates the on-disk size of Delta tables and parquets read in the notebook,
    and propagates broadcast_safe status to derived temp views.

    Returns dict with structure:
        {
            "schema.table": {
                "size_bytes": int,
                "size_mb": float,
                "broadcast_safe": bool,
                "source_type": "delta" | "parquet" | "parquet_var" | "derived_view",
            }
        }
    """
    result: Dict[str, dict] = {}

    for table in extract_read_tables(source):
        size = _delta_size_bytes(spark, table)
        if size < 0:
            continue
        size_mb = size / (1024 * 1024)
        result[table] = {
            "size_bytes": size,
            "size_mb": round(size_mb, 1),
            "broadcast_safe": size_mb < BROADCAST_SAFE_MB,
            "source_type": "delta",
        }

    for path in extract_read_parquets(source):
        name = path.rstrip("/").split("/")[-1]
        size = _parquet_size_bytes(dbutils, path)
        if size < 0:
            continue
        size_mb = size / (1024 * 1024)
        key = f"{name} ({path})"
        result[key] = {
            "size_bytes": size,
            "size_mb": round(size_mb, 1),
            "broadcast_safe": size_mb < BROADCAST_SAFE_MB,
            "source_type": "parquet",
        }

    # Propagate status to derived temp views
    derived = build_derived_view_context(source, result)
    result.update(derived)

    return result


def format_size_context(sizes: Dict[str, dict]) -> str:
    """Formats the sizes dict as text for injection into the LLM prompt."""
    if not sizes:
        return "(sizes could not be estimated — use conservative broadcast criteria)"

    lines = [f"TABLE SIZES ON DISK (broadcast threshold: {BROADCAST_SAFE_MB} MB):"]
    for name, info in sorted(sizes.items(), key=lambda x: (-x[1]["size_mb"] if x[1]["size_mb"] >= 0 else float("inf"))):
        if info["broadcast_safe"]:
            tag = "✓ broadcast OK"
        elif info["size_mb"] < 0:
            tag = "✗ NO broadcast (large source — size not estimable)"
        else:
            tag = f"✗ NO broadcast ({info['size_mb']:.0f} MB > threshold)"
        extra = f"  [derived from: {info['derived_from']}]" if info.get("derived_from") else ""
        lines.append(f"  {name}: {info['size_mb']:.1f} MB  — {tag}{extra}")
    return "\n".join(lines)
