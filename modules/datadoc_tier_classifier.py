"""
datadoc_tier_classifier — Classifies a notebook into tier green/yellow based on
how safe it is to apply automatic optimizations.

Tiers:
  green  — Writes Delta tables, no external side effects. Validatable end-to-end.
            Proposal can be made with automatic validation + 1-click approval.
  yellow — Has side effects (Sheets/Slack/external API), self-reference, no detected
            tables, or notebook too large. Generates proposal but does NOT validate
            automatically. Manual review required.

Usage (from the main notebook):

    from datadoc_tier_classifier import classify_notebook
    result = classify_notebook(source_code)
    # {"tier": "green", "target_tables": [...], "side_effects": [], "reason": "..."}
"""

import re
from typing import Dict, List


SIDE_EFFECT_PATTERNS = [
    (r"requests\.(post|get|put|delete|patch)\s*\(", "HTTP external (requests)"),
    (r"urllib\.request\.urlopen", "HTTP external (urllib)"),
    (r"slack_webhook|SLACK_WEBHOOK|hooks\.slack", "Slack webhook"),
    (r"sheets\.spreadsheets|gspread|google\.auth|service_account", "Google Sheets/Drive"),
    (r"smtplib|sendmail|send_email|SMTP", "Email"),
    (r"dbutils\.notebook\.run\(", "Nested notebook call"),
    (r"\.writeStream|\.start\(\)", "Structured Streaming"),
    (r"subprocess\.(run|call|Popen)", "External subprocess"),
]

OUTPUT_TABLE_PATTERNS = [
    # .saveAsTable("schema.table") / .saveAsTable('schema.table')
    r"\.saveAsTable\(\s*['\"]([a-zA-Z0-9_.]+)['\"]",
    # MERGE INTO schema.table
    r"MERGE\s+INTO\s+([a-zA-Z0-9_.]+)",
    # INSERT (OVERWRITE) INTO schema.table
    r"INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?(?:TABLE\s+)?([a-zA-Z0-9_.]+)",
    # CREATE OR REPLACE TABLE schema.table
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([a-zA-Z0-9_.]+)",
    # DeltaTable.forName(..., "schema.table")
    r"DeltaTable\.forName\([^,]+,\s*['\"]([a-zA-Z0-9_.]+)['\"]",
]

# Tables that are NOT real targets even if they appear in the patterns
IGNORED_TABLES = {
    "current_date",
    "current_timestamp",
    "DELTA",
    "TABLE",
}

# Safety limits
MAX_NOTEBOOK_CHARS = 200_000  # ~50k tokens approx, margin for Opus to respond

READ_TABLE_PATTERNS = [
    r"spark\.read\.table\(\s*['\"]([a-zA-Z0-9_.]+)['\"]",
    r"(?:FROM|JOIN)\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)(?:\s|$)",
    r"spark\.table\(\s*['\"]([a-zA-Z0-9_.]+)['\"]",
]


def extract_target_tables(source: str) -> List[str]:
    """Extracts all output tables (schema.table) detected in the source, ignoring comments."""
    non_comment_lines = [
        line for line in source.splitlines()
        if not line.lstrip().startswith("#") and not line.lstrip().startswith("--")
    ]
    searchable = "\n".join(non_comment_lines)

    tables = set()
    for pattern in OUTPUT_TABLE_PATTERNS:
        for match in re.findall(pattern, searchable, re.IGNORECASE):
            if "." in match and match not in IGNORED_TABLES:
                tables.add(match)
    return sorted(tables)


def extract_read_tables(source: str) -> set:
    """Extracts read tables (schema.table) from the source, ignoring comments."""
    non_comment_lines = [
        line for line in source.splitlines()
        if not line.lstrip().startswith("#") and not line.lstrip().startswith("--")
    ]
    searchable = "\n".join(non_comment_lines)
    tables = set()
    for pattern in READ_TABLE_PATTERNS:
        for match in re.findall(pattern, searchable, re.IGNORECASE):
            if "." in match and match not in IGNORED_TABLES:
                tables.add(match.lower())
    return tables


def detect_side_effects(source: str) -> List[str]:
    """Detects external side effect patterns."""
    effects = []
    seen_labels = set()
    for pattern, label in SIDE_EFFECT_PATTERNS:
        if re.search(pattern, source, re.IGNORECASE) and label not in seen_labels:
            effects.append(label)
            seen_labels.add(label)
    return effects


def classify_notebook(source: str) -> Dict:
    """
    Classifies a notebook into tier green/yellow/red.

    Returns:
        {
            "tier": "green" | "yellow" | "red",
            "target_tables": [...],
            "side_effects": [...],
            "reason": "..."
        }
    """
    tables = extract_target_tables(source)
    side_effects = detect_side_effects(source)

    if len(source) > MAX_NOTEBOOK_CHARS:
        return {
            "tier": "yellow",
            "target_tables": tables,
            "side_effects": side_effects,
            "reason": f"notebook too large ({len(source):,} chars > {MAX_NOTEBOOK_CHARS:,}). "
                      "Generates proposal without automatic validation."
        }

    if side_effects:
        return {
            "tier": "yellow",
            "target_tables": tables,
            "side_effects": side_effects,
            "reason": f"has side effects: {', '.join(side_effects)}. "
                      "Generates proposal but does NOT validate automatically."
        }

    if not tables:
        return {
            "tier": "yellow",
            "target_tables": [],
            "side_effects": [],
            "reason": "no output tables detected. Generates proposal without automatic validation."
        }

    # Detect self-reference: tables that are both read AND written
    read_tables = extract_read_tables(source)
    self_ref = sorted({t for t in tables if t.lower() in read_tables})
    if self_ref:
        return {
            "tier": "yellow",
            "target_tables": tables,
            "side_effects": [],
            "self_reference": True,
            "reason": f"self-reference detected: notebook reads AND writes {', '.join(self_ref)}. "
                      "Automatic validation cannot run correctly on this pattern."
        }

    return {
        "tier": "green",
        "target_tables": tables,
        "side_effects": [],
        "reason": f"Automatically validatable over {len(tables)} Delta table(s)."
    }


if __name__ == "__main__":
    # Smoke test with a representative snippet
    sample = """
    # Databricks notebook source
    df = spark.read.table("dwh_analytics.dl_clientes")
    df.write.mode("overwrite").saveAsTable("dwh_analytics.dl_clientes_stock")

    spark.sql("MERGE INTO dwh_analytics.dl_transacciones USING updates ON ...")
    """
    print(classify_notebook(sample))
