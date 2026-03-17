"""
Step 3 – UI Schema Aggregation & Trigger Logic Mapping.

Reads the normalized CSV and:
  1. Aggregates unique dropdown values for the UI master lists.
  2. Maps each record to a "Trigger Logic" UI state.
  3. Writes ui_schema.json containing:
       - dropdown master lists (forecast_variables, sources, timeframes, units)
       - per-document trigger logic mapping
       - full normalized records (for the UI table / API)

Output: ui_schema.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Config import
# ---------------------------------------------------------------------------
try:
    from .config import NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI, UI_SCHEMA_JSON_OPENAI, UI_SCHEMA_JSON_GEMINI
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI, UI_SCHEMA_JSON_OPENAI, UI_SCHEMA_JSON_GEMINI

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trigger Logic Mapping
# ---------------------------------------------------------------------------

def determine_trigger_logic(row: pd.Series) -> str:
    """
    Map a normalized record to one of three UI Trigger Logic states.

    Rules (in priority order):
      1. is_conditional=True
             → "Conditional (Sequential / AND)"
      2. total_thresholds > 1 AND is_conditional=False
             → "Conditional (Either / OR)"
      3. Fallback (single threshold, not conditional)
             → "Single"
    """
    is_cond        = bool(row.get("is_conditional", False))
    act_type       = str(row.get("activation_type", "")).lower()
    total_thresh   = int(row.get("total_thresholds", 1))

    if is_cond:
        return "Conditional (Sequential / AND)"

    if total_thresh > 1 and not is_cond:
        return "Conditional (Either / OR)"

    if "single" in act_type and not is_cond:
        return "Single"

    return "Single"


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def _unique_sorted(series: pd.Series) -> list[str]:
    """Return sorted unique non-null string values from a Series."""
    return sorted(
        {str(v).strip() for v in series.dropna() if str(v).strip() not in ("", "None", "nan")}
    )


def build_dropdown_masters(df: pd.DataFrame) -> dict[str, list[str]]:
    """
    Build the four master dropdown lists from normalized data.
    """
    return {
        "forecast_variables": _unique_sorted(df["forecast_variable"]),
        "sources":            _unique_sorted(df["normalized_source"]),
        "timeframe_units":    _unique_sorted(df["timeframe_unit"]),
        "threshold_units":    _unique_sorted(df["threshold_unit"]),
        "hazard_types":       _unique_sorted(df["hazard_type"]),
        "threshold_operators": _unique_sorted(df["threshold_operator"]),
    }


def build_trigger_logic_mapping(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Return one entry per unique (document_name, statement_key) pair
    with the resolved trigger logic label.
    """
    mapping: list[dict[str, Any]] = []
    grouped = df.groupby(["document_name", "statement_key"], sort=False)

    for (doc_name, stmt_key), group in grouped:
        # Use the first row for statement-level metadata
        first = group.iloc[0]
        logic = determine_trigger_logic(first)

        mapping.append({
            "document_name":    doc_name,
            "hazard_type":      first.get("hazard_type", ""),
            "statement_key":    stmt_key,
            "activation_type":  first.get("activation_type", ""),
            "is_conditional":   bool(first.get("is_conditional", False)),
            "total_thresholds": int(first.get("total_thresholds", 1)),
            "trigger_logic":    logic,
        })

    return mapping


def build_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Convert the normalized DataFrame to a list of dicts for JSON export.
    Drops internal pipeline columns and adds trigger_logic per row.
    """
    df = df.copy()
    df["trigger_logic"] = df.apply(determine_trigger_logic, axis=1)

    # Drop columns that are only useful during pipeline processing
    drop_cols = [c for c in ["llm_error"] if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Replace NaN with None for clean JSON serialisation
    df = df.where(pd.notnull(df), None)

    return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Main aggregation function
# ---------------------------------------------------------------------------

def aggregate(
    normalized_csv: Path,
    output_path: Path,
) -> dict[str, Any]:
    """
    Load the normalized CSV, build the UI schema, and write ui_schema.json.

    Args:
        normalized_csv: Path to the normalized CSV (OpenAI or Gemini)
        output_path: Path for the output JSON schema

    Returns the schema dict.
    """
    logger.info("Loading normalized CSV from: %s", normalized_csv)
    df = pd.read_csv(normalized_csv)

    # Filter out rows where LLM normalization failed
    failed_mask = df["llm_error"].notna() if "llm_error" in df.columns else pd.Series(False, index=df.index)
    failed_count = failed_mask.sum()
    if failed_count:
        logger.warning("%d records had LLM errors and will be excluded from the schema.", failed_count)
    df_ok = df[~failed_mask].copy()

    if df_ok.empty:
        raise ValueError("No successfully normalized records found. Check step2 output.")

    # Build components
    dropdown_masters   = build_dropdown_masters(df_ok)
    trigger_logic_map  = build_trigger_logic_mapping(df_ok)
    records            = build_records(df_ok)

    # Summary statistics
    logic_counts = {}
    for entry in trigger_logic_map:
        lbl = entry["trigger_logic"]
        logic_counts[lbl] = logic_counts.get(lbl, 0) + 1

    schema: dict[str, Any] = {
        "metadata": {
            "description": "EAP Trigger UI Schema – master dropdown lists and normalized records",
            "total_documents":  int(df_ok["document_name"].nunique()),
            "total_statements": int(df_ok.groupby(["document_name", "statement_key"]).ngroups),
            "total_thresholds": int(len(df_ok)),
            "failed_records":   int(failed_count),
            "trigger_logic_summary": logic_counts,
        },
        "dropdown_masters": dropdown_masters,
        "trigger_logic_mapping": trigger_logic_map,
        "normalized_records": records,
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(schema, fh, indent=2, ensure_ascii=False)

    logger.info("Saved UI schema → %s", output_path)
    return schema


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    parser = argparse.ArgumentParser(description="Step 3 – UI Schema Aggregation")
    parser.add_argument(
        "--llm",
        choices=["openai", "gemini"],
        default="openai",
        help="Which LLM's output to aggregate: 'openai' or 'gemini'"
    )
    args = parser.parse_args()

    # Select input/output based on LLM choice
    if args.llm == "openai":
        normalized_csv = NORMALIZED_CSV_OPENAI
        output_path = UI_SCHEMA_JSON_OPENAI
    else:
        normalized_csv = NORMALIZED_CSV_GEMINI
        output_path = UI_SCHEMA_JSON_GEMINI

    if not normalized_csv.exists():
        print(f"❌  Normalized CSV not found at {normalized_csv}.")
        print("   Run step2_llm.py --llm {openai|gemini} first.")
        sys.exit(1)

    schema = aggregate(normalized_csv, output_path)
    meta   = schema["metadata"]
    dm     = schema["dropdown_masters"]

    print(f"\n✅  Step 3 complete.")
    print(f"   Documents  : {meta['total_documents']}")
    print(f"   Statements : {meta['total_statements']}")
    print(f"   Thresholds : {meta['total_thresholds']}")
    print(f"   Failed     : {meta['failed_records']}")
    print(f"\n   Trigger Logic distribution:")
    for logic, count in meta["trigger_logic_summary"].items():
        print(f"     {logic:<40} {count}")
    print(f"\n   Dropdown masters:")
    for key, values in dm.items():
        print(f"     {key:<25} ({len(values)} unique): {values[:5]}{'…' if len(values) > 5 else ''}")
    print(f"\n   Output → {output_path}")
