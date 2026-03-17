"""
Enhanced UI Schema Aggregator with Variable Frequency Filtering.

This variant of step3_aggregator.py adds filtering capability to only include
the most common forecast variables in the main dropdown lists, with optional
separate handling for rare/special variables.

Usage:
    python step3_aggregator_filtered.py [--llm openai|gemini] [--min-frequency N]
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from .config import NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI, UI_SCHEMA_JSON_OPENAI, UI_SCHEMA_JSON_GEMINI
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI, UI_SCHEMA_JSON_OPENAI, UI_SCHEMA_JSON_GEMINI

logger = logging.getLogger(__name__)


def determine_trigger_logic(row: pd.Series) -> str:
    """Map a normalized record to one of three UI Trigger Logic states."""
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


def _unique_sorted(series: pd.Series) -> list[str]:
    """Return sorted unique non-null string values from a Series."""
    return sorted(
        {str(v).strip() for v in series.dropna() if str(v).strip() not in ("", "None", "nan")}
    )


def _get_variable_frequency(df: pd.DataFrame) -> dict[str, int]:
    """Calculate frequency of each forecast variable."""
    variables = df["forecast_variable"].dropna().astype(str).str.strip()
    variables = variables[variables.notna() & (variables != "") & (variables != "nan")]
    return variables.value_counts().to_dict()


def build_dropdown_masters(
    df: pd.DataFrame,
    min_frequency: int = 1,
    include_frequency: bool = False,
) -> dict[str, list[str] | dict[str, Any]]:
    """
    Build dropdown lists with optional frequency filtering.

    Args:
        df: Normalized DataFrame
        min_frequency: Minimum frequency threshold for forecast_variables
        include_frequency: If True, include frequency metadata in output

    Returns:
        Dictionary with dropdown lists
    """
    # Get all variables and their frequencies
    var_freq = _get_variable_frequency(df)

    # Filter forecast_variables by minimum frequency
    common_vars = sorted([v for v, c in var_freq.items() if c >= min_frequency])
    rare_vars = sorted([v for v, c in var_freq.items() if c < min_frequency])

    dropdown_masters = {
        "forecast_variables": common_vars,
        "sources":            _unique_sorted(df["normalized_source"]),
        "timeframe_units":    _unique_sorted(df["timeframe_unit"]),
        "threshold_units":    _unique_sorted(df["threshold_unit"]),
        "hazard_types":       _unique_sorted(df["hazard_type"]),
        "threshold_operators": _unique_sorted(df["threshold_operator"]),
    }

    if include_frequency:
        dropdown_masters["_metadata"] = {
            "forecast_variables_common": len(common_vars),
            "forecast_variables_rare": len(rare_vars),
            "min_frequency_threshold": min_frequency,
            "variable_frequency": var_freq,
            "rare_variables": rare_vars,
        }

    return dropdown_masters


def build_trigger_logic_mapping(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Return one entry per unique (document_name, statement_key) pair."""
    mapping: list[dict[str, Any]] = []
    grouped = df.groupby(["document_name", "statement_key"], sort=False)

    for (doc_name, stmt_key), group in grouped:
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
    """Convert normalized DataFrame to list of dicts for JSON export."""
    df = df.copy()
    df["trigger_logic"] = df.apply(determine_trigger_logic, axis=1)

    # Drop internal pipeline columns
    drop_cols = [c for c in ["llm_error"] if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Replace NaN with None
    df = df.where(pd.notnull(df), None)

    return df.to_dict(orient="records")


def aggregate_with_filtering(
    normalized_csv: Path,
    output_path: Path,
    min_frequency: int = 4,
) -> dict[str, Any]:
    """
    Load CSV, build UI schema with filtered forecast variables.

    Args:
        normalized_csv: Path to normalized CSV
        output_path: Output JSON path
        min_frequency: Minimum frequency for forecast_variables

    Returns:
        The schema dictionary
    """
    logger.info("Loading normalized CSV from: %s", normalized_csv)
    df = pd.read_csv(normalized_csv)

    # Filter rows with LLM errors
    failed_mask = df["llm_error"].notna() if "llm_error" in df.columns else pd.Series(False, index=df.index)
    failed_count = failed_mask.sum()
    if failed_count:
        logger.warning("%d records had LLM errors and will be excluded.", failed_count)
    df_ok = df[~failed_mask].copy()

    if df_ok.empty:
        raise ValueError("No successfully normalized records found.")

    # Build components
    dropdown_masters   = build_dropdown_masters(df_ok, min_frequency=min_frequency, include_frequency=True)
    trigger_logic_map  = build_trigger_logic_mapping(df_ok)
    records            = build_records(df_ok)

    # Summary statistics
    logic_counts = {}
    for entry in trigger_logic_map:
        lbl = entry["trigger_logic"]
        logic_counts[lbl] = logic_counts.get(lbl, 0) + 1

    schema: dict[str, Any] = {
        "metadata": {
            "description": "EAP Trigger UI Schema with frequency-filtered forecast variables",
            "total_documents":  int(df_ok["document_name"].nunique()),
            "total_statements": int(df_ok.groupby(["document_name", "statement_key"]).ngroups),
            "total_thresholds": int(len(df_ok)),
            "failed_records":   int(failed_count),
            "min_frequency_threshold": min_frequency,
            "trigger_logic_summary": logic_counts,
        },
        "dropdown_masters": dropdown_masters,
        "trigger_logic_mapping": trigger_logic_map,
        "normalized_records": records,
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(schema, fh, indent=2, ensure_ascii=False)

    logger.info("Saved UI schema to %s", output_path)
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

    parser = argparse.ArgumentParser(
        description="Step 3 – UI Schema Aggregation with Frequency Filtering"
    )
    parser.add_argument(
        "--llm",
        choices=["openai", "gemini"],
        default="openai",
        help="Which LLM's output to aggregate",
    )
    parser.add_argument(
        "--min-frequency",
        type=int,
        default=4,
        help="Minimum frequency for forecast_variables to include in main dropdown",
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
        print(f"Normalized CSV not found at {normalized_csv}")
        sys.exit(1)

    schema = aggregate_with_filtering(normalized_csv, output_path, args.min_frequency)
    meta   = schema["metadata"]
    masters = schema["dropdown_masters"]
    metadata_info = masters.get("_metadata", {})

    print(f"\n========== Step 3 Complete (With Filtering) ==========")
    print(f"\nDocuments  : {meta['total_documents']}")
    print(f"Statements : {meta['total_statements']}")
    print(f"Thresholds : {meta['total_thresholds']}")
    print(f"Failed     : {meta['failed_records']}")
    print(f"\nFrequency Filter Settings:")
    print(f"  Min frequency threshold : {args.min_frequency}")
    print(f"  Common variables (>= {args.min_frequency}): {metadata_info.get('forecast_variables_common', 'N/A')}")
    print(f"  Rare/special variables  : {metadata_info.get('forecast_variables_rare', 'N/A')}")
    print(f"\nDropdown masters:")
    for key in ["forecast_variables", "sources", "timeframe_units", "threshold_units"]:
        values = masters.get(key, [])
        if isinstance(values, list):
            print(f"  {key:<25} ({len(values):3d} items)")
    print(f"\nTrigger Logic distribution:")
    for logic, count in meta["trigger_logic_summary"].items():
        print(f"  {logic:<40} {count}")
    print(f"\nOutput -> {output_path}")
