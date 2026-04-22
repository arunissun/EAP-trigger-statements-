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
    from .config import (
        NORMALIZED_CSV_OPENAI,
        NORMALIZED_CSV_GEMINI,
        UI_SCHEMA_JSON_OPENAI,
        UI_SCHEMA_JSON_GEMINI,
        TAXONOMY_PROPOSAL_JSON_OPENAI,
        TAXONOMY_PROPOSAL_JSON_GEMINI,
        FORECAST_VARIABLES_REFERENCE_JSON,
        REQUIRE_APPROVED_TAXONOMY,
        MAX_PRIMARY_CANONICALS,
    )
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        NORMALIZED_CSV_OPENAI,
        NORMALIZED_CSV_GEMINI,
        UI_SCHEMA_JSON_OPENAI,
        UI_SCHEMA_JSON_GEMINI,
        TAXONOMY_PROPOSAL_JSON_OPENAI,
        TAXONOMY_PROPOSAL_JSON_GEMINI,
        FORECAST_VARIABLES_REFERENCE_JSON,
        REQUIRE_APPROVED_TAXONOMY,
        MAX_PRIMARY_CANONICALS,
    )

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


def _load_taxonomy_primary_order(llm: str, allow_unapproved_taxonomy: bool = False) -> list[str]:
    """Load canonical order from approved reference/proposal taxonomy artifacts."""
    source_candidates = [FORECAST_VARIABLES_REFERENCE_JSON]
    if llm == "gemini":
        source_candidates.append(TAXONOMY_PROPOSAL_JSON_GEMINI)
    else:
        source_candidates.append(TAXONOMY_PROPOSAL_JSON_OPENAI)

    for path in source_candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Could not parse taxonomy JSON at %s: %s", path, exc)
            continue

        metadata = payload.get("metadata", {})
        approved_status = str(metadata.get("approval_status", "approved")).strip().lower()
        approved = approved_status in {"approved", "locked", "locked_approved"}
        if REQUIRE_APPROVED_TAXONOMY and not allow_unapproved_taxonomy and not approved:
            raise RuntimeError(
                f"Taxonomy at {path} is not approved. "
                "Approve/lock it first or run with --allow-unapproved-taxonomy for development."
            )

        if isinstance(payload.get("canonical_variables"), list):
            order: list[str] = []
            for item in payload["canonical_variables"]:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "")).strip()
                if name and name.lower() != "other" and name not in order:
                    order.append(name)
            if order:
                logger.info("Loaded canonical order (%d items) from %s", len(order), path)
                return order

        proposal = payload.get("proposal", {})
        proposed = proposal.get("proposed_canonical_variables", [])
        if isinstance(proposed, list):
            order = []
            for item in proposed:
                name = str(item).strip()
                if name and name.lower() != "other" and name not in order:
                    order.append(name)
            if order:
                logger.info("Loaded canonical order (%d items) from %s", len(order), path)
                return order

    logger.warning("No canonical order loaded; falling back to observed canonical values in data.")
    return []


def build_dropdown_masters(
    df: pd.DataFrame,
    canonical_order: list[str] | None = None,
    max_primary_canonicals: int = MAX_PRIMARY_CANONICALS,
    min_frequency: int = 1,
    include_frequency: bool = False,
) -> dict[str, list[str] | dict[str, Any]]:
    """
    Build dropdown lists with 3-layer taxonomy contract and cascading logic.
    """
    observed_canonicals = [
        str(c).strip() for c in df["canonical_variable"].dropna().unique()
        if str(c).strip() and str(c).strip() != "Other"
    ]

    ordered_canonicals: list[str] = []
    canonical_order = canonical_order or []
    for item in canonical_order:
        if item in observed_canonicals and item not in ordered_canonicals:
            ordered_canonicals.append(item)

    for item in sorted(observed_canonicals):
        if item not in ordered_canonicals:
            ordered_canonicals.append(item)

    canonical_vars = ordered_canonicals[:max_primary_canonicals]
    overflow_canonicals = ordered_canonicals[max_primary_canonicals:]

    # Rare variables mapped to Other
    advanced_other = sorted([
        v for v in df[df["canonical_variable"] == "Other"]["original_variable"].dropna().unique() 
        if v.strip()
    ])

    if overflow_canonicals:
        for c in overflow_canonicals:
            if c not in advanced_other:
                advanced_other.append(c)
        advanced_other = sorted(advanced_other)

    secondary_subcategory_dropdown = {}
    canonical_to_units = {}
    canonical_to_operators = {}
    subcategory_to_units_overrides: dict[str, dict[str, list[str]]] = {}
    subcategory_to_operators_overrides: dict[str, dict[str, list[str]]] = {}

    for canonical in canonical_vars:
        subset = df[df["canonical_variable"] == canonical]

        # Subcategories
        subcats = sorted([s for s in subset["subcategory"].dropna().unique() if s.strip()])
        secondary_subcategory_dropdown[canonical] = subcats

        # Units and operators
        canonical_to_units[canonical] = sorted([u for u in subset["threshold_unit"].dropna().unique() if u.strip()])
        canonical_to_operators[canonical] = sorted([o for o in subset["threshold_operator"].dropna().unique() if o.strip()])

        unit_overrides: dict[str, list[str]] = {}
        operator_overrides: dict[str, list[str]] = {}
        for subcat in subcats:
            scoped = subset[subset["subcategory"] == subcat]
            unit_overrides[subcat] = sorted([
                u for u in scoped["threshold_unit"].dropna().unique() if str(u).strip()
            ])
            operator_overrides[subcat] = sorted([
                o for o in scoped["threshold_operator"].dropna().unique() if str(o).strip()
            ])

        subcategory_to_units_overrides[canonical] = unit_overrides
        subcategory_to_operators_overrides[canonical] = operator_overrides

    dropdown_masters = {
        "primary_dropdown_top10": canonical_vars,
        "secondary_subcategory_dropdown": secondary_subcategory_dropdown,
        "advanced_other_search": advanced_other,
        "canonical_to_units": canonical_to_units,
        "canonical_to_operators": canonical_to_operators,
        "subcategory_to_units_overrides": subcategory_to_units_overrides,
        "subcategory_to_operators_overrides": subcategory_to_operators_overrides,
        "sources":            _unique_sorted(df["normalized_source"]),
        "timeframe_units":    _unique_sorted(df["timeframe_unit"]),
        "hazard_types":       _unique_sorted(df["hazard_type"]),
    }

    if include_frequency:
        var_freq = _get_variable_frequency(df)
        dropdown_masters["_metadata"] = {
            "canonical_count": len(canonical_vars),
            "advanced_other_count": len(advanced_other),
            "overflow_canonical_count": len(overflow_canonicals),
            "max_primary_canonicals": max_primary_canonicals,
            "variable_frequency": var_freq,
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
    llm: str = "openai",
    max_primary_canonicals: int = MAX_PRIMARY_CANONICALS,
    allow_unapproved_taxonomy: bool = False,
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

    required_columns = {"canonical_variable", "subcategory", "original_variable"}
    missing = sorted(required_columns - set(df_ok.columns))
    if missing:
        raise ValueError(
            "Step 3 filtered mode requires Phase 2 remapping columns "
            f"{missing}. Rerun Step 2 first so normalized CSV includes canonical_variable, "
            "subcategory, and original_variable."
        )

    canonical_order = _load_taxonomy_primary_order(
        llm=llm,
        allow_unapproved_taxonomy=allow_unapproved_taxonomy,
    )

    # Build components
    dropdown_masters   = build_dropdown_masters(
        df_ok,
        canonical_order=canonical_order,
        max_primary_canonicals=max_primary_canonicals,
        min_frequency=min_frequency,
        include_frequency=True,
    )
    trigger_logic_map  = build_trigger_logic_mapping(df_ok)
    records            = build_records(df_ok)

    # Summary statistics
    logic_counts = {}
    for entry in trigger_logic_map:
        lbl = entry["trigger_logic"]
        logic_counts[lbl] = logic_counts.get(lbl, 0) + 1

    schema: dict[str, Any] = {
        "metadata": {
            "description": "EAP Trigger UI Schema with canonical/subcategory dropdown contract",
            "total_documents":  int(df_ok["document_name"].nunique()),
            "total_statements": int(df_ok.groupby(["document_name", "statement_key"]).ngroups),
            "total_thresholds": int(len(df_ok)),
            "failed_records":   int(failed_count),
            "min_frequency_threshold": min_frequency,
            "max_primary_canonicals": max_primary_canonicals,
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
    parser.add_argument(
        "--max-primary-canonicals",
        type=int,
        default=MAX_PRIMARY_CANONICALS,
        help=f"Maximum canonical variables shown in primary dropdown (default: {MAX_PRIMARY_CANONICALS})",
    )
    parser.add_argument(
        "--allow-unapproved-taxonomy",
        action="store_true",
        help="Allow building schema from non-approved taxonomy artifacts (development mode).",
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

    schema = aggregate_with_filtering(
        normalized_csv,
        output_path,
        llm=args.llm,
        max_primary_canonicals=args.max_primary_canonicals,
        allow_unapproved_taxonomy=args.allow_unapproved_taxonomy,
        min_frequency=args.min_frequency,
    )
    meta   = schema["metadata"]
    masters = schema["dropdown_masters"]
    metadata_info = masters.get("_metadata", {})

    print(f"\n========== Step 3 Complete (With Filtering) ==========")
    print(f"\nDocuments  : {meta['total_documents']}")
    print(f"Statements : {meta['total_statements']}")
    print(f"Thresholds : {meta['total_thresholds']}")
    print(f"Failed     : {meta['failed_records']}")
    print(f"\nFrequency Filter Settings:")
    print(f"  Min frequency threshold : {args.min_frequency} (Note: Now using Taxonomy mapping instead of frequency filter for primary variables)")
    print(f"  Max primary canonicals  : {args.max_primary_canonicals}")
    print(f"  Canonical variables     : {metadata_info.get('canonical_count', 'N/A')}")
    print(f"  Advanced/Other variables: {metadata_info.get('advanced_other_count', 'N/A')}")
    print(f"\nDropdown masters:")
    for key in ["primary_dropdown_top10", "advanced_other_search", "sources", "timeframe_units"]:
        values = masters.get(key, [])
        if isinstance(values, list):
            print(f"  {key:<25} ({len(values):3d} items)")
    print(f"\nTrigger Logic distribution:")
    for logic, count in meta["trigger_logic_summary"].items():
        print(f"  {logic:<40} {count}")
    print(f"\nOutput -> {output_path}")
