"""
Phase 0.1a — Combination Matrix Draft Generator.

Reads normalized_thresholds_openai.csv and produces combination_matrix_draft.csv
with one row per (canonical_variable × subcategory) combination.

For each group, infers applicability rules from the data:
  - accumulation_window_applicable: threshold_text matches "in/within/over X hours/days/weeks"
  - persistence_applicable: threshold_text contains "consecutive"
  - probability_applicable: any row in the group has a non-null probability_value
  - lead_time_applicable: any row in the group has a non-null lead_time_value

Outputs:
  ui_normalization_output/combination_matrix_draft.csv

Usage (standalone):
    python -m scripts.ui_normalization.step0_matrix_generator
    # or with explicit input/output:
    python -m scripts.ui_normalization.step0_matrix_generator --input path/to/normalized.csv
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    from .config import (
        NORMALIZED_CSV_OPENAI,
        COMBINATION_MATRIX_DRAFT_CSV,
    )
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        NORMALIZED_CSV_OPENAI,
        COMBINATION_MATRIX_DRAFT_CSV,
    )

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex patterns for applicability inference
# ---------------------------------------------------------------------------

# Matches "in 72 hours", "within 3 days", "over 2 weeks", "during 1 month",
# "accumulated over 48 hours", "in a week", "in a day"
_ACCUM_WINDOW_RE = re.compile(
    r"\b(?:in|within|over|during|across)\s+(?:the\s+)?(?:a\s+)?"
    r"(?:(?P<val>\d+(?:\.\d+)?)\s*)?(?P<unit>hours?|days?|weeks?|months?)\b",
    re.IGNORECASE,
)

# Matches "3 consecutive days", "for 3 consecutive hours", "2 days in a row"
_PERSISTENCE_RE = re.compile(
    r"(?:(?P<val>\d+)\s+consecutive\s+(?P<unit>hours?|days?)"
    r"|(?P<val2>\d+)\s+(?P<unit2>hours?|days?)\s+in\s+a\s+row)",
    re.IGNORECASE,
)

# Canonical variables that are inherently forecast-based (probability always possible)
_FORECAST_BASED_CANONICALS = {
    "Hydrological Flow",
    "Precipitation",
    "Wind",
    "Temperature",
    "Alert/Warning Status",
}

# Canonicals where accumulation window is physically meaningful
_ACCUM_WINDOW_CANONICALS = {
    "Precipitation",
}

# Canonicals where persistence is physically meaningful
_PERSISTENCE_CANONICALS = {
    "Temperature",
    "Hydrological Flow",
    "Wind",
    "Precipitation",
    "Alert/Warning Status",
}


def _normalize_unit(raw: str) -> str:
    """Normalise a matched unit string to its plural, canonical form."""
    s = raw.lower().rstrip(".")
    mapping = {
        "hour": "hours",
        "day": "days",
        "week": "weeks",
        "month": "months",
    }
    return mapping.get(s, s if s.endswith("s") else s + "s")


def _detect_accumulation_window(texts: pd.Series) -> tuple[str, str]:
    """Return (applicable_flag, pipe_separated_units) for the group."""
    units: set[str] = set()
    for text in texts.dropna():
        for m in _ACCUM_WINDOW_RE.finditer(str(text)):
            units.add(_normalize_unit(m.group("unit")))
    if units:
        return "yes", "|".join(sorted(units))
    return "no", "n/a"


def _detect_persistence(texts: pd.Series) -> tuple[str, str]:
    """Return (applicable_flag, pipe_separated_units) for the group."""
    units: set[str] = set()
    for text in texts.dropna():
        m = _PERSISTENCE_RE.search(str(text))
        if m:
            raw_unit = m.group("unit") or m.group("unit2") or "days"
            units.add(_normalize_unit(raw_unit))
    if units:
        return "yes", "|".join(sorted(units))
    return "no", "n/a"


def _probability_flag(group: pd.DataFrame, canonical: str) -> str:
    """Return yes/optional/no for probability_applicable."""
    has_prob = group["probability_value"].notna().any()
    if has_prob:
        return "yes"
    if canonical in _FORECAST_BASED_CANONICALS:
        return "optional"
    return "no"


def _lead_time_flag(group: pd.DataFrame, canonical: str) -> str:
    """Return yes/optional/no for lead_time_applicable."""
    has_lt = group["lead_time_value"].notna().any() and (group["lead_time_value"] > 0).any()
    if has_lt:
        return "yes"
    if canonical in _FORECAST_BASED_CANONICALS:
        return "optional"
    return "no"


def _lead_time_units(group: pd.DataFrame) -> str:
    """Collect distinct timeframe_unit values seen for this group."""
    units = (
        group["timeframe_unit"]
        .dropna()
        .astype(str)
        .str.lower()
        .str.strip()
        .unique()
    )
    valid = [u for u in units if u and u not in ("nan", "none", "")]
    return "|".join(sorted(valid)) if valid else "n/a"


def _hazard_types(group: pd.DataFrame) -> str:
    """Collect distinct hazard_type values for the group."""
    col = "hazard_type" if "hazard_type" in group.columns else None
    if col is None:
        return "other"
    vals = (
        group[col]
        .dropna()
        .astype(str)
        .str.lower()
        .str.strip()
        .unique()
    )
    valid = [v for v in vals if v and v not in ("nan", "none", "")]
    return "|".join(sorted(valid)) if valid else "other"


def _example_statement(group: pd.DataFrame) -> str:
    """Return the most informative threshold_text from the group."""
    texts = group["threshold_text"].dropna().astype(str)
    # Prefer longer texts as they carry more context
    if texts.empty:
        return ""
    return texts.iloc[texts.str.len().argmax()]


def _source_eap_count(group: pd.DataFrame) -> int:
    col = "document_id" if "document_id" in group.columns else None
    if col is None:
        return 0
    return group[col].nunique()


def _primary_units(group: pd.DataFrame) -> str:
    """Collect all distinct non-null threshold_unit values, pipe-separated."""
    units = (
        group["threshold_unit"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
    )
    valid = [u for u in units if u and u.lower() not in ("nan", "none", "")]
    return "|".join(sorted(valid)) if valid else "n/a"


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_matrix_draft(
    input_csv: Path,
    output_csv: Path,
) -> pd.DataFrame:
    """
    Read the normalized thresholds CSV, group by (canonical_variable × subcategory),
    infer applicability rules, and write combination_matrix_draft.csv.

    Returns the draft DataFrame.
    """
    logger.info("Reading normalized thresholds from %s", input_csv)
    df = pd.read_csv(input_csv)

    required_cols = {"canonical_variable", "subcategory", "threshold_unit",
                     "threshold_text", "probability_value", "lead_time_value"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Input CSV is missing columns: {missing}")

    # Drop rows without a canonical classification
    df = df[df["canonical_variable"].notna() & (df["canonical_variable"].str.strip() != "")]
    logger.info("Processing %d valid records across %d unique groups",
                len(df),
                df.groupby(["canonical_variable", "subcategory"]).ngroups)

    rows: list[dict] = []
    for (canonical, subcat), grp in df.groupby(
        ["canonical_variable", "subcategory"], sort=True, dropna=False
    ):
        texts = grp["threshold_text"]

        # Accumulation window — check data patterns, then apply domain override
        aw_flag, aw_units = _detect_accumulation_window(texts)
        if canonical in _ACCUM_WINDOW_CANONICALS and aw_flag == "no":
            aw_flag = "conditional"  # may occur in data not yet seen

        # Persistence — check data patterns, then apply domain override
        pers_flag, pers_units = _detect_persistence(texts)
        if canonical in _PERSISTENCE_CANONICALS and pers_flag == "no":
            pers_flag = "conditional"

        rows.append(
            {
                "canonical_variable": canonical,
                "subcategory": subcat,
                "primary_unit": _primary_units(grp),
                "accumulation_window_applicable": aw_flag,
                "accumulation_window_units": aw_units,
                "persistence_applicable": pers_flag,
                "persistence_units": pers_units,
                "probability_applicable": _probability_flag(grp, canonical),
                "lead_time_applicable": _lead_time_flag(grp, canonical),
                "lead_time_units": _lead_time_units(grp),
                # Defaults — LLM enrichment (Phase 0.1b) will refine these
                "geographic_scope_required": "optional",
                "example_statement": _example_statement(grp),
                "hazard_types": _hazard_types(grp),
                "notes": "",
                # Provenance
                "data_source": "auto_inferred",
                "row_count": len(grp),
                "source_eap_count": _source_eap_count(grp),
            }
        )

    draft = pd.DataFrame(rows)
    draft.sort_values(["canonical_variable", "subcategory"], inplace=True)
    draft.reset_index(drop=True, inplace=True)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    draft.to_csv(output_csv, index=False)
    logger.info(
        "Combination matrix draft written → %s  (%d rows)", output_csv, len(draft)
    )
    return draft


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 0.1a — Combination Matrix Draft Generator"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=NORMALIZED_CSV_OPENAI,
        help=f"Path to normalized thresholds CSV (default: {NORMALIZED_CSV_OPENAI})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=COMBINATION_MATRIX_DRAFT_CSV,
        help=f"Path to write combination_matrix_draft.csv (default: {COMBINATION_MATRIX_DRAFT_CSV})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )
    args = _parse_args()
    build_matrix_draft(args.input, args.output)
