"""
Phase 0.2 / 0.3 / 0.4 — Update FORECAST_VARIABLES_REFERENCE.json to v2 structure.

Reads the approved combination matrix (combination_matrix_enriched.csv or
combination_matrix_v1.xlsx) and rewrites FORECAST_VARIABLES_REFERENCE.json with:

  - A canonicals array (one entry per canonical_variable) whose subcategories carry
    structured field-applicability rules derived directly from the matrix rows.
    (Phase 0.2 — canonical variable taxonomy)

  - A geographic_scope_types array with the 6 validated scope type enum values.
    (Phase 0.3 — geographic scope field vocabulary)

  - A connectors dict with within_statement, cross_statement, and inter_phase lists.
    (Phase 0.4 — statement connector vocabulary)

The output replaces the old tier-based v1 JSON and becomes the contract used by:
  - Phase 1 enhanced extraction (step02_normalize.py) — canonical + scope enums
  - Phase 3 connector classifier (step2c_connector_classifier.py) — connector vocab
  - Phase 5 UI schema packaging — canonical hierarchy for the frontend

Usage (from project root):
    python -m scripts.ui_normalization.matrix04_update_taxonomy
    python -m scripts.ui_normalization.matrix04_update_taxonomy --source xlsx
    python -m scripts.ui_normalization.matrix04_update_taxonomy --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from .config import (
        COMBINATION_MATRIX_ENRICHED_CSV,
        COMBINATION_MATRIX_XLSX,
        FORECAST_VARIABLES_REFERENCE_JSON,
        GEOGRAPHIC_SCOPE_TYPES,
        WITHIN_STATEMENT_CONNECTORS,
        CROSS_STATEMENT_CONNECTORS,
        INTER_PHASE_CONNECTORS,
    )
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        COMBINATION_MATRIX_ENRICHED_CSV,
        COMBINATION_MATRIX_XLSX,
        FORECAST_VARIABLES_REFERENCE_JSON,
        GEOGRAPHIC_SCOPE_TYPES,
        WITHIN_STATEMENT_CONNECTORS,
        CROSS_STATEMENT_CONNECTORS,
        INTER_PHASE_CONNECTORS,
    )

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BOOL_MAP = {
    "yes": True,
    "no": False,
    "optional": "optional",
    "conditional": "conditional",
    "not_applicable": False,
    "not applicable": False,
}


def _applicable(val: Any) -> bool | str:
    """Convert matrix applicability string to True / False / 'optional'."""
    if pd.isna(val):
        return False
    s = str(val).strip().lower()
    return _BOOL_MAP.get(s, False)


def _units_list(val: Any) -> list[str]:
    """Split a pipe-separated unit string into a sorted deduplicated list.
    Returns [] when empty / NaN."""
    if pd.isna(val) or str(val).strip() in ("", "n/a", "nan"):
        return []
    parts = [u.strip() for u in str(val).split("|") if u.strip()]
    return sorted(set(parts))


def _primary_units(val: Any) -> list[str]:
    """Split primary_unit pipe string; filter sentinel 'NaN' strings."""
    if pd.isna(val):
        return []
    parts = [u.strip() for u in str(val).split("|") if u.strip().lower() not in ("nan", "")]
    return sorted(set(parts))


def _hazards_list(val: Any) -> list[str]:
    """Split pipe-separated hazard_types into a sorted list."""
    if pd.isna(val):
        return []
    return sorted(set(h.strip() for h in str(val).split("|") if h.strip()))


# ---------------------------------------------------------------------------
# Core builder
# ---------------------------------------------------------------------------

def build_taxonomy_v2(df: pd.DataFrame) -> dict:
    """Convert the combination matrix DataFrame to the v2 taxonomy dict."""

    canonicals: list[dict] = []

    for canonical_name, group in df.groupby("canonical_variable", sort=True):
        subcategories: list[dict] = []

        for _, row in group.iterrows():
            # --- Field applicability rules ---
            acc_applicable = _applicable(row.get("accumulation_window_applicable"))
            acc_units = _units_list(row.get("accumulation_window_units"))

            pers_applicable = _applicable(row.get("persistence_applicable"))
            pers_units = _units_list(row.get("persistence_units"))

            prob_applicable = _applicable(row.get("probability_applicable"))

            lt_applicable = _applicable(row.get("lead_time_applicable"))
            lt_units = _units_list(row.get("lead_time_units"))

            geo_scope = str(row.get("geographic_scope_required", "optional")).strip().lower()
            if geo_scope in ("nan", ""):
                geo_scope = "optional"

            subcat: dict = {
                "name": str(row["subcategory"]).strip(),
                "aliases": [],   # populated manually or in Phase 5 via LLM
                "primary_units": _primary_units(row.get("primary_unit")),
                "accumulation_window": {
                    "applicable": acc_applicable,
                    **({"units": acc_units} if acc_applicable and acc_units else {}),
                },
                "persistence": {
                    "applicable": pers_applicable,
                    **({"units": pers_units} if pers_applicable and pers_units else {}),
                },
                "probability": {
                    "applicable": prob_applicable,
                },
                "lead_time": {
                    "applicable": lt_applicable,
                    **({"units": lt_units} if lt_applicable and lt_units else {}),
                },
                "geographic_scope": geo_scope,
                "hazard_types": _hazards_list(row.get("hazard_types")),
            }

            # Include example_statement when present
            ex = row.get("example_statement")
            if not pd.isna(ex) and str(ex).strip():
                subcat["example_statement"] = str(ex).strip()

            # Include notes when present
            notes = row.get("notes")
            if not pd.isna(notes) and str(notes).strip():
                subcat["notes"] = str(notes).strip()

            subcategories.append(subcat)

        canonicals.append({
            "name": str(canonical_name).strip(),
            "subcategories": subcategories,
        })

    return {
        "taxonomy_version": f"{date.today().strftime('%Y-%m')}-v2",
        "generated_date": date.today().isoformat(),
        "description": (
            "Canonical variable taxonomy for EAP Trigger UI. "
            "Generated from approved combination matrix (Phase 0.1c). "
            "Includes geographic scope types (Phase 0.3) and connector "
            "vocabulary (Phase 0.4)."
        ),
        "source_matrix": "combination_matrix_enriched.csv",
        "canonicals": canonicals,
        "geographic_scope_types": GEOGRAPHIC_SCOPE_TYPES,
        "geographic_scope_label_required_for": ["station_gauge", "watershed_basin",
                                                 "administrative_unit", "count_threshold",
                                                 "regional"],
        "connectors": {
            "within_statement": WITHIN_STATEMENT_CONNECTORS,
            "cross_statement": CROSS_STATEMENT_CONNECTORS,
            "inter_phase": INTER_PHASE_CONNECTORS,
        },
    }


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def _load_matrix(source: str = "csv") -> pd.DataFrame:
    """Load the combination matrix from CSV (default) or from the XLSX Sheet1."""
    if source == "xlsx":
        if not COMBINATION_MATRIX_XLSX.exists():
            raise FileNotFoundError(f"XLSX matrix not found: {COMBINATION_MATRIX_XLSX}")
        df = pd.read_excel(COMBINATION_MATRIX_XLSX, sheet_name="Matrix")
        logger.info("Loaded %d rows from %s (Sheet: Matrix)", len(df), COMBINATION_MATRIX_XLSX)
    else:
        if not COMBINATION_MATRIX_ENRICHED_CSV.exists():
            raise FileNotFoundError(
                f"Enriched matrix CSV not found: {COMBINATION_MATRIX_ENRICHED_CSV}\n"
                "Run --phase0-matrix-enrich first."
            )
        df = pd.read_csv(COMBINATION_MATRIX_ENRICHED_CSV)
        logger.info("Loaded %d rows from %s", len(df), COMBINATION_MATRIX_ENRICHED_CSV)
    return df


def update_taxonomy(
    source: str = "csv",
    output_path: Path | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Build the v2 taxonomy dict and write it to FORECAST_VARIABLES_REFERENCE.json.

    Args:
        source:      'csv' (default) or 'xlsx' — which matrix file to read.
        output_path: Override the default output path.
        dry_run:     If True, print the JSON but do not write the file.

    Returns:
        The taxonomy dict.
    """
    df = _load_matrix(source)
    taxonomy = build_taxonomy_v2(df)

    out = output_path or FORECAST_VARIABLES_REFERENCE_JSON

    if dry_run:
        print(json.dumps(taxonomy, indent=2, ensure_ascii=False))
        logger.info("Dry-run: JSON printed, no file written.")
    else:
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(taxonomy, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        logger.info(
            "v2 taxonomy written → %s  (%d canonicals, %d subcategories total)",
            out,
            len(taxonomy["canonicals"]),
            sum(len(c["subcategories"]) for c in taxonomy["canonicals"]),
        )

    return taxonomy


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=["csv", "xlsx"],
        default="csv",
        help="Which matrix file to read: 'csv' (default) or 'xlsx' (Sheet: Matrix)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Override output JSON path (default: FORECAST_VARIABLES_REFERENCE.json at project root)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the generated JSON to stdout without writing the file.",
    )
    args = parser.parse_args()

    update_taxonomy(source=args.source, output_path=args.output, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
