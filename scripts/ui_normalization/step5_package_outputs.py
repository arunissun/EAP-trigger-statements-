"""
Phase 5 — Output Packaging & UI Schema Update.

Produces two final output artifacts:

  1. FORECAST_VARIABLES_REFERENCE.json v2
     Refreshed from the current combination matrix by re-running the
     Phase 0.2/0.3/0.4 update logic (matrix04_update_taxonomy.py).
     The file is re-written only when the matrix has changed since the
     last run (detected by comparing taxonomy_version + generated_date).
     Pass --force-taxonomy-refresh to always re-write.

  2. ui_schema_openai_filtered.json v2
     The existing dropdown / unit / operator schema (produced by step05b)
     is enriched in-place with four new Phase 5 fields:

       accumulation_window_rules
           Per canonical × subcategory: whether the "accumulation window"
           UI field should appear and which time units are valid.

       persistence_rules
           Per canonical × subcategory: whether the "persistence duration"
           UI field should appear and which time units are valid.

       geographic_scope_contract
           The validated geographic-scope enum, which values require a
           free-text label field, and the default value.

       connector_vocabulary
           Complete connector vocabularies for within-statement, cross-
           statement, and inter-phase logic used by the UI logic builder.

       combination_matrix_version
           Locked version string copied from FORECAST_VARIABLES_REFERENCE.json
           so consumers can detect stale caches.

Inputs (must exist):
  FORECAST_VARIABLES_REFERENCE.json   (Phase 0.4 output)
  ui_normalization_output/ui_schema_openai_filtered.json  (Step 3 filtered output)

Outputs (updated in-place):
  FORECAST_VARIABLES_REFERENCE.json           (re-written if matrix changed)
  ui_normalization_output/ui_schema_openai_filtered.json  (Phase 5 fields appended)

Usage (from project root):
    python -m scripts.ui_normalization.step5_package_outputs
    python -m scripts.ui_normalization.step5_package_outputs --force-taxonomy-refresh
    python -m scripts.ui_normalization.step5_package_outputs --skip-taxonomy-refresh
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

# ---------------------------------------------------------------------------
# Config import (works standalone or as package)
# ---------------------------------------------------------------------------
try:
    from .config import (
        FORECAST_VARIABLES_REFERENCE_JSON,
        COMBINATION_MATRIX_ENRICHED_CSV,
        OUTPUT_DIR,
        GEOGRAPHIC_SCOPE_TYPES,
        WITHIN_STATEMENT_CONNECTORS,
        CROSS_STATEMENT_CONNECTORS,
        INTER_PHASE_CONNECTORS,
    )
    from .matrix04_update_taxonomy import update_taxonomy
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        FORECAST_VARIABLES_REFERENCE_JSON,
        COMBINATION_MATRIX_ENRICHED_CSV,
        OUTPUT_DIR,
        GEOGRAPHIC_SCOPE_TYPES,
        WITHIN_STATEMENT_CONNECTORS,
        CROSS_STATEMENT_CONNECTORS,
        INTER_PHASE_CONNECTORS,
    )
    from matrix04_update_taxonomy import update_taxonomy

logger = logging.getLogger(__name__)

# Phase 5 target schema path (the filtered schema produced by Step 3)
UI_SCHEMA_FILTERED_JSON = OUTPUT_DIR / "ui_schema_openai_filtered.json"

# ---------------------------------------------------------------------------
# 5.1 — Verify / refresh FORECAST_VARIABLES_REFERENCE.json
# ---------------------------------------------------------------------------

def refresh_taxonomy(force: bool = False) -> dict:
    """Re-run Phase 0.4 taxonomy update if needed.

    If the taxonomy JSON already exists and the matrix has not changed
    (same taxonomy_version prefix), the existing file is returned without
    re-writing. Pass force=True to always re-write.

    Returns:
        The current taxonomy dict (freshly built or loaded from disk).
    """
    if not COMBINATION_MATRIX_ENRICHED_CSV.exists():
        raise FileNotFoundError(
            f"Combination matrix not found at {COMBINATION_MATRIX_ENRICHED_CSV}. "
            "Run --phase0-matrix-enrich first."
        )

    if not force and FORECAST_VARIABLES_REFERENCE_JSON.exists():
        try:
            existing = json.loads(FORECAST_VARIABLES_REFERENCE_JSON.read_text(encoding="utf-8"))
            # Re-write only when the matrix source annotation has changed
            existing_source = existing.get("source_matrix", "")
            if existing_source == COMBINATION_MATRIX_ENRICHED_CSV.name:
                logger.info(
                    "FORECAST_VARIABLES_REFERENCE.json is current (source: %s) — skipping refresh.",
                    existing_source,
                )
                return existing
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read existing taxonomy JSON (%s); will refresh.", exc)

    logger.info("Refreshing FORECAST_VARIABLES_REFERENCE.json from %s …", COMBINATION_MATRIX_ENRICHED_CSV.name)
    taxonomy = update_taxonomy(source="csv")
    logger.info(
        "  → %d canonicals / %d subcategories written to %s",
        len(taxonomy["canonicals"]),
        sum(len(c["subcategories"]) for c in taxonomy["canonicals"]),
        FORECAST_VARIABLES_REFERENCE_JSON,
    )
    return taxonomy


# ---------------------------------------------------------------------------
# 5.2 helpers — build the four new Phase 5 schema fields
# ---------------------------------------------------------------------------

def _build_applicability_rules(
    taxonomy: dict,
    field: str,               # "accumulation_window" or "persistence"
) -> dict[str, dict[str, dict]]:
    """Build per-canonical × per-subcategory applicability rules for one field.

    Args:
        taxonomy:  The full v2 taxonomy dict from FORECAST_VARIABLES_REFERENCE.json.
        field:     "accumulation_window" or "persistence".

    Returns:
        {
          "Precipitation": {
            "Total rainfall": {"applicable": true, "units": ["hours", "days", "weeks"]},
            "Seasonal total": {"applicable": false}
          },
          ...
        }
    """
    rules: dict[str, dict[str, dict]] = {}

    for canonical in taxonomy.get("canonicals", []):
        can_name = canonical["name"]
        subcat_rules: dict[str, dict] = {}
        for subcat in canonical.get("subcategories", []):
            sub_name = subcat["name"]
            field_block = subcat.get(field, {})
            applicable = field_block.get("applicable", False)
            entry: dict = {"applicable": bool(applicable) if isinstance(applicable, bool) else applicable}
            if applicable and field_block.get("units"):
                entry["units"] = field_block["units"]
            subcat_rules[sub_name] = entry
        if subcat_rules:
            rules[can_name] = subcat_rules

    return rules


def _build_geographic_scope_contract(taxonomy: dict) -> dict:
    """Build the geographic scope contract block.

    Uses GEOGRAPHIC_SCOPE_TYPES from config (validated vocabulary) plus the
    label_required_for list embedded in the taxonomy JSON.

    Returns:
        {
          "enum_values": [...],
          "label_required_for": [...],
          "default": "national",
          "ui_note": "..."
        }
    """
    label_required = taxonomy.get(
        "geographic_scope_label_required_for",
        ["station_gauge", "watershed_basin", "administrative_unit", "count_threshold", "regional"],
    )
    return {
        "enum_values": taxonomy.get("geographic_scope_types", GEOGRAPHIC_SCOPE_TYPES),
        "label_required_for": label_required,
        "default": "national",
        "ui_note": (
            "Always show a geographic scope selector; default = 'national'. "
            "When a value in 'label_required_for' is selected, render a free-text "
            "label field for the specific name (station name, basin name, district, etc.)."
        ),
    }


def _build_connector_vocabulary(taxonomy: dict) -> dict:
    """Build the full connector vocabulary block.

    Prefers values embedded in the taxonomy JSON (written by Phase 0.4);
    falls back to config constants.

    Returns:
        {
          "within_statement": [...],
          "cross_statement": [...],
          "inter_phase": [...]
        }
    """
    embedded = taxonomy.get("connectors", {})
    return {
        "within_statement": embedded.get("within_statement", WITHIN_STATEMENT_CONNECTORS),
        "cross_statement":  embedded.get("cross_statement",  CROSS_STATEMENT_CONNECTORS),
        "inter_phase":      embedded.get("inter_phase",      INTER_PHASE_CONNECTORS),
    }


# ---------------------------------------------------------------------------
# 5.2 — Enrich ui_schema_openai_filtered.json
# ---------------------------------------------------------------------------

def enrich_ui_schema(
    taxonomy: dict,
    schema_path: Path = UI_SCHEMA_FILTERED_JSON,
) -> dict:
    """Read the existing UI schema and add Phase 5 fields.

    The function is idempotent: Phase 5 keys are overwritten on each run so
    re-running after a matrix update automatically refreshes the rules.

    Args:
        taxonomy:    The v2 taxonomy dict (from FORECAST_VARIABLES_REFERENCE.json).
        schema_path: Path to the existing ui_schema_openai_filtered.json.

    Returns:
        The enriched schema dict (also written to disk).
    """
    if not schema_path.exists():
        raise FileNotFoundError(
            f"UI schema not found at {schema_path}. "
            "Run --step3-mode filtered first."
        )

    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    # ── 5.2 Phase 5 fields ────────────────────────────────────────────────
    schema["accumulation_window_rules"] = _build_applicability_rules(taxonomy, "accumulation_window")
    schema["persistence_rules"]          = _build_applicability_rules(taxonomy, "persistence")
    schema["geographic_scope_contract"]  = _build_geographic_scope_contract(taxonomy)
    schema["connector_vocabulary"]        = _build_connector_vocabulary(taxonomy)
    schema["combination_matrix_version"]  = taxonomy.get("taxonomy_version", "unknown")

    schema_path.write_text(
        json.dumps(schema, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("UI schema v2 written → %s", schema_path)

    # Log field summary
    can_count = len(schema["accumulation_window_rules"])
    acc_sub_total = sum(len(v) for v in schema["accumulation_window_rules"].values())
    logger.info(
        "  accumulation_window_rules: %d canonicals / %d subcategories",
        can_count, acc_sub_total,
    )
    pers_sub_total = sum(len(v) for v in schema["persistence_rules"].values())
    logger.info(
        "  persistence_rules: %d canonicals / %d subcategories",
        can_count, pers_sub_total,
    )
    logger.info(
        "  geographic_scope_contract: %d enum values",
        len(schema["geographic_scope_contract"]["enum_values"]),
    )
    logger.info(
        "  connector_vocabulary: within=%d / cross=%d / inter_phase=%d",
        len(schema["connector_vocabulary"]["within_statement"]),
        len(schema["connector_vocabulary"]["cross_statement"]),
        len(schema["connector_vocabulary"]["inter_phase"]),
    )
    logger.info("  combination_matrix_version: %s", schema["combination_matrix_version"])

    return schema


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_phase5_packaging(
    force_taxonomy_refresh: bool = False,
    skip_taxonomy_refresh: bool = False,
) -> tuple[dict, dict]:
    """Run Phase 5: refresh taxonomy + enrich UI schema.

    Args:
        force_taxonomy_refresh: Always re-write FORECAST_VARIABLES_REFERENCE.json.
        skip_taxonomy_refresh:  Skip taxonomy re-write; load from disk as-is.

    Returns:
        Tuple of (taxonomy_dict, enriched_schema_dict).
    """
    # 5.1 — Taxonomy
    if skip_taxonomy_refresh:
        if not FORECAST_VARIABLES_REFERENCE_JSON.exists():
            raise FileNotFoundError(
                f"Taxonomy JSON not found at {FORECAST_VARIABLES_REFERENCE_JSON}. "
                "Run --phase0-taxonomy-update first or remove --skip-taxonomy-refresh."
            )
        taxonomy = json.loads(FORECAST_VARIABLES_REFERENCE_JSON.read_text(encoding="utf-8"))
        logger.info(
            "Taxonomy refresh skipped — loaded from %s (%d canonicals)",
            FORECAST_VARIABLES_REFERENCE_JSON,
            len(taxonomy.get("canonicals", [])),
        )
    else:
        taxonomy = refresh_taxonomy(force=force_taxonomy_refresh)

    # 5.2 — UI schema enrichment
    schema = enrich_ui_schema(taxonomy)

    return taxonomy, schema


# ---------------------------------------------------------------------------
# Standalone entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Phase 5: refresh FORECAST_VARIABLES_REFERENCE.json and enrich UI schema."
    )
    refresh_group = parser.add_mutually_exclusive_group()
    refresh_group.add_argument(
        "--force-taxonomy-refresh",
        action="store_true",
        help="Always re-write FORECAST_VARIABLES_REFERENCE.json even if already current.",
    )
    refresh_group.add_argument(
        "--skip-taxonomy-refresh",
        action="store_true",
        help="Skip taxonomy re-write; load existing JSON from disk.",
    )
    args = parser.parse_args()

    taxonomy, schema = run_phase5_packaging(
        force_taxonomy_refresh=args.force_taxonomy_refresh,
        skip_taxonomy_refresh=args.skip_taxonomy_refresh,
    )

    print(f"Done.")
    print(f"  Taxonomy → {FORECAST_VARIABLES_REFERENCE_JSON}  "
          f"({len(taxonomy['canonicals'])} canonicals)")
    print(f"  UI schema → {UI_SCHEMA_FILTERED_JSON}  "
          f"(version: {schema.get('combination_matrix_version', '?')})")


if __name__ == "__main__":
    main()
