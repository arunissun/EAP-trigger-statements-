"""
EAP UI Normalization Pipeline – End-to-End Orchestrator.

Usage (from project root):
    python -m scripts.ui_normalization.run_pipeline [OPTIONS]

Options:
    --step1-only            Run only Step 1 (explode JSON)
    --step2-only            Run only Step 2 (LLM normalization, requires step1 output)
    --step3-only            Run only Step 3 (aggregate UI schema, requires step2 output)
    --skip-step2            Run Step 1 + Step 3, skipping LLM calls (useful for testing)
    --step3-mode            Step 3 aggregator mode: standard|filtered (default: standard)
    --min-frequency         Minimum frequency used by filtered Step 3 mode (default: 4)
    --limit N               Only normalize the first N records (for quick testing)
    --phase0-matrix-draft   Phase 0.1a: generate combination_matrix_draft.csv
    --phase0-matrix-enrich  Phase 0.1b: LLM-enrich combination matrix
    --phase0-matrix-export  Phase 0.1c: consolidate + export combination_matrix_v1.xlsx
    --phase0-taxonomy-update  Phase 0.2/0.3/0.4: regenerate FORECAST_VARIABLES_REFERENCE.json v2
    --phase1-enhanced       Phase 1: re-run extraction with 18-field enhanced prompt
                            (adds geographic scope, accumulation window, persistence,
                            is_observational, referenced_eap_id, connectors).
                            Output: normalized_thresholds_v2_openai.csv
    --phase2-matching       Phase 2: LLM taxonomy matching against combination matrix.
                            Two-pass (deterministic + LLM); builds combination_path per
                            record. Output: taxonomy_match_results_v2.csv +
                            out_of_matrix_review.csv
    --phase3-connectors     Phase 3: Connector classification. Output: connector_map.json
    --phase4-export         Phase 4: Export expert_review_package.xlsx (three sheets).
                            No LLM calls. Requires Phase 2 output; Phase 3 optional.
    --phase5-package        Phase 5: Output packaging.
                            (1) Refresh FORECAST_VARIABLES_REFERENCE.json from matrix.
                            (2) Enrich ui_schema_openai_filtered.json with
                                accumulation_window_rules, persistence_rules,
                                geographic_scope_contract, connector_vocabulary,
                                combination_matrix_version. No LLM calls.
                            Options: --force-taxonomy-refresh | --skip-taxonomy-refresh
    --help                  Show this message

Without options, runs all three steps in sequence.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Allow running as `python run_pipeline.py` from inside the folder too
# ---------------------------------------------------------------------------
_HERE = Path(__file__).parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
if str(_HERE.parent) not in sys.path:
    sys.path.insert(0, str(_HERE.parent))

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("run_pipeline")


# ---------------------------------------------------------------------------
# Import pipeline steps
# ---------------------------------------------------------------------------
try:
    from .step01_explode  import explode_json, save_exploded
    from .step02_normalize       import normalize_dataframe, normalize_dataframe_enhanced
    from .step03_prep_taxonomy_input import export_phase0_flattened
    from .step04_propose_taxonomy import run_phase1_taxonomy
    from .step05_aggregate_schema import aggregate
    from .step05b_aggregate_filtered import aggregate_with_filtering
    from .matrix01_draft_generator import build_matrix_draft
    from .matrix02_llm_enrich import enrich_matrix
    from .matrix03_export_excel import consolidate_and_export as export_matrix_excel
    from .matrix04_update_taxonomy import update_taxonomy as update_taxonomy_v2
    from .step2b_matrix_matcher import run_phase2_matching
    from .step2c_connector_classifier import run_phase3_connector_classification
    from .export_review_artifact import export_review_package as run_phase4_export
    from .step5_package_outputs import run_phase5_packaging
    from .config import (
        INPUT_JSON, EXPLODED_CSV,
        NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI,
        NORMALIZED_V2_CSV_OPENAI, NORMALIZED_V2_CSV_GEMINI,
        UI_SCHEMA_JSON_OPENAI, UI_SCHEMA_JSON_GEMINI,
        OUTPUT_DIR,
        PHASE0_FLATTENED_CSV_OPENAI, PHASE0_FLATTENED_CSV_GEMINI,
        TAXONOMY_PROPOSAL_JSON_OPENAI, TAXONOMY_PROPOSAL_JSON_GEMINI,
        TAXONOMY_QUALITY_JSON_OPENAI, TAXONOMY_QUALITY_JSON_GEMINI,
        TAXONOMY_REVIEW_MD_OPENAI, TAXONOMY_REVIEW_MD_GEMINI,
        PHASE1_CANONICAL_TARGET, PHASE1_COVERAGE_TARGET,
        PHASE1_MIN_SUPPORT_RECORDS, PHASE1_MAX_ALIAS_OVERLAP,
        PHASE1_MAX_ITERATIONS,
        MAX_PRIMARY_CANONICALS,
        COMBINATION_MATRIX_DRAFT_CSV,
        COMBINATION_MATRIX_ENRICHED_CSV,
        COMBINATION_MATRIX_XLSX,
        FORECAST_VARIABLES_REFERENCE_JSON,
        TAXONOMY_MATCH_RESULTS_CSV,
        OUT_OF_MATRIX_REVIEW_CSV,
        CONNECTOR_MAP_JSON,
        EXPERT_REVIEW_XLSX,
        UI_SCHEMA_OPENAI_FILTERED_JSON,
    )
except ImportError:
    from step01_explode   import explode_json, save_exploded
    from step02_normalize        import normalize_dataframe, normalize_dataframe_enhanced
    from step03_prep_taxonomy_input import export_phase0_flattened
    from step04_propose_taxonomy import run_phase1_taxonomy
    from step05_aggregate_schema import aggregate
    from step05b_aggregate_filtered import aggregate_with_filtering
    from matrix01_draft_generator import build_matrix_draft
    from matrix02_llm_enrich import enrich_matrix
    from matrix03_export_excel import consolidate_and_export as export_matrix_excel
    from matrix04_update_taxonomy import update_taxonomy as update_taxonomy_v2
    from step2b_matrix_matcher import run_phase2_matching
    from step2c_connector_classifier import run_phase3_connector_classification
    from export_review_artifact import export_review_package as run_phase4_export
    from step5_package_outputs import run_phase5_packaging
    from config import (
        INPUT_JSON, EXPLODED_CSV,
        NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI,
        NORMALIZED_V2_CSV_OPENAI, NORMALIZED_V2_CSV_GEMINI,
        UI_SCHEMA_JSON_OPENAI, UI_SCHEMA_JSON_GEMINI,
        OUTPUT_DIR,
        PHASE0_FLATTENED_CSV_OPENAI, PHASE0_FLATTENED_CSV_GEMINI,
        TAXONOMY_PROPOSAL_JSON_OPENAI, TAXONOMY_PROPOSAL_JSON_GEMINI,
        TAXONOMY_QUALITY_JSON_OPENAI, TAXONOMY_QUALITY_JSON_GEMINI,
        TAXONOMY_REVIEW_MD_OPENAI, TAXONOMY_REVIEW_MD_GEMINI,
        PHASE1_CANONICAL_TARGET, PHASE1_COVERAGE_TARGET,
        PHASE1_MIN_SUPPORT_RECORDS, PHASE1_MAX_ALIAS_OVERLAP,
        PHASE1_MAX_ITERATIONS,
        MAX_PRIMARY_CANONICALS,
        COMBINATION_MATRIX_DRAFT_CSV,
        COMBINATION_MATRIX_ENRICHED_CSV,
        COMBINATION_MATRIX_XLSX,
        FORECAST_VARIABLES_REFERENCE_JSON,
        TAXONOMY_MATCH_RESULTS_CSV,
        OUT_OF_MATRIX_REVIEW_CSV,
        CONNECTOR_MAP_JSON,
        EXPERT_REVIEW_XLSX,
        UI_SCHEMA_OPENAI_FILTERED_JSON,
    )

import pandas as pd


# ---------------------------------------------------------------------------
# Step runners
# ---------------------------------------------------------------------------

def run_step1() -> pd.DataFrame:
    logger.info("=" * 60)
    logger.info("STEP 1 – Deep Parsing & Exploding JSON")
    logger.info("=" * 60)
    t0 = time.time()
    df = explode_json(INPUT_JSON)
    save_exploded(df, EXPLODED_CSV)
    logger.info("Step 1 done in %.1fs  →  %d records", time.time() - t0, len(df))
    return df


def run_step2(
    df: pd.DataFrame,
    limit: int | None = None,
    llm_choice: str = "both",
    allow_unapproved_taxonomy: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Run Step 2 normalization.
    
    Args:
        df: Exploded DataFrame
        limit: Limit number of records (for testing)
        llm_choice: "openai", "gemini", or "both" (default)
    
    Returns:
        Tuple of (openai_df, gemini_df) - one or both may be None
    """
    logger.info("=" * 60)
    logger.info("STEP 2 – LLM Normalization")
    logger.info("=" * 60)
    
    if limit:
        logger.info("  ⚠️  --limit %d: only normalizing first %d records.", limit, limit)
        df = df.head(limit)
    
    openai_df = None
    gemini_df = None
    
    if llm_choice in ["openai", "both"]:
        logger.info("→ Running Azure OpenAI normalization...")
        t0 = time.time()
        openai_df = normalize_dataframe(
            df,
            llm_choice="openai",
            allow_unapproved_taxonomy=allow_unapproved_taxonomy,
        )
        errors = openai_df["llm_error"].notna().sum() if "llm_error" in openai_df.columns else 0
        logger.info("  OpenAI done in %.1fs  →  %d records  (%d errors)", time.time() - t0, len(openai_df), errors)
    
    if llm_choice in ["gemini", "both"]:
        logger.info("→ Running Gemini normalization...")
        t0 = time.time()
        gemini_df = normalize_dataframe(
            df,
            llm_choice="gemini",
            allow_unapproved_taxonomy=allow_unapproved_taxonomy,
        )
        errors = gemini_df["llm_error"].notna().sum() if "llm_error" in gemini_df.columns else 0
        logger.info("  Gemini done in %.1fs  →  %d records  (%d errors)", time.time() - t0, len(gemini_df), errors)
    
    return openai_df, gemini_df


def _step3_output_path(llm_name: str, step3_mode: str) -> Path:
    """Resolve output path for Step 3 based on LLM and aggregation mode."""
    if llm_name == "openai":
        base = UI_SCHEMA_JSON_OPENAI
    else:
        base = UI_SCHEMA_JSON_GEMINI

    if step3_mode == "filtered":
        return base.with_name(f"{base.stem}_filtered{base.suffix}")

    return base


def _phase0_output_path(llm_name: str) -> Path:
    if llm_name == "openai":
        return PHASE0_FLATTENED_CSV_OPENAI
    return PHASE0_FLATTENED_CSV_GEMINI


def _taxonomy_output_paths(llm_name: str) -> tuple[Path, Path, Path]:
    if llm_name == "openai":
        return (
            TAXONOMY_PROPOSAL_JSON_OPENAI,
            TAXONOMY_QUALITY_JSON_OPENAI,
            TAXONOMY_REVIEW_MD_OPENAI,
        )
    return (
        TAXONOMY_PROPOSAL_JSON_GEMINI,
        TAXONOMY_QUALITY_JSON_GEMINI,
        TAXONOMY_REVIEW_MD_GEMINI,
    )


def run_combination_matrix_draft() -> None:
    """Phase 0.1a — Generate combination_matrix_draft.csv from normalized CSV."""
    logger.info("=" * 60)
    logger.info("PHASE 0.1a - Combination Matrix Draft Generator")
    logger.info("=" * 60)
    if not NORMALIZED_CSV_OPENAI.exists():
        raise FileNotFoundError(
            f"OpenAI normalized CSV not found at {NORMALIZED_CSV_OPENAI}. "
            "Run --step2-only --llm openai first."
        )
    t0 = time.time()
    draft = build_matrix_draft(NORMALIZED_CSV_OPENAI, COMBINATION_MATRIX_DRAFT_CSV)
    logger.info(
        "Phase 0.1a done in %.1fs  →  %d rows  →  %s",
        time.time() - t0,
        len(draft),
        COMBINATION_MATRIX_DRAFT_CSV,
    )


def run_combination_matrix_enrichment(llm_choice: str = "openai") -> None:
    """Phase 0.1b — LLM-enrich combination_matrix_draft.csv → combination_matrix_enriched.csv."""
    logger.info("=" * 60)
    logger.info("PHASE 0.1b - LLM Enrichment of Combination Matrix")
    logger.info("=" * 60)
    if not COMBINATION_MATRIX_DRAFT_CSV.exists():
        raise FileNotFoundError(
            f"Draft matrix not found at {COMBINATION_MATRIX_DRAFT_CSV}. "
            "Run --phase0-matrix-draft first."
        )
    # Use openai if both requested (single enrichment pass is sufficient)
    llm = "openai" if llm_choice in ("openai", "both") else "gemini"
    t0 = time.time()
    enriched = run_matrix_enrichment(llm)
    logger.info(
        "Phase 0.1b done in %.1fs  →  %d rows  →  %s",
        time.time() - t0,
        len(enriched),
        COMBINATION_MATRIX_ENRICHED_CSV,
    )


def run_matrix_enrichment(llm: str = "openai") -> "pd.DataFrame":
    """Thin wrapper so run_combination_matrix_enrichment can call enrich_matrix."""
    return enrich_matrix(
        input_csv=COMBINATION_MATRIX_DRAFT_CSV,
        output_csv=COMBINATION_MATRIX_ENRICHED_CSV,
        llm=llm,
    )


def run_combination_matrix_export(no_consolidate: bool = False) -> None:
    """Phase 0.1c — Consolidate Temperature rows + export combination_matrix_v1.xlsx."""
    logger.info("=" * 60)
    logger.info("PHASE 0.1c - Combination Matrix Export to Excel")
    logger.info("=" * 60)
    if not COMBINATION_MATRIX_ENRICHED_CSV.exists():
        raise FileNotFoundError(
            f"Enriched matrix not found at {COMBINATION_MATRIX_ENRICHED_CSV}. "
            "Run --phase0-matrix-enrich first."
        )
    import time as _time
    t0 = _time.time()
    df = export_matrix_excel(
        output_xlsx=COMBINATION_MATRIX_XLSX,
        apply_consolidation=not no_consolidate,
    )
    logger.info(
        "Phase 0.1c done in %.1fs  →  %d rows  →  %s",
        _time.time() - t0,
        len(df),
        COMBINATION_MATRIX_XLSX,
    )


def run_taxonomy_update(source: str = "csv") -> None:
    """Phase 0.2/0.3/0.4 — Regenerate FORECAST_VARIABLES_REFERENCE.json to v2 structure.

    Reads the approved combination matrix and writes the v2 JSON containing:
      - 12-canonical taxonomy with per-subcategory field-applicability rules (Phase 0.2)
      - Geographic scope type enum (Phase 0.3)
      - Statement connector vocabulary (Phase 0.4)
    """
    logger.info("=" * 60)
    logger.info("PHASE 0.2/0.3/0.4 - Update FORECAST_VARIABLES_REFERENCE.json → v2")
    logger.info("=" * 60)

    if source == "xlsx":
        if not COMBINATION_MATRIX_XLSX.exists():
            raise FileNotFoundError(
                f"Approved matrix XLSX not found at {COMBINATION_MATRIX_XLSX}. "
                "Run --phase0-matrix-export first."
            )
    else:
        if not COMBINATION_MATRIX_ENRICHED_CSV.exists():
            raise FileNotFoundError(
                f"Enriched matrix CSV not found at {COMBINATION_MATRIX_ENRICHED_CSV}. "
                "Run --phase0-matrix-enrich first."
            )

    import time as _time
    t0 = _time.time()
    taxonomy = update_taxonomy_v2(source=source)
    n_canonicals = len(taxonomy["canonicals"])
    n_subcats = sum(len(c["subcategories"]) for c in taxonomy["canonicals"])
    logger.info(
        "Phase 0.2/0.3/0.4 done in %.1fs  →  %d canonicals / %d subcategories  →  %s",
        _time.time() - t0,
        n_canonicals,
        n_subcats,
        FORECAST_VARIABLES_REFERENCE_JSON,
    )


def run_phase0(llm_choice: str = "both") -> None:
    logger.info("=" * 60)
    logger.info("PHASE 0 - Fresh flattened candidate export")
    logger.info("=" * 60)

    if llm_choice in ["openai", "both"]:
        if not NORMALIZED_CSV_OPENAI.exists():
            raise FileNotFoundError(f"OpenAI normalized CSV not found at {NORMALIZED_CSV_OPENAI}")
        export_phase0_flattened(NORMALIZED_CSV_OPENAI, PHASE0_FLATTENED_CSV_OPENAI)

    if llm_choice in ["gemini", "both"]:
        if not NORMALIZED_CSV_GEMINI.exists():
            raise FileNotFoundError(f"Gemini normalized CSV not found at {NORMALIZED_CSV_GEMINI}")
        export_phase0_flattened(NORMALIZED_CSV_GEMINI, PHASE0_FLATTENED_CSV_GEMINI)


def run_phase1_enhanced(
    llm_choice: str = "openai",
    limit: int | None = None,
    allow_unapproved_taxonomy: bool = False,
) -> tuple["pd.DataFrame | None", "pd.DataFrame | None"]:
    """Phase 1 — Enhanced Extraction Schema (plan section: Phase 1).

    Re-runs LLM extraction over the exploded CSV with an 18-field prompt that adds:
      - geographic_scope_type / geographic_scope_label
      - accumulation_window_value / accumulation_window_unit
      - persistence_value / persistence_unit
      - is_observational
      - referenced_eap_id
      - within_statement_connector / cross_statement_connector

    Input:  exploded_thresholds.csv  (must exist from Step 1)
    Output: normalized_thresholds_v2_openai.csv  and/or  normalized_thresholds_v2_gemini.csv

    Args:
        llm_choice: "openai", "gemini", or "both".
        limit: Only process the first N records (for quick testing).
        allow_unapproved_taxonomy: Bypass taxonomy approval gate (dev mode).
    """
    logger.info("=" * 60)
    logger.info("PHASE 1 - Enhanced Extraction Schema (18 fields)")
    logger.info("=" * 60)

    if not EXPLODED_CSV.exists():
        raise FileNotFoundError(
            f"Exploded CSV not found at {EXPLODED_CSV}. "
            "Run --step1-only first."
        )

    df = pd.read_csv(EXPLODED_CSV)
    if limit:
        logger.info("  ⚠️  --limit %d: only processing first %d records.", limit, limit)
        df = df.head(limit)

    openai_df = None
    gemini_df = None

    if llm_choice in ["openai", "both"]:
        logger.info("→ Running Phase 1 enhanced extraction with Azure OpenAI …")
        t0 = time.time()
        openai_df = normalize_dataframe_enhanced(
            df,
            output_path=NORMALIZED_V2_CSV_OPENAI,
            llm_choice="openai",
            allow_unapproved_taxonomy=allow_unapproved_taxonomy,
        )
        errors = openai_df["llm_error"].notna().sum() if "llm_error" in openai_df.columns else 0
        logger.info(
            "  Phase 1 OpenAI done in %.1fs  →  %d records  (%d errors)  →  %s",
            time.time() - t0,
            len(openai_df),
            errors,
            NORMALIZED_V2_CSV_OPENAI,
        )

    if llm_choice in ["gemini", "both"]:
        logger.info("→ Running Phase 1 enhanced extraction with Gemini …")
        t0 = time.time()
        gemini_df = normalize_dataframe_enhanced(
            df,
            output_path=NORMALIZED_V2_CSV_GEMINI,
            llm_choice="gemini",
            allow_unapproved_taxonomy=allow_unapproved_taxonomy,
        )
        errors = gemini_df["llm_error"].notna().sum() if "llm_error" in gemini_df.columns else 0
        logger.info(
            "  Phase 1 Gemini done in %.1fs  →  %d records  (%d errors)  →  %s",
            time.time() - t0,
            len(gemini_df),
            errors,
            NORMALIZED_V2_CSV_GEMINI,
        )

    return openai_df, gemini_df


def _run_phase2_matching(
    llm_choice: str = "openai",
    limit: int | None = None,
) -> None:
    """Phase 2 — LLM Taxonomy Matching Against Combination Matrix.

    Reads the Phase 1 enhanced output (normalized_thresholds_v2_openai.csv) or
    falls back to the standard normalized CSV.  For each record, performs a
    two-pass match against combination_matrix_enriched.csv:
      1. Deterministic lookup by (canonical_variable, subcategory) + unit check
      2. LLM disambiguation for unresolved records

    Writes:
      ui_normalization_output/taxonomy_match_results_v2.csv
      ui_normalization_output/out_of_matrix_review.csv
    """
    logger.info("=" * 60)
    logger.info("PHASE 2 - LLM Taxonomy Matching Against Combination Matrix")
    logger.info("=" * 60)

    # For --llm both, run OpenAI (single pass is sufficient; matrix is universal)
    llm = "openai" if llm_choice in ("openai", "both") else "gemini"

    t0 = time.time()
    result_df = run_phase2_matching(llm=llm, limit=limit)
    n_matched  = (result_df["out_of_matrix"] == False).sum()  # noqa: E712
    n_oom      = (result_df["out_of_matrix"] == True).sum()   # noqa: E712
    logger.info(
        "Phase 2 done in %.1fs  →  %d records  (%d matched / %d out-of-matrix)",
        time.time() - t0,
        len(result_df),
        n_matched,
        n_oom,
    )
    logger.info("  Results  → %s", TAXONOMY_MATCH_RESULTS_CSV)
    if n_oom:
        logger.info("  OOM review → %s", OUT_OF_MATRIX_REVIEW_CSV)


def _run_phase3_connectors(
    llm_choice: str = "openai",
    limit: int | None = None,
) -> None:
    """Phase 3 — Connector Classification Pass.

    For each EAP document, classifies logical relationships between trigger
    statements (connector_map) and between activation phases (inter_phase_connector).

    Input:  taxonomy_match_results_v2.csv  (Phase 2 output)
    Output: ui_normalization_output/connector_map.json
    """
    logger.info("=" * 60)
    logger.info("PHASE 3 - Connector Classification Pass")
    logger.info("=" * 60)

    if not TAXONOMY_MATCH_RESULTS_CSV.exists():
        raise FileNotFoundError(
            f"Phase 2 output not found at {TAXONOMY_MATCH_RESULTS_CSV}. "
            "Run --phase2-matching first."
        )

    # Use openai if both requested (single classification pass is sufficient)
    llm = "openai" if llm_choice in ("openai", "both") else "gemini"

    t0 = time.time()
    results = run_phase3_connector_classification(llm=llm, limit=limit)

    llm_count  = sum(1 for r in results if r.get("classification_method") == "llm")
    det_count  = sum(1 for r in results if r.get("classification_method") == "deterministic")
    err_count  = sum(1 for r in results if r.get("classification_method") == "error")
    avg_conf   = (
        sum(r.get("classification_confidence", 0.0) for r in results) / len(results)
        if results else 0.0
    )
    logger.info(
        "Phase 3 done in %.1fs  →  %d documents  "
        "(LLM: %d / Deterministic: %d / Error: %d / Avg conf: %.2f)",
        time.time() - t0, len(results),
        llm_count, det_count, err_count, avg_conf,
    )
    logger.info("  Connector map → %s", CONNECTOR_MAP_JSON)


def _run_phase4_export() -> None:
    """Phase 4 — Automated Review Artifact Export.

    Consolidates Phase 2 + Phase 3 outputs into a single three-sheet Excel
    workbook (expert_review_package.xlsx) ready for domain expert review.

    Input:  taxonomy_match_results_v2.csv + out_of_matrix_review.csv + connector_map.json
    Output: ui_normalization_output/expert_review_package.xlsx
    """
    logger.info("=" * 60)
    logger.info("PHASE 4 - Automated Review Artifact Export")
    logger.info("=" * 60)

    if not TAXONOMY_MATCH_RESULTS_CSV.exists():
        raise FileNotFoundError(
            f"Phase 2 output not found at {TAXONOMY_MATCH_RESULTS_CSV}. "
            "Run --phase2-matching first."
        )
    if not CONNECTOR_MAP_JSON.exists():
        logger.warning(
            "Phase 3 connector map not found at %s. "
            "Sheet 3 will be empty. Run --phase3-connectors to populate it.",
            CONNECTOR_MAP_JSON,
        )

    import time as _time
    t0 = _time.time()
    out = run_phase4_export(output_xlsx=EXPERT_REVIEW_XLSX)
    logger.info("Phase 4 done in %.1fs  →  %s", _time.time() - t0, out)


def _run_phase5_package(
    force_taxonomy_refresh: bool = False,
    skip_taxonomy_refresh: bool = False,
) -> None:
    """Phase 5 — Output Packaging & UI Schema Update.

    Two sub-tasks:
      5.1  Re-verify / refresh FORECAST_VARIABLES_REFERENCE.json v2 from the
           current combination matrix (no-op when already current).
      5.2  Enrich ui_schema_openai_filtered.json in-place with four new fields:
           accumulation_window_rules, persistence_rules, geographic_scope_contract,
           connector_vocabulary, and combination_matrix_version.

    Inputs:  FORECAST_VARIABLES_REFERENCE.json + ui_schema_openai_filtered.json
    Outputs: both files updated in-place
    """
    logger.info("=" * 60)
    logger.info("PHASE 5 - Output Packaging & UI Schema v2")
    logger.info("=" * 60)

    if not UI_SCHEMA_OPENAI_FILTERED_JSON.exists():
        raise FileNotFoundError(
            f"Filtered UI schema not found at {UI_SCHEMA_OPENAI_FILTERED_JSON}. "
            "Run --step3-only --step3-mode filtered first."
        )

    import time as _time
    t0 = _time.time()
    taxonomy, schema = run_phase5_packaging(
        force_taxonomy_refresh=force_taxonomy_refresh,
        skip_taxonomy_refresh=skip_taxonomy_refresh,
    )

    n_canonicals = len(taxonomy.get("canonicals", []))
    n_subcats = sum(len(c["subcategories"]) for c in taxonomy.get("canonicals", []))
    matrix_ver = schema.get("combination_matrix_version", "?")

    logger.info(
        "Phase 5 done in %.1fs  →  %d canonicals / %d subcategories  "
        "(version: %s)",
        _time.time() - t0, n_canonicals, n_subcats, matrix_ver,
    )
    logger.info("  Taxonomy → %s", FORECAST_VARIABLES_REFERENCE_JSON)
    logger.info("  UI schema v2 → %s", UI_SCHEMA_OPENAI_FILTERED_JSON)


def run_phase1(
    llm_choice: str = "both",
    canonical_target: int = PHASE1_CANONICAL_TARGET,
    coverage_target: float = PHASE1_COVERAGE_TARGET,
    min_support_records: int = PHASE1_MIN_SUPPORT_RECORDS,
    max_alias_overlap: float = PHASE1_MAX_ALIAS_OVERLAP,
    max_iterations: int = PHASE1_MAX_ITERATIONS,
) -> None:
    logger.info("=" * 60)
    logger.info("PHASE 1 - LLM taxonomy proposal and quality gate")
    logger.info("=" * 60)

    if llm_choice in ["openai", "both"]:
        phase0_path = _phase0_output_path("openai")
        if not phase0_path.exists():
            raise FileNotFoundError(f"Phase 0 input missing: {phase0_path}")
        p_json, q_json, r_md = _taxonomy_output_paths("openai")
        run_phase1_taxonomy(
            input_csv=phase0_path,
            llm="openai",
            proposal_json=p_json,
            quality_json=q_json,
            review_md=r_md,
            canonical_target=canonical_target,
            coverage_target=coverage_target,
            min_support_records=min_support_records,
            max_alias_overlap=max_alias_overlap,
            max_iterations=max_iterations,
        )

    if llm_choice in ["gemini", "both"]:
        phase0_path = _phase0_output_path("gemini")
        if not phase0_path.exists():
            raise FileNotFoundError(f"Phase 0 input missing: {phase0_path}")
        p_json, q_json, r_md = _taxonomy_output_paths("gemini")
        run_phase1_taxonomy(
            input_csv=phase0_path,
            llm="gemini",
            proposal_json=p_json,
            quality_json=q_json,
            review_md=r_md,
            canonical_target=canonical_target,
            coverage_target=coverage_target,
            min_support_records=min_support_records,
            max_alias_overlap=max_alias_overlap,
            max_iterations=max_iterations,
        )


def run_step3(
    llm_choice: str = "both",
    step3_mode: str = "standard",
    max_primary_canonicals: int = MAX_PRIMARY_CANONICALS,
    allow_unapproved_taxonomy: bool = False,
    min_frequency: int = 4,
) -> tuple[dict | None, dict | None]:
    """
    Run Step 3 aggregation.
    
    Args:
        llm_choice: "openai", "gemini", or "both" (default)
    
    Returns:
        Tuple of (openai_schema, gemini_schema) - one or both may be None
    """
    logger.info("=" * 60)
    logger.info("STEP 3 – UI Schema Aggregation & Trigger Logic Mapping")
    logger.info("=" * 60)
    logger.info("Step 3 mode: %s", step3_mode)
    if step3_mode == "filtered":
        logger.info("Minimum frequency threshold: %d", min_frequency)
    
    openai_schema = None
    gemini_schema = None
    
    if llm_choice in ["openai", "both"]:
        logger.info("→ Aggregating OpenAI results...")
        t0 = time.time()
        openai_output = _step3_output_path("openai", step3_mode)
        if step3_mode == "filtered":
            openai_schema = aggregate_with_filtering(
                NORMALIZED_CSV_OPENAI,
                openai_output,
                llm="openai",
                max_primary_canonicals=max_primary_canonicals,
                allow_unapproved_taxonomy=allow_unapproved_taxonomy,
                min_frequency=min_frequency,
            )
        else:
            openai_schema = aggregate(NORMALIZED_CSV_OPENAI, openai_output)
        logger.info("  OpenAI aggregation done in %.1fs", time.time() - t0)
    
    if llm_choice in ["gemini", "both"]:
        logger.info("→ Aggregating Gemini results...")
        t0 = time.time()
        gemini_output = _step3_output_path("gemini", step3_mode)
        if step3_mode == "filtered":
            gemini_schema = aggregate_with_filtering(
                NORMALIZED_CSV_GEMINI,
                gemini_output,
                llm="gemini",
                max_primary_canonicals=max_primary_canonicals,
                allow_unapproved_taxonomy=allow_unapproved_taxonomy,
                min_frequency=min_frequency,
            )
        else:
            gemini_schema = aggregate(NORMALIZED_CSV_GEMINI, gemini_output)
        logger.info("  Gemini aggregation done in %.1fs", time.time() - t0)
    
    return openai_schema, gemini_schema


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EAP UI Normalization Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--step1-only", action="store_true", help="Run only Step 1")
    group.add_argument("--step2-only", action="store_true", help="Run only Step 2")
    group.add_argument("--step3-only", action="store_true", help="Run only Step 3")
    group.add_argument("--phase0-only", action="store_true", help="Run only Phase 0 (requires Step 2 output)")
    group.add_argument("--phase1-only", action="store_true", help="Run only Phase 1 (requires Phase 0 output)")
    group.add_argument("--phase0-matrix-draft", action="store_true",
                       help="Phase 0.1a: generate combination_matrix_draft.csv from normalized CSV")
    group.add_argument("--phase0-matrix-enrich", action="store_true",
                       help="Phase 0.1b: LLM-enrich combination_matrix_draft.csv → combination_matrix_enriched.csv")
    group.add_argument("--phase0-matrix-full", action="store_true",
                       help="Run Phase 0.1a + 0.1b in sequence (draft → LLM enrichment)")
    group.add_argument("--phase0-matrix-export", action="store_true",
                       help="Phase 0.1c: consolidate Temperature rows + export combination_matrix_v1.xlsx")
    group.add_argument("--phase0-taxonomy-update", action="store_true",
                       help="Phase 0.2/0.3/0.4: regenerate FORECAST_VARIABLES_REFERENCE.json to v2 "
                            "canonical structure (reads combination_matrix_enriched.csv by default; "
                            "use --taxonomy-source xlsx to read from combination_matrix_v1.xlsx)")
    group.add_argument("--phase1-enhanced", action="store_true",
                       help="Phase 1: re-run LLM extraction with 18-field enhanced prompt "
                            "(adds geographic scope, accumulation window, persistence, "
                            "is_observational, referenced_eap_id, connectors). "
                            "Reads exploded_thresholds.csv; outputs normalized_thresholds_v2_openai.csv.")
    group.add_argument("--phase2-matching", action="store_true",
                       help="Phase 2: LLM taxonomy matching against combination matrix. "
                            "Two-pass (deterministic + LLM); builds combination_path per record. "
                            "Outputs taxonomy_match_results_v2.csv + out_of_matrix_review.csv.")
    group.add_argument("--phase3-connectors", action="store_true",
                       help="Phase 3: Connector classification pass. For each document, classify "
                            "the logical relationships between trigger statements (AND/OR/THEN/IF_THEN) "
                            "and between phases (ENABLES/PRECEDES/etc.). "
                            "Input: taxonomy_match_results_v2.csv. Output: connector_map.json.")
    group.add_argument("--phase4-export", action="store_true",
                       help="Phase 4: Automated review artifact export. Consolidates Phase 2 + "
                            "Phase 3 outputs into expert_review_package.xlsx (three sheets: "
                            "All Matched Records / Out-of-Matrix Records / Connector Map Summary).")
    group.add_argument("--phase5-package", action="store_true",
                       help="Phase 5: Output packaging. (1) Re-verifies / refreshes "
                            "FORECAST_VARIABLES_REFERENCE.json from the current combination "
                            "matrix. (2) Enriches ui_schema_openai_filtered.json in-place with "
                            "accumulation_window_rules, persistence_rules, "
                            "geographic_scope_contract, connector_vocabulary, and "
                            "combination_matrix_version.")
    group.add_argument("--skip-step2", action="store_true",
                       help="Run Step 1 + Step 3, skip LLM calls")

    parser.add_argument(
        "--no-consolidate",
        action="store_true",
        help="When used with --phase0-matrix-export, skip Temperature consolidation.",
    )
    parser.add_argument(
        "--taxonomy-source",
        choices=["csv", "xlsx"],
        default="csv",
        help="Matrix source for --phase0-taxonomy-update: 'csv' (default) or 'xlsx'.",
    )
    phase5_group = parser.add_mutually_exclusive_group()
    phase5_group.add_argument(
        "--force-taxonomy-refresh",
        action="store_true",
        help="Used with --phase5-package: always re-write FORECAST_VARIABLES_REFERENCE.json "
             "even if already current.",
    )
    phase5_group.add_argument(
        "--skip-taxonomy-refresh",
        action="store_true",
        help="Used with --phase5-package: skip taxonomy re-write; load existing JSON from disk.",
    )
    parser.add_argument("--run-phase0", action="store_true",
                        help="After Step 2, run Phase 0 flattened export")
    parser.add_argument("--run-phase1", action="store_true",
                        help="After Phase 0, run Phase 1 taxonomy proposal")
    
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit LLM normalization to first N records (for testing)")
    parser.add_argument("--llm",
                        choices=["openai", "gemini", "both"],
                        default="both",
                        help="Which LLM(s) to run: 'openai', 'gemini', or 'both' (default: both)")
    parser.add_argument(
        "--step3-mode",
        choices=["standard", "filtered"],
        default="standard",
        help="Which Step 3 aggregator to run: 'standard' or 'filtered' (default: standard)",
    )
    parser.add_argument(
        "--min-frequency",
        type=int,
        default=4,
        help="Minimum forecast-variable frequency used when --step3-mode filtered (default: 4)",
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
        help="Allow Phase 2/3 execution with non-approved taxonomy artifacts (development mode).",
    )
    parser.add_argument("--phase1-canonical-target", type=int, default=PHASE1_CANONICAL_TARGET,
                        help=f"Canonical variable target for Phase 1 (default: {PHASE1_CANONICAL_TARGET})")
    parser.add_argument("--phase1-coverage-target", type=float, default=PHASE1_COVERAGE_TARGET,
                        help=f"Coverage target for Phase 1 (default: {PHASE1_COVERAGE_TARGET})")
    parser.add_argument("--phase1-min-support", type=int, default=PHASE1_MIN_SUPPORT_RECORDS,
                        help=f"Minimum support records per canonical group (default: {PHASE1_MIN_SUPPORT_RECORDS})")
    parser.add_argument("--phase1-max-alias-overlap", type=float, default=PHASE1_MAX_ALIAS_OVERLAP,
                        help=f"Maximum alias overlap between canonical groups (default: {PHASE1_MAX_ALIAS_OVERLAP})")
    parser.add_argument("--phase1-max-iterations", type=int, default=PHASE1_MAX_ITERATIONS,
                        help=f"Maximum Phase 1 proposal iterations (default: {PHASE1_MAX_ITERATIONS})")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("══════════════════════════════════════════════════════════╗")
    logger.info("║   EAP UI Normalization Pipeline                          ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info("Output directory: %s", OUTPUT_DIR)

    pipeline_start = time.time()

    # ── Step 1 only ──────────────────────────────────────────────────────────
    if args.step1_only:
        run_step1()

    # ── Step 2 only ──────────────────────────────────────────────────────────
    elif args.step2_only:
        if not EXPLODED_CSV.exists():
            logger.error("Exploded CSV not found. Run --step1-only first.")
            sys.exit(1)
        df = pd.read_csv(EXPLODED_CSV)
        run_step2(
            df,
            limit=args.limit,
            llm_choice=args.llm,
            allow_unapproved_taxonomy=args.allow_unapproved_taxonomy,
        )

    # ── Step 3 only ──────────────────────────────────────────────────────────
    elif args.step3_only:
        if args.llm in ["openai", "both"] and not NORMALIZED_CSV_OPENAI.exists():
            logger.error("OpenAI normalized CSV not found at %s. Run --step2-only --llm openai first.", NORMALIZED_CSV_OPENAI)
            sys.exit(1)
        if args.llm in ["gemini", "both"] and not NORMALIZED_CSV_GEMINI.exists():
            logger.error("Gemini normalized CSV not found at %s. Run --step2-only --llm gemini first.", NORMALIZED_CSV_GEMINI)
            sys.exit(1)
        run_step3(
            llm_choice=args.llm,
            step3_mode=args.step3_mode,
            max_primary_canonicals=args.max_primary_canonicals,
            allow_unapproved_taxonomy=args.allow_unapproved_taxonomy,
            min_frequency=args.min_frequency,
        )

    # ── Phase 0 only ────────────────────────────────────────────────────────
    elif args.phase0_only:
        run_phase0(llm_choice=args.llm)

    # ── Phase 0.1a — Combination Matrix Draft ───────────────────────────────
    elif args.phase0_matrix_draft:
        run_combination_matrix_draft()

    # ── Phase 0.1b — LLM Enrichment ─────────────────────────────────────────
    elif args.phase0_matrix_enrich:
        run_combination_matrix_enrichment(llm_choice=args.llm)

    # ── Phase 0.1a + 0.1b — Full combination matrix pipeline ─────────────────
    elif args.phase0_matrix_full:
        run_combination_matrix_draft()
        run_combination_matrix_enrichment(llm_choice=args.llm)

    # ── Phase 0.1c — Consolidate + Excel export ───────────────────────────────
    elif args.phase0_matrix_export:
        run_combination_matrix_export(no_consolidate=args.no_consolidate)

    # ── Phase 0.2/0.3/0.4 — Update FORECAST_VARIABLES_REFERENCE.json → v2 ───
    elif args.phase0_taxonomy_update:
        run_taxonomy_update(source=args.taxonomy_source)

    # ── Phase 1 Enhanced Extraction ──────────────────────────────────────────
    elif args.phase1_enhanced:
        run_phase1_enhanced(
            llm_choice=args.llm,
            limit=args.limit,
            allow_unapproved_taxonomy=args.allow_unapproved_taxonomy,
        )

    # ── Phase 2 — LLM Taxonomy Matching Against Combination Matrix ────────────
    elif args.phase2_matching:
        _run_phase2_matching(llm_choice=args.llm, limit=args.limit)

    # ── Phase 3 — Connector Classification ───────────────────────────────────
    elif args.phase3_connectors:
        _run_phase3_connectors(llm_choice=args.llm, limit=args.limit)
    # ── Phase 4 — Automated Review Artifact Export ────────────────────────
    elif args.phase4_export:
        _run_phase4_export()
    # ── Phase 5 — Output Packaging & UI Schema v2 ─────────────────────────
    elif args.phase5_package:
        _run_phase5_package(
            force_taxonomy_refresh=getattr(args, "force_taxonomy_refresh", False),
            skip_taxonomy_refresh=getattr(args, "skip_taxonomy_refresh", False),
        )
    # ── Phase 1 only (legacy taxonomy proposal) ─────────────────────────────
    elif args.phase1_only:
        run_phase1(
            llm_choice=args.llm,
            canonical_target=args.phase1_canonical_target,
            coverage_target=args.phase1_coverage_target,
            min_support_records=args.phase1_min_support,
            max_alias_overlap=args.phase1_max_alias_overlap,
            max_iterations=args.phase1_max_iterations,
        )

    # ── Skip Step 2 (Step 1 + Step 3) ────────────────────────────────────────
    elif args.skip_step2:
        run_step1()
        # Check if at least one LLM output exists
        if args.llm == "openai" and not NORMALIZED_CSV_OPENAI.exists():
            logger.error("OpenAI normalized CSV not found. Cannot skip Step 2.")
            sys.exit(1)
        elif args.llm == "gemini" and not NORMALIZED_CSV_GEMINI.exists():
            logger.error("Gemini normalized CSV not found. Cannot skip Step 2.")
            sys.exit(1)
        elif args.llm == "both" and not (NORMALIZED_CSV_OPENAI.exists() and NORMALIZED_CSV_GEMINI.exists()):
            logger.error("Both normalized CSVs are required for --llm both. Run --step2-only --llm both first.")
            sys.exit(1)
        run_step3(
            llm_choice=args.llm,
            step3_mode=args.step3_mode,
            max_primary_canonicals=args.max_primary_canonicals,
            allow_unapproved_taxonomy=args.allow_unapproved_taxonomy,
            min_frequency=args.min_frequency,
        )

    # ── Full pipeline (default) ───────────────────────────────────────────────
    else:
        df_exploded     = run_step1()
        openai_df, gemini_df = run_step2(
            df_exploded,
            limit=args.limit,
            llm_choice=args.llm,
            allow_unapproved_taxonomy=args.allow_unapproved_taxonomy,
        )
        if args.run_phase0 or args.run_phase1:
            run_phase0(llm_choice=args.llm)
        if args.run_phase1:
            run_phase1(
                llm_choice=args.llm,
                canonical_target=args.phase1_canonical_target,
                coverage_target=args.phase1_coverage_target,
                min_support_records=args.phase1_min_support,
                max_alias_overlap=args.phase1_max_alias_overlap,
                max_iterations=args.phase1_max_iterations,
            )
        openai_schema, gemini_schema = run_step3(
            llm_choice=args.llm,
            step3_mode=args.step3_mode,
            max_primary_canonicals=args.max_primary_canonicals,
            allow_unapproved_taxonomy=args.allow_unapproved_taxonomy,
            min_frequency=args.min_frequency,
        )

        # Final summary
        logger.info("")
        logger.info("╔══════════════════════════════════════════════════════════╗")
        logger.info("║   PIPELINE COMPLETE                                      ║")
        logger.info("╚══════════════════════════════════════════════════════════╝")
        logger.info("  Total time      : %.1fs", time.time() - pipeline_start)
        
        if args.llm in ["openai", "both"] and openai_schema:
            meta = openai_schema["metadata"]
            dm   = openai_schema["dropdown_masters"]
            logger.info("")
            logger.info("  ── Azure OpenAI Results ──")
            logger.info("    Documents       : %d", meta["total_documents"])
            logger.info("    Statements      : %d", meta["total_statements"])
            logger.info("    Thresholds      : %d", meta["total_thresholds"])
            logger.info("    Failed records  : %d", meta["failed_records"])
            logger.info("    Output files:")
            logger.info("      %s", NORMALIZED_CSV_OPENAI)
            logger.info("      %s", _step3_output_path("openai", args.step3_mode))
        
        if args.llm in ["gemini", "both"] and gemini_schema:
            meta = gemini_schema["metadata"]
            dm   = gemini_schema["dropdown_masters"]
            logger.info("")
            logger.info("  ── Gemini Results ──")
            logger.info("    Documents       : %d", meta["total_documents"])
            logger.info("    Statements      : %d", meta["total_statements"])
            logger.info("    Thresholds      : %d", meta["total_thresholds"])
            logger.info("    Failed records  : %d", meta["failed_records"])
            logger.info("    Output files:")
            logger.info("      %s", NORMALIZED_CSV_GEMINI)
            logger.info("      %s", _step3_output_path("gemini", args.step3_mode))


if __name__ == "__main__":
    main()
