"""
EAP UI Normalization Pipeline – End-to-End Orchestrator.

Usage (from project root):
    python -m scripts.ui_normalization.run_pipeline [OPTIONS]

Options:
    --step1-only    Run only Step 1 (explode JSON)
    --step2-only    Run only Step 2 (LLM normalization, requires step1 output)
    --step3-only    Run only Step 3 (aggregate UI schema, requires step2 output)
    --skip-step2    Run Step 1 + Step 3, skipping LLM calls (useful for testing)
    --step3-mode    Step 3 aggregator mode: standard|filtered (default: standard)
    --min-frequency Minimum frequency used by filtered Step 3 mode (default: 4)
    --limit N       Only normalize the first N records (for quick testing)
    --help          Show this message

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
    from .step1_exploder  import explode_json, save_exploded
    from .step2_llm       import normalize_dataframe
    from .phase0_prepare_flattened import export_phase0_flattened
    from .phase1_taxonomy import run_phase1_taxonomy
    from .step3_aggregator import aggregate
    from .step3_aggregator_filtered import aggregate_with_filtering
    from .config import (
        INPUT_JSON, EXPLODED_CSV,
        NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI,
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
    )
except ImportError:
    from step1_exploder   import explode_json, save_exploded
    from step2_llm        import normalize_dataframe
    from phase0_prepare_flattened import export_phase0_flattened
    from phase1_taxonomy import run_phase1_taxonomy
    from step3_aggregator import aggregate
    from step3_aggregator_filtered import aggregate_with_filtering
    from config import (
        INPUT_JSON, EXPLODED_CSV,
        NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI,
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
    group.add_argument("--skip-step2", action="store_true",
                       help="Run Step 1 + Step 3, skip LLM calls")

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

    logger.info("╔══════════════════════════════════════════════════════════╗")
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

    # ── Phase 1 only ────────────────────────────────────────────────────────
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
