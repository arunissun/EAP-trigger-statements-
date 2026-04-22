"""
Phase 4 — Automated Review Artifact Export.

Consolidates all pipeline outputs (Phase 2 + Phase 3) into a single shareable
Excel package ready to send to domain experts.

Input files:
  ui_normalization_output/taxonomy_match_results_v2.csv   (Phase 2)
  ui_normalization_output/out_of_matrix_review.csv        (Phase 2)
  ui_normalization_output/connector_map.json              (Phase 3)

Output:
  ui_normalization_output/expert_review_package.xlsx      (three-sheet workbook)

Sheet structure:
  1. All Matched Records  — every threshold condition row with full structured data
  2. Out-of-Matrix Records — rows flagged for potential matrix expansion
  3. Connector Map Summary — per-document phase map + connector classification

Usage (from project root):
    python -m scripts.ui_normalization.export_review_artifact
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Config import (works standalone or as package)
# ---------------------------------------------------------------------------
try:
    from .config import (
        TAXONOMY_MATCH_RESULTS_CSV,
        OUT_OF_MATRIX_REVIEW_CSV,
        CONNECTOR_MAP_JSON,
        OUTPUT_DIR,
    )
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        TAXONOMY_MATCH_RESULTS_CSV,
        OUT_OF_MATRIX_REVIEW_CSV,
        CONNECTOR_MAP_JSON,
        OUTPUT_DIR,
    )

logger = logging.getLogger(__name__)

EXPERT_REVIEW_XLSX = OUTPUT_DIR / "expert_review_package.xlsx"

# ---------------------------------------------------------------------------
# Column selection for Sheet 1 (All Matched Records)
# Only columns specified in the plan; tolerate absent columns gracefully.
# ---------------------------------------------------------------------------
_SHEET1_COLUMNS = [
    "document_id",
    "document_name",
    "hazard_type",
    "threshold_text",
    # matched taxonomy
    "matched_canonical",
    "matched_subcategory",
    "matched_unit",
    # structured fields
    "accumulation_window_value",
    "accumulation_window_unit",
    "persistence_value",
    "persistence_unit",
    "probability_value",
    "lead_time_value",
    "lead_time_unit",
    "geographic_scope_type",
    "geographic_scope_label",
    # connector fields
    "within_statement_connector",
    "cross_statement_connector",
    # match quality
    "match_confidence",
    "out_of_matrix",
]

# Friendly display names for Sheet 1 headers
_SHEET1_RENAME = {
    "matched_canonical":          "Matched Canonical",
    "matched_subcategory":        "Matched Subcategory",
    "matched_unit":               "Matched Unit",
    "accumulation_window_value":  "Accum Window Value",
    "accumulation_window_unit":   "Accum Window Unit",
    "persistence_value":          "Persistence Value",
    "persistence_unit":           "Persistence Unit",
    "probability_value":          "Probability (%)",
    "lead_time_value":            "Lead Time Value",
    "lead_time_unit":             "Lead Time Unit",
    "geographic_scope_type":      "Geo Scope Type",
    "geographic_scope_label":     "Geo Scope Label",
    "within_statement_connector": "Within-Stmt Connector",
    "cross_statement_connector":  "Cross-Stmt Connector",
    "match_confidence":           "Match Confidence",
    "out_of_matrix":              "Out of Matrix",
    "document_id":                "Document ID",
    "document_name":              "Document Name",
    "hazard_type":                "Hazard Type",
    "threshold_text":             "Threshold Text",
}

# ---------------------------------------------------------------------------
# Column selection for Sheet 2 (Out-of-Matrix Records)
# ---------------------------------------------------------------------------
_SHEET2_COLUMNS = [
    "document_id",
    "document_name",
    "statement_key",
    "threshold_index",
    "threshold_text",
    "canonical_variable",
    "subcategory",
    "threshold_unit",
    "match_method",
    "match_confidence",
    "geographic_scope_type",
    "geographic_scope_label",
    "matched_canonical",
    "matched_subcategory",
]

_SHEET2_RENAME = {
    "document_id":            "Document ID",
    "document_name":          "Document Name",
    "statement_key":          "Statement Key",
    "threshold_index":        "Threshold Index",
    "threshold_text":         "Threshold Text",
    "canonical_variable":     "Extracted Canonical",
    "subcategory":            "Extracted Subcategory",
    "threshold_unit":         "Extracted Unit",
    "match_method":           "Match Method",
    "match_confidence":       "Match Confidence",
    "geographic_scope_type":  "Geo Scope Type",
    "geographic_scope_label": "Geo Scope Label",
    "matched_canonical":      "Suggested Canonical",
    "matched_subcategory":    "Suggested Subcategory",
}


def _build_sheet1(match_csv: Path) -> pd.DataFrame:
    """Build Sheet 1: All Matched Records."""
    df = pd.read_csv(match_csv)

    # Resolve lead_time_unit — Phase 1 stores it as 'timeframe_unit'
    if "lead_time_unit" not in df.columns and "timeframe_unit" in df.columns:
        df["lead_time_unit"] = df["timeframe_unit"]

    # Select available columns only (tolerate gaps between pipeline versions)
    available = [c for c in _SHEET1_COLUMNS if c in df.columns]
    missing = [c for c in _SHEET1_COLUMNS if c not in df.columns]
    if missing:
        logger.debug("Sheet 1: columns not found in CSV (skipped): %s", missing)

    df = df[available].copy()
    df = df.rename(columns={k: v for k, v in _SHEET1_RENAME.items() if k in df.columns})
    return df


def _build_sheet2(oom_csv: Path) -> pd.DataFrame:
    """Build Sheet 2: Out-of-Matrix Records."""
    if not oom_csv.exists():
        logger.warning("Out-of-matrix CSV not found at %s; Sheet 2 will be empty.", oom_csv)
        return pd.DataFrame()

    df = pd.read_csv(oom_csv)
    if df.empty:
        logger.info("No out-of-matrix records found; Sheet 2 will be empty.")
        return df

    available = [c for c in _SHEET2_COLUMNS if c in df.columns]
    df = df[available].copy()
    df = df.rename(columns={k: v for k, v in _SHEET2_RENAME.items() if k in df.columns})
    return df


def _build_sheet3(connector_json: Path) -> pd.DataFrame:
    """Build Sheet 3: Connector Map Summary.

    Flattens the per-document connector_map.json into a tabular format:
      one row per document with columns for each classification result.
    """
    if not connector_json.exists():
        logger.warning("Connector map JSON not found at %s; Sheet 3 will be empty.", connector_json)
        return pd.DataFrame()

    with connector_json.open(encoding="utf-8") as fh:
        data = json.load(fh)

    if not data:
        return pd.DataFrame()

    rows = []
    for entry in data:
        phase_map = entry.get("phase_map") or {}
        connector_map = entry.get("connector_map") or {}

        rows.append({
            "Document ID":             entry.get("document_id", ""),
            "Document Name":           entry.get("document_name", ""),
            "Hazard Type":             entry.get("hazard_type", ""),
            "Activation Type":         entry.get("activation_type", ""),
            "Phase Map":               _fmt_dict(phase_map),
            "Statement Connectors":    _fmt_dict(connector_map),
            "Inter-Phase Connector":   entry.get("inter_phase_connector", ""),
            "Stop Mechanism Present":  entry.get("stop_mechanism_present", ""),
            "Stop Connector":          entry.get("stop_connector", ""),
            "Classification Method":   entry.get("classification_method", ""),
            "Classification Confidence": entry.get("classification_confidence", ""),
            "Notes":                   entry.get("notes", ""),
        })

    return pd.DataFrame(rows)


def _fmt_dict(d: dict) -> str:
    """Render a dict as a compact multi-line string for an Excel cell."""
    if not d:
        return ""
    return "\n".join(f"{k}: {v}" for k, v in d.items())


# ---------------------------------------------------------------------------
# Excel writer helpers
# ---------------------------------------------------------------------------

def _apply_sheet_formatting(
    worksheet,
    df: pd.DataFrame,
    workbook,
    freeze_cols: int = 4,
) -> None:
    """Apply column widths, header bold, text-wrap for long-text columns."""
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1})
    wrap_fmt = workbook.add_format({"text_wrap": True, "valign": "top"})
    default_fmt = workbook.add_format({"valign": "top"})

    # Write header row with bold format
    for col_idx, col_name in enumerate(df.columns):
        worksheet.write(0, col_idx, col_name, header_fmt)

    # Auto-fit column widths (capped)
    for col_idx, col_name in enumerate(df.columns):
        col_str = str(col_name)
        max_len = max(
            len(col_str),
            df.iloc[:, col_idx].astype(str).str.len().max() if not df.empty else 0,
        )
        # Long-text columns get wrap + fixed width
        if col_name in ("Threshold Text", "Notes", "Phase Map", "Statement Connectors"):
            worksheet.set_column(col_idx, col_idx, min(max_len, 60), wrap_fmt)
        else:
            worksheet.set_column(col_idx, col_idx, min(max_len + 2, 40), default_fmt)

    # Freeze first N columns + header row
    worksheet.freeze_panes(1, freeze_cols)


def export_review_package(
    match_csv: Path = TAXONOMY_MATCH_RESULTS_CSV,
    oom_csv: Path = OUT_OF_MATRIX_REVIEW_CSV,
    connector_json: Path = CONNECTOR_MAP_JSON,
    output_xlsx: Path = EXPERT_REVIEW_XLSX,
) -> Path:
    """Build and write the three-sheet expert review Excel workbook.

    Args:
        match_csv:       Path to taxonomy_match_results_v2.csv (Phase 2 output).
        oom_csv:         Path to out_of_matrix_review.csv (Phase 2 output).
        connector_json:  Path to connector_map.json (Phase 3 output).
        output_xlsx:     Destination path for the Excel workbook.

    Returns:
        Path to the written workbook.
    """
    if not match_csv.exists():
        raise FileNotFoundError(
            f"Phase 2 match results not found at {match_csv}. "
            "Run --phase2-matching first."
        )

    logger.info("Building Sheet 1: All Matched Records ...")
    sheet1 = _build_sheet1(match_csv)
    logger.info("  → %d rows", len(sheet1))

    logger.info("Building Sheet 2: Out-of-Matrix Records ...")
    sheet2 = _build_sheet2(oom_csv)
    logger.info("  → %d rows", len(sheet2))

    logger.info("Building Sheet 3: Connector Map Summary ...")
    sheet3 = _build_sheet3(connector_json)
    logger.info("  → %d rows", len(sheet3))

    output_xlsx.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_xlsx, engine="xlsxwriter") as writer:
        workbook = writer.book

        # ── Sheet 1 ────────────────────────────────────────────────────────
        if not sheet1.empty:
            sheet1.to_excel(writer, sheet_name="All Matched Records", index=False, startrow=0)
            ws1 = writer.sheets["All Matched Records"]
            _apply_sheet_formatting(ws1, sheet1, workbook, freeze_cols=4)
        else:
            pd.DataFrame().to_excel(writer, sheet_name="All Matched Records", index=False)

        # ── Sheet 2 ────────────────────────────────────────────────────────
        sheet_name2 = "Out-of-Matrix Records"
        if not sheet2.empty:
            sheet2.to_excel(writer, sheet_name=sheet_name2, index=False, startrow=0)
            ws2 = writer.sheets[sheet_name2]
            _apply_sheet_formatting(ws2, sheet2, workbook, freeze_cols=3)
        else:
            # Still create the sheet so recipients see all tabs
            pd.DataFrame(
                columns=list(_SHEET2_RENAME.values())
            ).to_excel(writer, sheet_name=sheet_name2, index=False)

        # ── Sheet 3 ────────────────────────────────────────────────────────
        if not sheet3.empty:
            sheet3.to_excel(writer, sheet_name="Connector Map Summary", index=False, startrow=0)
            ws3 = writer.sheets["Connector Map Summary"]
            _apply_sheet_formatting(ws3, sheet3, workbook, freeze_cols=2)
        else:
            pd.DataFrame().to_excel(writer, sheet_name="Connector Map Summary", index=False)

    logger.info("Expert review package written → %s", output_xlsx)
    return output_xlsx


# ---------------------------------------------------------------------------
# Standalone entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    parser = argparse.ArgumentParser(description="Phase 4: export expert_review_package.xlsx")
    parser.add_argument(
        "--output",
        type=Path,
        default=EXPERT_REVIEW_XLSX,
        help=f"Destination XLSX path (default: {EXPERT_REVIEW_XLSX})",
    )
    args = parser.parse_args()

    out = export_review_package(output_xlsx=args.output)
    print(f"Done → {out}")


if __name__ == "__main__":
    main()
