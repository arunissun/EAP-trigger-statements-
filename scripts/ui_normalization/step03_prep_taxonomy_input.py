"""
Phase 0 - Prepare flattened normalization candidates from fresh trigger extraction.

Creates a flat table required by the taxonomy pass with columns:
  - document_id
  - hazard_type
  - trigger_statement
  - forecast_variable
  - unit
  - operator
  - source
  - lead_time_days
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import pandas as pd

try:
    from .config import (
        NORMALIZED_CSV_OPENAI,
        NORMALIZED_CSV_GEMINI,
        PHASE0_FLATTENED_CSV_OPENAI,
        PHASE0_FLATTENED_CSV_GEMINI,
    )
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        NORMALIZED_CSV_OPENAI,
        NORMALIZED_CSV_GEMINI,
        PHASE0_FLATTENED_CSV_OPENAI,
        PHASE0_FLATTENED_CSV_GEMINI,
    )

logger = logging.getLogger(__name__)


TEMPORAL_VARIABLE_TERMS = {
    "lead time",
    "forecast lead time",
    "duration",
    "conditions duration",
    "heatwave duration",
    "forecast horizon",
    "forecast period",
    "timeframe",
    "time window",
}


def _is_temporal_variable_name(name: str) -> bool:
    value = str(name or "").strip().lower()
    return value in TEMPORAL_VARIABLE_TERMS


def _resolve_variable_for_temporal_rows(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Reassign temporal pseudo-variables (e.g., lead time) to a nearby/sibling
    non-temporal forecast variable in the same trigger statement context.

    Returns:
      - resolved_forecast_variable series
      - resolution_note series
    """
    resolved = df["forecast_variable"].astype(str).str.strip().copy()
    notes = pd.Series(["as_extracted"] * len(df), index=df.index, dtype="object")

    temporal_mask = resolved.apply(_is_temporal_variable_name)
    if not temporal_mask.any():
        return resolved, notes

    for idx in df.index[temporal_mask]:
        row = df.loc[idx]

        statement_candidates = pd.DataFrame()
        if "document_id" in df.columns and "statement_key" in df.columns:
            statement_candidates = df[
                (df["document_id"] == row.get("document_id"))
                & (df["statement_key"] == row.get("statement_key"))
            ].copy()

        if not statement_candidates.empty:
            statement_candidates["candidate_variable"] = (
                statement_candidates["forecast_variable"].astype(str).str.strip()
            )
            statement_candidates = statement_candidates[
                ~statement_candidates["candidate_variable"].apply(_is_temporal_variable_name)
            ]
            statement_candidates = statement_candidates[
                statement_candidates["candidate_variable"] != ""
            ]

            if not statement_candidates.empty:
                choice = (
                    statement_candidates["candidate_variable"].value_counts().sort_values(ascending=False).index[0]
                )
                resolved.loc[idx] = choice
                notes.loc[idx] = "reassigned_from_statement_context"
                continue

        document_candidates = pd.DataFrame()
        if "document_id" in df.columns:
            document_candidates = df[df["document_id"] == row.get("document_id")].copy()

        if not document_candidates.empty:
            document_candidates["candidate_variable"] = (
                document_candidates["forecast_variable"].astype(str).str.strip()
            )
            document_candidates = document_candidates[
                ~document_candidates["candidate_variable"].apply(_is_temporal_variable_name)
            ]
            document_candidates = document_candidates[
                document_candidates["candidate_variable"] != ""
            ]

            if not document_candidates.empty:
                choice = (
                    document_candidates["candidate_variable"].value_counts().sort_values(ascending=False).index[0]
                )
                resolved.loc[idx] = choice
                notes.loc[idx] = "reassigned_from_document_context"
                continue

        resolved.loc[idx] = ""
        notes.loc[idx] = "dropped_unresolved_temporal_variable"

    return resolved, notes


def _to_days(value: float, unit: str) -> float:
    unit_norm = str(unit or "").strip().lower()
    if unit_norm == "hours":
        return float(value) / 24.0
    if unit_norm == "days":
        return float(value)
    if unit_norm == "weeks":
        return float(value) * 7.0
    if unit_norm == "months":
        return float(value) * 30.0
    return float(value)


def _parse_lead_time_from_text(raw_lead_time: str) -> float | None:
    text = str(raw_lead_time or "").strip().lower()
    if not text:
        return None

    numbers = [float(v) for v in re.findall(r"\d+(?:\.\d+)?", text)]
    if not numbers:
        return None

    selected = max(numbers)
    if "hour" in text:
        return selected / 24.0
    if "week" in text:
        return selected * 7.0
    if "month" in text:
        return selected * 30.0
    return selected


def _compute_lead_time_days(df: pd.DataFrame) -> pd.Series:
    values = []
    for _, row in df.iterrows():
        lead_value = row.get("lead_time_value")
        timeframe = row.get("timeframe_unit")
        if pd.notna(lead_value) and str(timeframe or "").strip():
            try:
                values.append(round(_to_days(float(lead_value), str(timeframe)), 2))
                continue
            except Exception:
                pass

        fallback = _parse_lead_time_from_text(str(row.get("lead_time", "")))
        values.append(round(fallback, 2) if fallback is not None else None)

    return pd.Series(values)


def export_phase0_flattened(normalized_csv: Path, output_csv: Path) -> pd.DataFrame:
    """Build and save the Phase 0 flattened table from a normalized CSV."""
    logger.info("Loading normalized records from %s", normalized_csv)
    df = pd.read_csv(normalized_csv)

    if "llm_error" in df.columns:
        df = df[df["llm_error"].isna()].copy()

    forecast_variable_raw = df.get("forecast_variable")
    if forecast_variable_raw is None:
        forecast_variable_raw = pd.Series([None] * len(df), index=df.index)
    df["forecast_variable"] = forecast_variable_raw.astype(str).str.strip()

    resolved_variable, resolution_note = _resolve_variable_for_temporal_rows(df)

    lead_time_days = _compute_lead_time_days(df)

    source_series = df.get("normalized_source")
    if source_series is None:
        source_series = pd.Series([None] * len(df))
    fallback_source = df.get("source_authority")
    if fallback_source is None:
        fallback_source = pd.Series([None] * len(df))

    out = pd.DataFrame(
        {
            "document_id": df.get("document_id"),
            "hazard_type": df.get("hazard_type"),
            "trigger_statement": df.get("threshold_text"),
            "forecast_variable": resolved_variable,
            "unit": df.get("threshold_unit"),
            "operator": df.get("threshold_operator"),
            "source": source_series.fillna(fallback_source),
            "lead_time_days": lead_time_days,
            "variable_resolution_note": resolution_note,
        }
    )

    out = out.dropna(subset=["forecast_variable"]).copy()
    out["forecast_variable"] = out["forecast_variable"].astype(str).str.strip()
    
    # Save unresolved temporal/blank rows to a review artifact before filtering
    unresolved_mask = out["forecast_variable"] == ""
    unresolved_rows = out[unresolved_mask]
    if not unresolved_rows.empty:
        review_csv = output_csv.parent / f"{output_csv.stem}_unresolved_temporal.csv"
        unresolved_rows.to_csv(review_csv, index=False, encoding="utf-8")
        logger.warning("Exported %d unresolved temporal pseudo-variable rows to %s", len(unresolved_rows), review_csv)

    out = out[~unresolved_mask]

    dropped_temporal_rows = int((resolution_note == "dropped_unresolved_temporal_variable").sum())
    reassigned_rows = int(
        (resolution_note == "reassigned_from_statement_context").sum()
        + (resolution_note == "reassigned_from_document_context").sum()
    )

    if reassigned_rows:
        logger.info("Reassigned %d temporal pseudo-variable rows to context variables.", reassigned_rows)
    if dropped_temporal_rows:
        logger.warning("Dropped %d unresolved temporal pseudo-variable rows.", dropped_temporal_rows)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False, encoding="utf-8")
    logger.info("Saved Phase 0 flattened table to %s (%d rows)", output_csv, len(out))
    return out


def _resolve_paths(llm: str) -> tuple[Path, Path]:
    if llm == "openai":
        return NORMALIZED_CSV_OPENAI, PHASE0_FLATTENED_CSV_OPENAI
    return NORMALIZED_CSV_GEMINI, PHASE0_FLATTENED_CSV_GEMINI


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 0 flattened table exporter")
    parser.add_argument("--llm", choices=["openai", "gemini"], default="openai")
    parser.add_argument("--input-csv", type=str, default=None, help="Optional custom normalized CSV path")
    parser.add_argument("--output-csv", type=str, default=None, help="Optional custom flattened CSV path")
    args = parser.parse_args()

    input_csv, output_csv = _resolve_paths(args.llm)
    if args.input_csv:
        input_csv = Path(args.input_csv)
    if args.output_csv:
        output_csv = Path(args.output_csv)

    if not input_csv.exists():
        raise FileNotFoundError(f"Normalized CSV not found: {input_csv}")

    export_phase0_flattened(input_csv, output_csv)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    main()
