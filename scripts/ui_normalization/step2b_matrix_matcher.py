"""
Phase 2 — LLM Taxonomy Matching Against Combination Matrix.

For every extracted threshold condition record (from normalized_thresholds_v2_openai.csv,
or normalized_thresholds_openai.csv as fallback), matches it against the approved
combination matrix using a two-pass approach:

  Pass 1 — Deterministic lookup
    • Exact match on (canonical_variable, subcategory) + unit in primary_unit list
      → match_method = "deterministic",       match_confidence = 1.0
    • Match on (canonical_variable, subcategory) only (unit absent / not listed)
      → match_method = "deterministic_alias", match_confidence = 0.95

  Pass 2 — LLM disambiguation
    • For records that did not match in Pass 1, send threshold_text + extracted fields
      to the LLM with all matrix rows as context; ask for best row index or "out_of_matrix"
      → match_method = "llm",                 match_confidence = value returned by LLM
    • Records where LLM returns "out_of_matrix"
      → match_method = "out_of_matrix",       match_confidence = 0.0

For every matched record a combination_path is constructed:
  canonical > subcategory > primary_unit [> accumulation_window] [> persistence]
              [> probability] [> lead_time] [> geographic_scope]

Outputs (written to ui_normalization_output/):
  taxonomy_match_results_v2.csv   — all records with combination_path + match quality
  out_of_matrix_review.csv        — out_of_matrix records for human / matrix-expansion review

Quality targets (logged at end):
  ≥ 90 % of records matched to a matrix row
  ≥ 90 % of records at match_confidence ≥ 0.90

Usage (from project root):
    python -m scripts.ui_normalization.step2b_matrix_matcher
    python -m scripts.ui_normalization.step2b_matrix_matcher --llm openai --limit 20
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Config import (works standalone or as package)
# ---------------------------------------------------------------------------
try:
    from .config import (
        AZURE_OPENAI_ENDPOINT,
        AZURE_OPENAI_API_KEY,
        AZURE_OPENAI_DEPLOYMENT,
        AZURE_OPENAI_API_VERSION,
        GEMINI_API_KEY,
        GEMINI_MODEL,
        LLM_TEMPERATURE,
        API_DELAY_SECONDS,
        MAX_RETRIES,
        COMBINATION_MATRIX_ENRICHED_CSV,
        NORMALIZED_V2_CSV_OPENAI,
        NORMALIZED_V2_CSV_GEMINI,
        NORMALIZED_CSV_OPENAI,
        NORMALIZED_CSV_GEMINI,
        OUTPUT_DIR,
    )
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        AZURE_OPENAI_ENDPOINT,
        AZURE_OPENAI_API_KEY,
        AZURE_OPENAI_DEPLOYMENT,
        AZURE_OPENAI_API_VERSION,
        GEMINI_API_KEY,
        GEMINI_MODEL,
        LLM_TEMPERATURE,
        API_DELAY_SECONDS,
        MAX_RETRIES,
        COMBINATION_MATRIX_ENRICHED_CSV,
        NORMALIZED_V2_CSV_OPENAI,
        NORMALIZED_V2_CSV_GEMINI,
        NORMALIZED_CSV_OPENAI,
        NORMALIZED_CSV_GEMINI,
        OUTPUT_DIR,
    )

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
TAXONOMY_MATCH_RESULTS_CSV = OUTPUT_DIR / "taxonomy_match_results_v2.csv"
OUT_OF_MATRIX_REVIEW_CSV   = OUTPUT_DIR / "out_of_matrix_review.csv"

# ---------------------------------------------------------------------------
# Output columns (preserve source columns + add Phase 2 columns)
# ---------------------------------------------------------------------------
PHASE2_COLUMNS = [
    "combination_path",
    "matched_canonical",
    "matched_subcategory",
    "matched_unit",
    "match_method",
    "match_confidence",
    "out_of_matrix",
]

# ---------------------------------------------------------------------------
# Type-A canonical/subcategory alias map
# Corrects Phase 1 misrouting before Pass 1 deterministic lookup.
# Key:   (canonical_variable.lower(), subcategory.lower()) as extracted by Phase 1
# Value: (correct_canonical_variable, correct_subcategory) as present in matrix
# ---------------------------------------------------------------------------
_CANONICAL_ALIAS_MAP: dict[tuple[str, str], tuple[str, str]] = {
    # Type-A: Phase 1 assigned wrong canonical entirely
    ("alert/warning status", "cold wave"):    ("Temperature", "Cold wave event"),
    ("alert/warning status", "el ni\u00f1o alert"): ("Other",  "ENSO index"),
    # Temperature subcategory consolidations (removed rows → surviving canonical row)
    ("temperature", "cold wave"):                       ("Temperature", "Cold wave event"),
    ("temperature", "daytime temperature"):             ("Temperature", "Max daily temperature"),
    ("temperature", "general temperature"):             ("Temperature", "Min temperature"),
    ("temperature", "heatwave occurrence probability"): ("Temperature", "Heatwave occurrence"),
    ("temperature", "max daily temp"):                  ("Temperature", "Max daily temperature"),
    ("temperature", "max temperature"):                 ("Temperature", "Max daily temperature"),
}

# ---------------------------------------------------------------------------
# System prompt for LLM disambiguation
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = """\
You are a taxonomy-matching specialist for humanitarian Emergency Action Plan (EAP) trigger analysis.

You are given a single extracted EAP threshold condition record (JSON) and a list of
combination matrix rows (JSON array, each with an "index" field).

Your task: identify which matrix row is the best match for the record, or declare it
"out_of_matrix" if no row fits.

Matching rules:
1. The primary keys are canonical_variable and subcategory.
   Prefer rows where both match the record's canonical_variable and subcategory fields.
2. If no subcategory matches exactly, find the closest semantically equivalent subcategory
   under the same canonical_variable. Partial text matches and synonym equivalences count.
3. Use the threshold_text and threshold_unit as additional signals to break ties.
4. Only return "out_of_matrix" if the canonical_variable itself does not appear in the
   matrix at all, or if the threshold represents a genuinely novel concept.
5. Return a confidence score (0.0–1.0) reflecting how certain you are of the match:
   1.0 = exact; 0.8–0.99 = strong; 0.5–0.79 = uncertain; < 0.5 = very weak.
   For out_of_matrix, return 0.0.

Return ONLY a valid JSON object with these fields (no markdown, no extra text):
{
  "matched_index": <integer matrix row index, or null if out_of_matrix>,
  "out_of_matrix": <true or false>,
  "confidence": <float 0.0–1.0>,
  "reasoning": "<one-sentence explanation>"
}
"""


# ---------------------------------------------------------------------------
# LLM clients (lazy singletons)
# ---------------------------------------------------------------------------
_openai_client = None


_RATE_LIMIT_WAIT_SECONDS = 65  # wait after a 429 before retrying


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import AzureOpenAI
        # max_retries=0: disable SDK auto-retry so our caller controls backoff
        _openai_client = AzureOpenAI(
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_API_KEY,
            api_version=AZURE_OPENAI_API_VERSION,
            max_retries=0,
        )
    return _openai_client


_gemini_client = None


def _get_gemini_client():
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        _gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    return _gemini_client


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def _clean_json(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("```"):
        parts = text.split("```")
        if len(parts) >= 2:
            text = parts[1]
            if text.lstrip().startswith("json"):
                text = text.lstrip()[4:]
    return text.strip()


def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True if the exception is an HTTP 429 / RateLimitError."""
    # openai SDK raises openai.RateLimitError for 429s
    try:
        from openai import RateLimitError
        if isinstance(exc, RateLimitError):
            return True
    except ImportError:
        pass
    # Fallback: inspect the string representation
    msg = str(exc).lower()
    return "429" in msg or "rate limit" in msg or "too many requests" in msg


def _safe_float(val: Any) -> Optional[float]:
    """Return val as float or None if absent/NaN/non-numeric."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _safe_str(val: Any) -> Optional[str]:
    """Return stripped string or None if absent/NaN."""
    if val is None:
        return None
    s = str(val).strip()
    return None if s.lower() in ("nan", "", "none") else s


# ---------------------------------------------------------------------------
# Combination path builder
# ---------------------------------------------------------------------------

def build_combination_path(
    canonical: str,
    subcategory: str,
    primary_unit: Optional[str],
    accumulation_window_value: Optional[float] = None,
    accumulation_window_unit: Optional[str] = None,
    persistence_value: Optional[float] = None,
    persistence_unit: Optional[str] = None,
    probability_value: Optional[float] = None,
    lead_time_value: Optional[float] = None,
    lead_time_unit: Optional[str] = None,
    geographic_scope_type: Optional[str] = None,
    geographic_scope_label: Optional[str] = None,
) -> str:
    """Build a canonical combination_path string from record fields.

    Format:
      canonical > subcategory > primary_unit [> accumulation_window]
                  [> persistence] [> probability] [> lead_time] [> geographic_scope]

    Absent optional segments are rendered as "—".
    """
    unit_seg = primary_unit or "—"

    # Accumulation window: "72 hours" or "—"
    if accumulation_window_value is not None and accumulation_window_unit:
        aw_v = int(accumulation_window_value) if accumulation_window_value == int(accumulation_window_value) else accumulation_window_value
        acc_seg = f"{aw_v} {accumulation_window_unit}"
    else:
        acc_seg = "—"

    # Persistence: "3 days" or "—"
    if persistence_value is not None and persistence_unit:
        p_v = int(persistence_value) if persistence_value == int(persistence_value) else persistence_value
        pers_seg = f"{p_v} {persistence_unit}"
    else:
        pers_seg = "—"

    # Probability: "70%" or "—"
    if probability_value is not None:
        p_val = int(probability_value) if probability_value == int(probability_value) else probability_value
        prob_seg = f"{p_val}%"
    else:
        prob_seg = "—"

    # Lead time: "10 days" or "—"
    if lead_time_value is not None and lead_time_unit:
        lt_v = int(lead_time_value) if lead_time_value == int(lead_time_value) else lead_time_value
        lt_seg = f"{lt_v} {lead_time_unit}"
    elif lead_time_value is not None:
        lt_seg = f"{lead_time_value}"
    else:
        lt_seg = "—"

    # Geographic scope: "station_gauge: Kotri" or "national" or "—"
    scope = _safe_str(geographic_scope_type) or "national"
    label = _safe_str(geographic_scope_label)
    geo_seg = f"{scope}: {label}" if label else scope

    return (
        f"{canonical} > {subcategory} > {unit_seg} > "
        f"{acc_seg} > {pers_seg} > {prob_seg} > {lt_seg} > {geo_seg}"
    )


# ---------------------------------------------------------------------------
# Load combination matrix
# ---------------------------------------------------------------------------

def load_matrix(matrix_path: Path) -> pd.DataFrame:
    """Load combination matrix and add 'index' column for LLM matching."""
    df = pd.read_csv(matrix_path)
    df = df.reset_index(drop=True)
    df["index"] = df.index
    return df


def _matrix_units_set(row: pd.Series) -> set[str]:
    """Return lowercase set of allowed units for a matrix row."""
    raw = _safe_str(row.get("primary_unit", ""))
    if not raw:
        return set()
    return {u.strip().lower() for u in raw.split("|") if u.strip()}


# ---------------------------------------------------------------------------
# Pass 1 — Deterministic matching
# ---------------------------------------------------------------------------

def deterministic_match(
    record_canonical: Optional[str],
    record_subcategory: Optional[str],
    record_unit: Optional[str],
    matrix: pd.DataFrame,
) -> tuple[Optional[pd.Series], str, float]:
    """Match a record deterministically against the matrix.

    Returns (matched_row_or_None, method, confidence).

    method values:
      "deterministic"       — canonical + subcategory + unit all match
      "deterministic_alias" — canonical + subcategory match; unit absent or not in list
      "no_match"            — not matched at all
    """
    if not record_canonical:
        return None, "no_match", 0.0

    # Normalise for comparison
    c_norm = record_canonical.strip().lower()
    s_norm = (record_subcategory or "").strip().lower()
    u_norm = (record_unit or "").strip().lower()

    # Filter rows by canonical_variable (case-insensitive)
    canon_mask = matrix["canonical_variable"].str.strip().str.lower() == c_norm
    canon_rows = matrix[canon_mask]

    if canon_rows.empty:
        return None, "no_match", 0.0

    # Further filter by subcategory
    if s_norm:
        sub_mask = canon_rows["subcategory"].str.strip().str.lower() == s_norm
        sub_rows = canon_rows[sub_mask]
    else:
        sub_rows = pd.DataFrame()

    if sub_rows.empty:
        # No exact subcategory match — cannot deterministically assign
        return None, "no_match", 0.0

    # sub_rows now contains 1+ rows with matching canonical + subcategory
    if len(sub_rows) == 1:
        row = sub_rows.iloc[0]
    else:
        # Prefer first matching row (matrix should not have duplicate canonical×subcategory)
        row = sub_rows.iloc[0]

    # Unit check
    if u_norm and u_norm in _matrix_units_set(row):
        return row, "deterministic", 1.0
    else:
        return row, "deterministic_alias", 0.95


# ---------------------------------------------------------------------------
# Pass 2 — LLM disambiguation
# ---------------------------------------------------------------------------

def _build_llm_user_message(record: pd.Series, matrix: pd.DataFrame) -> str:
    """Construct the LLM user message for disambiguation."""
    # Subset of record fields relevant for matching
    rec_dict = {
        "threshold_text":       _safe_str(record.get("threshold_text")),
        "canonical_variable":   _safe_str(record.get("canonical_variable")),
        "subcategory":          _safe_str(record.get("subcategory")),
        "threshold_unit":       _safe_str(record.get("threshold_unit")),
        "forecast_variable":    _safe_str(record.get("forecast_variable")),
        "hazard_type":          _safe_str(record.get("hazard_type")),
        "is_observational":     record.get("is_observational"),
    }

    # Build compact matrix list with just matching-relevant columns
    matrix_rows = []
    for _, mrow in matrix.iterrows():
        matrix_rows.append({
            "index":            int(mrow["index"]),
            "canonical_variable": _safe_str(mrow.get("canonical_variable")),
            "subcategory":      _safe_str(mrow.get("subcategory")),
            "primary_unit":     _safe_str(mrow.get("primary_unit")),
            "example_statement":_safe_str(mrow.get("example_statement")),
            "hazard_types":     _safe_str(mrow.get("hazard_types")),
        })

    return json.dumps({
        "record": rec_dict,
        "matrix_rows": matrix_rows,
    }, ensure_ascii=False, indent=2)


def _call_openai_llm(user_message: str) -> dict:
    client = _get_openai_client()
    resp = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        temperature=LLM_TEMPERATURE,
        max_tokens=300,
        response_format={"type": "json_object"},
    )
    raw = (resp.choices[0].message.content or "").strip()
    return json.loads(_clean_json(raw))


def _call_gemini_llm(user_message: str) -> dict:
    client = _get_gemini_client()
    full_prompt = f"{_SYSTEM_PROMPT}\n\n{user_message}"
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=full_prompt,
        config={"temperature": LLM_TEMPERATURE, "max_output_tokens": 300},
    )
    raw = (resp.text or "").strip()
    return json.loads(_clean_json(raw))


def llm_match(
    record: pd.Series,
    matrix: pd.DataFrame,
    llm: str = "openai",
) -> tuple[Optional[pd.Series], str, float]:
    """Call the LLM to disambiguate a record against the combination matrix.

    Returns (matched_row_or_None, method, confidence).
    """
    user_message = _build_llm_user_message(record, matrix)

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if llm == "openai":
                result = _call_openai_llm(user_message)
            else:
                result = _call_gemini_llm(user_message)

            if result.get("out_of_matrix") or result.get("matched_index") is None:
                return None, "out_of_matrix", 0.0

            idx = int(result["matched_index"])
            conf = float(result.get("confidence", 0.8))
            conf = max(0.0, min(1.0, conf))

            matching_rows = matrix[matrix["index"] == idx]
            if matching_rows.empty:
                logger.warning(
                    "LLM returned index %d which does not exist in matrix; treating as out_of_matrix",
                    idx,
                )
                return None, "out_of_matrix", 0.0

            return matching_rows.iloc[0], "llm", conf

        except Exception as exc:
            last_exc = exc
            # Detect 429 / RateLimitError and apply a long wait so the
            # Azure TPM window (60 s) resets before we retry.
            is_rate_limit = _is_rate_limit_error(exc)
            if is_rate_limit:
                logger.warning(
                    "LLM match attempt %d/%d — rate limit (429). Waiting %ds before retry …",
                    attempt, MAX_RETRIES, _RATE_LIMIT_WAIT_SECONDS,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(_RATE_LIMIT_WAIT_SECONDS)
            else:
                logger.warning(
                    "LLM match attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc
                )
                if attempt < MAX_RETRIES:
                    time.sleep(API_DELAY_SECONDS * attempt)

    logger.error("All LLM match attempts failed for record; treating as out_of_matrix. Last error: %s", last_exc)
    return None, "out_of_matrix", 0.0


# ---------------------------------------------------------------------------
# Per-record match orchestrator
# ---------------------------------------------------------------------------

def match_record(
    record: pd.Series,
    matrix: pd.DataFrame,
    llm: str = "openai",
) -> dict[str, Any]:
    """Match one record against the combination matrix.

    Returns a dict with PHASE2_COLUMNS values.
    """
    canonical  = _safe_str(record.get("canonical_variable"))
    subcategory = _safe_str(record.get("subcategory"))
    unit        = _safe_str(record.get("threshold_unit"))

    # Apply Type-A alias remapping before Pass 1 (corrects Phase 1 canonical misrouting)
    alias_key = (
        (canonical or "").lower().strip(),
        (subcategory or "").lower().strip(),
    )
    if alias_key in _CANONICAL_ALIAS_MAP:
        remapped_canonical, remapped_subcategory = _CANONICAL_ALIAS_MAP[alias_key]
        logger.debug(
            "Alias remap: (%s, %s) → (%s, %s)",
            canonical, subcategory, remapped_canonical, remapped_subcategory,
        )
        canonical   = remapped_canonical
        subcategory = remapped_subcategory

    # Pass 1
    matched_row, method, confidence = deterministic_match(
        canonical, subcategory, unit, matrix
    )

    # Pass 2 — LLM disambiguation for unmatched records
    if method == "no_match":
        time.sleep(API_DELAY_SECONDS)
        matched_row, method, confidence = llm_match(record, matrix, llm=llm)

    # Build outputs
    out_of_matrix = (method == "out_of_matrix")

    if matched_row is not None:
        matched_canonical  = _safe_str(matched_row.get("canonical_variable")) or canonical
        matched_subcategory = _safe_str(matched_row.get("subcategory")) or subcategory
        # Use the record's actual unit (may not be in matrix list yet); fall back to matrix primary_unit
        matched_unit = unit or (_safe_str(matched_row.get("primary_unit")) or "").split("|")[0].strip()

        # Resolve lead_time_unit: prefer timeframe_unit from record
        lead_time_unit = _safe_str(record.get("timeframe_unit"))

        combination_path = build_combination_path(
            canonical=matched_canonical,
            subcategory=matched_subcategory,
            primary_unit=matched_unit,
            accumulation_window_value=_safe_float(record.get("accumulation_window_value")),
            accumulation_window_unit=_safe_str(record.get("accumulation_window_unit")),
            persistence_value=_safe_float(record.get("persistence_value")),
            persistence_unit=_safe_str(record.get("persistence_unit")),
            probability_value=_safe_float(record.get("probability_value")),
            lead_time_value=_safe_float(record.get("lead_time_value")),
            lead_time_unit=lead_time_unit,
            geographic_scope_type=_safe_str(record.get("geographic_scope_type")),
            geographic_scope_label=_safe_str(record.get("geographic_scope_label")),
        )
    else:
        matched_canonical   = canonical or ""
        matched_subcategory = subcategory or ""
        matched_unit        = unit or ""
        combination_path    = ""

    return {
        "combination_path":   combination_path,
        "matched_canonical":  matched_canonical,
        "matched_subcategory": matched_subcategory,
        "matched_unit":       matched_unit,
        "match_method":       method,
        "match_confidence":   round(confidence, 4),
        "out_of_matrix":      out_of_matrix,
    }


# ---------------------------------------------------------------------------
# DataFrame-level runner
# ---------------------------------------------------------------------------

def match_dataframe(
    input_csv: Path,
    matrix_csv: Path,
    output_csv: Path,
    out_of_matrix_csv: Path,
    llm: str = "openai",
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """Match all records in input_csv against matrix_csv and write outputs.

    Args:
        input_csv:         Phase 1 output CSV (normalized_thresholds_v2_openai.csv or v1).
        matrix_csv:        Combination matrix CSV (combination_matrix_enriched.csv).
        output_csv:        Where to write all matched results.
        out_of_matrix_csv: Where to write out-of-matrix records for human review.
        llm:               "openai" or "gemini".
        limit:             Only process first N records (for testing).

    Returns:
        Full matched DataFrame.
    """
    logger.info("Loading input: %s", input_csv)
    df = pd.read_csv(input_csv)

    logger.info("Loading combination matrix: %s", matrix_csv)
    matrix = load_matrix(matrix_csv)
    logger.info("Matrix: %d rows", len(matrix))

    if limit:
        logger.info("  ⚠️  --limit %d applied", limit)
        df = df.head(limit)

    total = len(df)
    logger.info("Records to match: %d", total)

    # Checkpoint: resume from last completed row
    results: list[dict[str, Any]] = []
    start_idx = 0

    if output_csv.exists():
        existing = pd.read_csv(output_csv)
        phase2_cols_present = all(c in existing.columns for c in PHASE2_COLUMNS)
        if phase2_cols_present and len(existing) > 0:
            start_idx = len(existing)
            # Reconstruct results list from existing output
            for _, row in existing.iterrows():
                results.append({c: row[c] for c in PHASE2_COLUMNS if c in row})
            logger.info("  Checkpoint: resuming from record %d / %d", start_idx, total)

    for i, (_, record) in enumerate(df.iterrows()):
        if i < start_idx:
            continue

        record_label = (
            f"{_safe_str(record.get('document_id'))} / "
            f"{_safe_str(record.get('statement_key'))} / "
            f"idx={record.get('threshold_index', i)}"
        )

        logger.debug("Matching record %d/%d: %s", i + 1, total, record_label)

        try:
            match_result = match_record(record, matrix, llm=llm)
        except Exception as exc:
            logger.error("Record %d failed unexpectedly: %s", i + 1, exc)
            match_result = {
                "combination_path":    "",
                "matched_canonical":   _safe_str(record.get("canonical_variable")) or "",
                "matched_subcategory": _safe_str(record.get("subcategory")) or "",
                "matched_unit":        _safe_str(record.get("threshold_unit")) or "",
                "match_method":        "error",
                "match_confidence":    0.0,
                "out_of_matrix":       True,
            }

        results.append(match_result)

        # Progress log every 25 records
        if (i + 1) % 25 == 0 or (i + 1) == total:
            matched_so_far = sum(1 for r in results if not r["out_of_matrix"])
            logger.info(
                "  Progress: %d / %d  (matched: %d, out_of_matrix: %d)",
                i + 1, total,
                matched_so_far, len(results) - matched_so_far,
            )

        # Checkpoint: save after every 25 records
        if (i + 1) % 25 == 0 or (i + 1) == total:
            _save_checkpoint(df, results, output_csv, start_idx=0)

    # Final merged output
    result_df = pd.DataFrame(results)
    merged = _merge_results(df, result_df)
    merged.to_csv(output_csv, index=False)
    logger.info("Wrote %d records to %s", len(merged), output_csv)

    # Quality report
    _log_quality_report(merged)

    # Out-of-matrix export
    oom = merged[merged["out_of_matrix"] == True].copy()  # noqa: E712
    if not oom.empty:
        oom_cols = [
            "document_id", "document_name", "statement_key", "threshold_index",
            "threshold_text", "canonical_variable", "subcategory", "threshold_unit",
            "matched_canonical", "matched_subcategory",
            "match_method", "match_confidence",
            "geographic_scope_type", "geographic_scope_label",
        ]
        oom_export_cols = [c for c in oom_cols if c in oom.columns]
        oom[oom_export_cols].to_csv(out_of_matrix_csv, index=False)
        logger.info("Wrote %d out-of-matrix records to %s", len(oom), out_of_matrix_csv)
    else:
        logger.info("No out-of-matrix records — skipping %s", out_of_matrix_csv)

    return merged


def _merge_results(
    source: pd.DataFrame,
    result_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge Phase 2 result columns into source DataFrame."""
    # Align lengths (result_df may be shorter if limit was applied)
    n = len(result_df)
    src = source.iloc[:n].reset_index(drop=True)
    result_df = result_df.reset_index(drop=True)

    for col in PHASE2_COLUMNS:
        if col in result_df.columns:
            src[col] = result_df[col]

    return src


def _save_checkpoint(
    source: pd.DataFrame,
    results: list[dict[str, Any]],
    output_csv: Path,
    start_idx: int = 0,
) -> None:
    """Write interim results to output_csv for checkpointing."""
    n = len(results)
    src = source.iloc[:n].reset_index(drop=True)
    result_df = pd.DataFrame(results).reset_index(drop=True)
    for col in PHASE2_COLUMNS:
        if col in result_df.columns:
            src[col] = result_df[col]
    src.to_csv(output_csv, index=False)


def _log_quality_report(df: pd.DataFrame) -> None:
    """Log Phase 2 quality metrics."""
    total = len(df)
    if total == 0:
        logger.warning("Quality report: no records.")
        return

    matched = df[df["out_of_matrix"] == False]  # noqa: E712
    oom     = df[df["out_of_matrix"] == True]   # noqa: E712
    high_conf = matched[matched["match_confidence"] >= 0.90] if "match_confidence" in matched.columns else matched

    match_pct = 100 * len(matched) / total
    high_pct  = 100 * len(high_conf) / total

    method_counts = matched["match_method"].value_counts().to_dict() if "match_method" in matched.columns else {}

    logger.info("=" * 60)
    logger.info("PHASE 2 QUALITY REPORT")
    logger.info("  Total records:           %d", total)
    logger.info("  Matched (in-matrix):     %d  (%.1f%%)", len(matched), match_pct)
    logger.info("  Out-of-matrix:           %d  (%.1f%%)", len(oom), 100 * len(oom) / total)
    logger.info("  High-confidence (≥0.90): %d  (%.1f%%)", len(high_conf), high_pct)
    for method, count in sorted(method_counts.items()):
        logger.info("    %-30s %d", method + ":", count)

    if match_pct < 90.0:
        logger.warning("  ⚠️  Match rate %.1f%% is below the 90%% target.", match_pct)
    else:
        logger.info("  ✅ Match rate target met (≥90%%).")

    if high_pct < 90.0:
        logger.warning("  ⚠️  High-confidence rate %.1f%% is below the 90%% target.", high_pct)
    else:
        logger.info("  ✅ High-confidence target met (≥90%%).")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_phase2_matching(
    llm: str = "openai",
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """Run Phase 2 matrix matching.

    Selects input CSV automatically:
      1. normalized_thresholds_v2_openai.csv  (Phase 1 enhanced output)
      2. normalized_thresholds_openai.csv      (fallback: Phase 0 output)

    Args:
        llm:   LLM to use for disambiguation — "openai" or "gemini".
        limit: Only process first N records (for testing).

    Returns:
        Matched DataFrame.
    """
    # Resolve input
    if llm == "openai":
        preferred_input  = NORMALIZED_V2_CSV_OPENAI
        fallback_input   = NORMALIZED_CSV_OPENAI
    else:
        preferred_input  = NORMALIZED_V2_CSV_GEMINI
        fallback_input   = NORMALIZED_CSV_GEMINI

    if preferred_input.exists():
        input_csv = preferred_input
        logger.info("Using Phase 1 enhanced input: %s", input_csv)
    elif fallback_input.exists():
        input_csv = fallback_input
        logger.info("Phase 1 v2 CSV not found — falling back to: %s", input_csv)
    else:
        raise FileNotFoundError(
            f"No input CSV found. Expected {preferred_input} or {fallback_input}. "
            "Run --phase1-enhanced or --step2-only first."
        )

    if not COMBINATION_MATRIX_ENRICHED_CSV.exists():
        raise FileNotFoundError(
            f"Combination matrix not found at {COMBINATION_MATRIX_ENRICHED_CSV}. "
            "Run --phase0-matrix-enrich first."
        )

    return match_dataframe(
        input_csv=input_csv,
        matrix_csv=COMBINATION_MATRIX_ENRICHED_CSV,
        output_csv=TAXONOMY_MATCH_RESULTS_CSV,
        out_of_matrix_csv=OUT_OF_MATRIX_REVIEW_CSV,
        llm=llm,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 2 — LLM Taxonomy Matching Against Combination Matrix"
    )
    parser.add_argument(
        "--llm",
        choices=["openai", "gemini"],
        default="openai",
        help="LLM to use for disambiguation (default: openai)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N records (for testing)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    args = _parse_args()
    run_phase2_matching(llm=args.llm, limit=args.limit)
