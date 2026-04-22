"""
Phase 0.1b — LLM Enrichment of the Combination Matrix Draft.

Reads combination_matrix_draft.csv (produced by step0_matrix_generator.py)
and calls the LLM to:
  1. Enrich every existing row (correct applicability flags, fill units, add
     geographic_scope_required, write/improve example_statement, assign
     hazard_types, add reviewer notes).
  2. Propose NEW rows for EAP-domain subcategories that are absent from the
     extracted corpus but are common in humanitarian EAP practice.

Outputs:
  ui_normalization_output/combination_matrix_enriched.csv
  (this file goes to the human reviewer who will approve it as
   combination_matrix_v1.xlsx — Phase 0.1c)

Usage (standalone):
    python -m scripts.ui_normalization.step0b_llm_enrichment
    python -m scripts.ui_normalization.step0b_llm_enrichment --llm openai
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd

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
        COMBINATION_MATRIX_DRAFT_CSV,
        COMBINATION_MATRIX_ENRICHED_CSV,
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
        COMBINATION_MATRIX_DRAFT_CSV,
        COMBINATION_MATRIX_ENRICHED_CSV,
    )

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Expected output columns (order preserved in CSV)
# ---------------------------------------------------------------------------
MATRIX_COLUMNS = [
    "canonical_variable",
    "subcategory",
    "primary_unit",
    "accumulation_window_applicable",
    "accumulation_window_units",
    "persistence_applicable",
    "persistence_units",
    "probability_applicable",
    "lead_time_applicable",
    "lead_time_units",
    "geographic_scope_required",
    "example_statement",
    "hazard_types",
    "notes",
    "data_source",
    "row_count",
    "source_eap_count",
]

# Allowed enum values — used for post-LLM validation
_ALLOWED_YES_NO_CONDITIONAL = {"yes", "no", "conditional"}
_ALLOWED_PROB_LEAD = {"yes", "no", "optional"}
_ALLOWED_GEO = {"always", "optional", "not_applicable"}

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a combination-matrix architect for humanitarian Emergency Action Plans (EAPs).

The combination matrix is a universal reference table that lists every valid
(canonical_variable × subcategory) combination used in EAP trigger statements,
together with rules about which optional fields apply to each combination.

Field definitions:
  canonical_variable       — top-level variable class (e.g. "Precipitation", "Hydrological Flow")
  subcategory              — specific type within the canonical (e.g. "Total rainfall", "River discharge")
  primary_unit             — pipe-separated list of valid measurement units (e.g. "mm|cm|inches")
  accumulation_window_applicable — does "amount measured over X hours/days" apply?
                             values: yes / no / conditional
  accumulation_window_units — pipe-separated unit options (hours|days|weeks|months) or "n/a"
  persistence_applicable   — does "sustained for X consecutive days/hours" apply?
                             values: yes / no
  persistence_units        — hours|days or "n/a"
  probability_applicable   — can a forecast probability (%) be attached?
                             values: yes / no / optional
  lead_time_applicable     — can a forecast lead time be attached?
                             values: yes / no / optional
  lead_time_units          — pipe-separated unit options or "n/a"
  geographic_scope_required — is geographic scope mandatory, optional, or not applicable?
                             values: always / optional / not_applicable
  example_statement        — one realistic EAP trigger statement (verbatim from corpus if possible)
  hazard_types             — pipe-separated list from: flood|drought|heatwave|cholera|cold_wave|cyclone|volcano|other
  notes                    — brief reviewer note about any domain rule, exception, or ambiguity
  data_source              — "auto_inferred" for rows from the corpus; "llm_proposed" for new rows you add

Canonical variables in scope (do not create others):
  - Precipitation
  - Hydrological Flow
  - Wind
  - Temperature
  - Infectious Disease
  - Population Impact
  - Agricultural Impact
  - Alert/Warning Status
  - Other

Domain rules you must apply:
  - accumulation_window_applicable = "yes" ONLY for Precipitation subcategories that measure
    rainfall over a time window (Total rainfall, Short-duration rainfall, etc.).
    For seasonal indices (SPI, EFI, seasonal total) use "no" — they have fixed climatological periods.
  - persistence_applicable = "yes" for: Temperature (heatwave/cold wave definitions require
    sustained days), Hydrological Flow (consecutive days above threshold), Wind (sustained winds).
  - probability_applicable = "optional" for all forecast-based subcategories not yet observed
    with a stated probability in the corpus.
  - lead_time_applicable = "optional" for all forecast-based subcategories.
  - lead_time_applicable = "no" for observational subcategories (e.g. "Cholera cases" which
    are real-time counts, not forecasts; "Flood extent" when measured, not forecast).
  - geographic_scope_required = "always" for station-gauge and named-basin subcategories.
  - geographic_scope_required = "optional" for national/regional default subcategories.

In addition to enriching existing rows, add NEW rows for subcategories known in EAP
practice but absent from the corpus. Required additions:
  Under Infectious Disease:
    - "AWD / Acute Watery Diarrhoea cases"
    - "Malnutrition rate (GAM)"
  Under Precipitation:
    - "Extreme precipitation index (EFI)"
    - "Standardised Precipitation Index (SPI)"
    - "Rainfall anomaly / departure from normal"
  Under Temperature:
    - "Heat index"
    - "Wind chill index"
    - "Temperature anomaly / departure from normal"
  Under Hydrological Flow:
    - "Return period exceedance"  (if not already present)
    - "Water level"               (if not already present)
  Under Agricultural Impact:
    - "Pasture / fodder condition score"  (if not already present)
  Under Alert/Warning Status:
    - "Colour alert level"
    - "Storm signal level"
    - "Advisory bulletin issuance"

Always return ONLY a valid JSON object with a single key "rows" whose value is an array, e.g.:
{"rows": [{...}, {...}]}

No markdown fences, no extra keys, no comments. Each element of the array must contain exactly the fields listed above.
"""


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


# ---------------------------------------------------------------------------
# LLM clients
# ---------------------------------------------------------------------------

_openai_client = None


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import AzureOpenAI
        _openai_client = AzureOpenAI(
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_API_KEY,
            api_version=AZURE_OPENAI_API_VERSION,
        )
    return _openai_client


_gemini_client = None


def _get_gemini_client():
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        _gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    return _gemini_client


def _call_openai(user_message: str) -> str:
    client = _get_openai_client()
    resp = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=LLM_TEMPERATURE,
        max_tokens=4000,
        response_format={"type": "json_object"},
    )
    content = (resp.choices[0].message.content or "").strip()
    # Surface any model-level error before returning
    parsed_preview = json.loads(content) if content.startswith("{") else {}
    if "error" in parsed_preview:
        raise ValueError(f"Model returned error payload: {parsed_preview['error']}")
    return content


def _call_gemini(user_message: str) -> str:
    client = _get_gemini_client()
    full_prompt = f"{_SYSTEM_PROMPT}\n\n{user_message}"
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=full_prompt,
        config={"temperature": LLM_TEMPERATURE},
    )
    return (response.text or "").strip()


def _llm_call_with_retries(llm: str, user_message: str) -> list[dict[str, Any]]:
    """Call the LLM and return the parsed JSON array, retrying on failure."""
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            if llm == "openai":
                raw = _call_openai(user_message)
            else:
                raw = _call_gemini(user_message)
            clean = _clean_json(raw)
            parsed = json.loads(clean)
            if isinstance(parsed, dict):
                # Expected: {"rows": [...]}
                if "rows" in parsed and isinstance(parsed["rows"], list):
                    return parsed["rows"]
                # Fallback: any key whose value is a list
                for key in parsed:
                    if isinstance(parsed[key], list):
                        return parsed[key]
                # Last resort: single-row object — wrap it
                row_keys = set(MATRIX_COLUMNS)
                if row_keys.intersection(set(parsed.keys())):
                    logger.debug("LLM returned single-row dict — wrapping in list")
                    return [parsed]
                raise ValueError(
                    f"LLM returned a dict with unrecognised keys: {list(parsed.keys())}"
                )
            if isinstance(parsed, list):
                return parsed
            raise ValueError(f"Unexpected JSON type: {type(parsed)}")
        except Exception as exc:
            last_error = exc
            logger.warning("LLM call attempt %d/%d failed: %s", attempt + 1, MAX_RETRIES, exc)
            time.sleep(API_DELAY_SECONDS)
    raise RuntimeError(f"LLM enrichment failed after {MAX_RETRIES} attempts") from last_error


# Batch size: keep each request well within GPT-3.5-Turbo's 4096-token output window
_ENRICH_BATCH_SIZE = 8
_NEW_ROWS_BATCH_SIZE = 6


def _enrich_batch(rows: list[dict], llm: str) -> list[dict[str, Any]]:
    """Enrich a small batch of existing rows and return the enriched list."""
    compact = [
        {
            "canonical_variable": r.get("canonical_variable", ""),
            "subcategory": r.get("subcategory", ""),
            "primary_unit": r.get("primary_unit", ""),
            "accumulation_window_applicable": r.get("accumulation_window_applicable", "no"),
            "accumulation_window_units": r.get("accumulation_window_units", "n/a"),
            "persistence_applicable": r.get("persistence_applicable", "no"),
            "persistence_units": r.get("persistence_units", "n/a"),
            "probability_applicable": r.get("probability_applicable", "no"),
            "lead_time_applicable": r.get("lead_time_applicable", "no"),
            "lead_time_units": r.get("lead_time_units", "n/a"),
            "geographic_scope_required": r.get("geographic_scope_required", "optional"),
            "example_statement": r.get("example_statement", ""),
            "hazard_types": r.get("hazard_types", "other"),
            "notes": r.get("notes", ""),
            "data_source": "auto_inferred",
            "row_count": r.get("row_count", 0),
            "source_eap_count": r.get("source_eap_count", 0),
        }
        for r in rows
    ]
    prompt = (
        "Enrich the following rows from the combination matrix draft following the "
        "domain rules in the system prompt. Set data_source = 'auto_inferred'. "
        "Return ONLY a JSON object with a single key 'rows' containing an array "
        "of the enriched rows.\n\n"
        + json.dumps(compact, indent=2, ensure_ascii=False)
    )
    return _llm_call_with_retries(llm, prompt)


def _propose_new_rows(existing_subcats: set[str], llm: str) -> list[dict[str, Any]]:
    """Ask the LLM to produce only the new rows for subcategories not yet in the draft."""
    prompt = (
        "The combination matrix already contains these subcategories:\n"
        + json.dumps(sorted(existing_subcats), indent=2, ensure_ascii=False)
        + "\n\nAdd ONLY the required subcategories listed in the system prompt that are "
        "NOT already present above. "
        "Set data_source = 'llm_proposed' and row_count = 0 for all new rows. "
        "Return a JSON object with a single key 'rows' containing an array of the new rows."
    )
    return _llm_call_with_retries(llm, prompt)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _coerce_row(row: dict) -> dict:
    """Ensure all required columns are present and enums are valid."""
    # Ensure all columns exist
    _numeric_cols = {"row_count", "source_eap_count"}
    for col in MATRIX_COLUMNS:
        if col not in row:
            if col in ("example_statement", "notes", "hazard_types"):
                row[col] = ""
            elif col in _numeric_cols:
                row[col] = 0
            else:
                row[col] = "n/a"

    # Coerce enum fields
    def _coerce_enum(key: str, allowed: set, default: str) -> None:
        v = str(row.get(key, "")).strip().lower().replace(" ", "_")
        if v not in allowed:
            logger.debug("Coercing %s value '%s' → '%s'", key, row.get(key), default)
            row[key] = default
        else:
            row[key] = v

    _coerce_enum("accumulation_window_applicable", _ALLOWED_YES_NO_CONDITIONAL, "no")
    _coerce_enum("persistence_applicable", {"yes", "no"}, "no")
    _coerce_enum("probability_applicable", _ALLOWED_PROB_LEAD, "optional")
    _coerce_enum("lead_time_applicable", _ALLOWED_PROB_LEAD, "optional")
    _coerce_enum("geographic_scope_required", _ALLOWED_GEO, "optional")

    # Ensure numeric provenance fields are ints (LLM may return "n/a", None, or empty string)
    def _safe_int(val: Any, default: int = 0) -> int:
        if val is None:
            return default
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    row["row_count"] = _safe_int(row.get("row_count", 0))
    row["source_eap_count"] = _safe_int(row.get("source_eap_count", 0))

    return row


# ---------------------------------------------------------------------------
# Main enrichment function
# ---------------------------------------------------------------------------

def enrich_matrix(
    input_csv: Path = COMBINATION_MATRIX_DRAFT_CSV,
    output_csv: Path = COMBINATION_MATRIX_ENRICHED_CSV,
    llm: str = "openai",
) -> pd.DataFrame:
    """
    Enrich the combination matrix draft with LLM domain knowledge and write
    combination_matrix_enriched.csv.

    Processing is split into small batches to stay within the model's output
    token window, then a separate call proposes new rows for subcategories
    absent from the corpus.

    Args:
        input_csv: Path to combination_matrix_draft.csv
        output_csv: Path to write the enriched output
        llm: "openai" or "gemini"

    Returns:
        Enriched DataFrame
    """
    if not input_csv.exists():
        raise FileNotFoundError(
            f"Draft matrix not found at {input_csv}. "
            "Run step0_matrix_generator.py first."
        )

    logger.info("Loading draft matrix from %s", input_csv)
    draft_df = pd.read_csv(input_csv)
    draft_rows = draft_df.to_dict(orient="records")
    logger.info("Draft has %d rows — enriching via %s (batch size %d)",
                len(draft_rows), llm, _ENRICH_BATCH_SIZE)

    # ── Pass 1: enrich existing rows in batches ──────────────────────────────
    enriched_rows: list[dict] = []
    for batch_start in range(0, len(draft_rows), _ENRICH_BATCH_SIZE):
        batch = draft_rows[batch_start: batch_start + _ENRICH_BATCH_SIZE]
        logger.info(
            "  Enriching rows %d–%d of %d …",
            batch_start + 1,
            min(batch_start + _ENRICH_BATCH_SIZE, len(draft_rows)),
            len(draft_rows),
        )
        enriched_batch = _enrich_batch(batch, llm)
        enriched_rows.extend(enriched_batch)
        time.sleep(API_DELAY_SECONDS)

    logger.info("Pass 1 done — %d enriched rows", len(enriched_rows))

    # ── Pass 2: propose new rows for subcategories not in the corpus ─────────
    existing_subcats = {
        str(r.get("subcategory", "")).strip()
        for r in enriched_rows
        if r.get("subcategory")
    }
    logger.info("Requesting new-row proposals for subcategories absent from corpus …")
    try:
        new_rows = _propose_new_rows(existing_subcats, llm)
        logger.info("  LLM proposed %d new rows", len(new_rows))
    except Exception as exc:
        logger.warning("New-row proposal failed (%s) — skipping, continuing with enriched rows only", exc)
        new_rows = []

    all_rows = enriched_rows + new_rows

    # ── Coerce and validate all rows ──────────────────────────────────────────
    all_rows = [_coerce_row(r) for r in all_rows]

    enriched_df = pd.DataFrame(all_rows, columns=MATRIX_COLUMNS)
    enriched_df.sort_values(["canonical_variable", "subcategory"], inplace=True)
    enriched_df.reset_index(drop=True, inplace=True)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    enriched_df.to_csv(output_csv, index=False)

    new_count = sum(
        1 for r in all_rows if str(r.get("data_source", "")).strip() == "llm_proposed"
    )
    logger.info(
        "Enriched matrix written → %s  (%d total rows; %d auto_inferred, %d llm_proposed)",
        output_csv,
        len(enriched_df),
        len(enriched_df) - new_count,
        new_count,
    )
    return enriched_df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 0.1b — LLM enrichment of the combination matrix draft"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=COMBINATION_MATRIX_DRAFT_CSV,
        help=f"Path to combination_matrix_draft.csv (default: {COMBINATION_MATRIX_DRAFT_CSV})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=COMBINATION_MATRIX_ENRICHED_CSV,
        help=f"Path to write enriched CSV (default: {COMBINATION_MATRIX_ENRICHED_CSV})",
    )
    parser.add_argument(
        "--llm",
        choices=["openai", "gemini"],
        default="openai",
        help="LLM to use for enrichment (default: openai)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )
    args = _parse_args()
    enrich_matrix(args.input, args.output, llm=args.llm)
