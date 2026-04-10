"""
Step 2 – LLM Normalization with Pydantic Structured Output.

For every exploded threshold record, calls an LLM (Azure OpenAI GPT-3.5-Turbo
as primary, Gemini Flash as fallback) and enforces the strict 8-field JSON
schema via a Pydantic model.

Output: normalized_thresholds_openai.csv  or  normalized_thresholds_gemini.csv
        (also returned as a DataFrame)
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, Field, field_validator, ValidationError

# ---------------------------------------------------------------------------
# Config import (works standalone or as package)
# ---------------------------------------------------------------------------
try:
    from .config import (
        AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY,
        AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION,
        GEMINI_API_KEY, GEMINI_MODEL,
        LLM_TEMPERATURE, LLM_MAX_TOKENS,
        API_DELAY_SECONDS, MAX_RETRIES,
        EXPLODED_CSV, NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI,
        TAXONOMY_PROPOSAL_JSON_OPENAI, TAXONOMY_PROPOSAL_JSON_GEMINI,
        FORECAST_VARIABLES_REFERENCE_JSON, REQUIRE_APPROVED_TAXONOMY,
    )
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY,
        AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION,
        GEMINI_API_KEY, GEMINI_MODEL,
        LLM_TEMPERATURE, LLM_MAX_TOKENS,
        API_DELAY_SECONDS, MAX_RETRIES,
        EXPLODED_CSV, NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI,
        TAXONOMY_PROPOSAL_JSON_OPENAI, TAXONOMY_PROPOSAL_JSON_GEMINI,
        FORECAST_VARIABLES_REFERENCE_JSON, REQUIRE_APPROVED_TAXONOMY,
    )

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pydantic schema – mirrors the exact UI fields
# ---------------------------------------------------------------------------

class NormalizedThreshold(BaseModel):
    """Strict schema for one normalized EAP threshold."""

    forecast_variable: str = Field(
        description=(
            "The core metric. Standardize synonyms. "
            "E.g. 'Rainfall total', 'River discharge', "
            "'Confirmed cholera cases', 'Affected population', "
            "'Inundation extent', 'Seasonal precipitation', "
            "'Wind speed', 'Flood return period probability'."
        )
    )
    threshold_operator: str = Field(
        description=(
            "Mathematical condition: '>=', '<=', '>', '<', '=', or 'between'. "
            "If 'lower tercile', use '<='."
        )
    )
    threshold_value: float = Field(
        description=(
            "Numerical tipping point. If a range (e.g. 800 to 1200), "
            "extract the minimum activation value (800). "
            "If text says 'one', output 1."
        )
    )
    threshold_unit: str = Field(
        description=(
            "Unit of measurement. E.g. 'mm', 'cusecs', 'cases', 'people', "
            "'%', 'knots', 'years', 'tercile', 'percentile', 'EFI index'."
        )
    )
    probability_value: Optional[float] = Field(
        default=None,
        description=(
            "Percentage likelihood of the forecast, stripped of '%'. "
            "Return null if none mentioned."
        )
    )
    lead_time_value: float = Field(
        description=(
            "Numerical warning time. If a range (3-5), extract the maximum value."
        )
    )
    timeframe_unit: str = Field(
        description=(
            "Must be exactly one of: 'hours', 'days', 'weeks', 'months'. "
            "Normalize 'dekads' to 'days' (1 dekad = 10 days)."
        )
    )
    normalized_source: str = Field(
        description=(
            "Clean, primary acronym of the forecasting agency. "
            "E.g. 'FFD', 'EPHI', 'GloFAS', 'DMH', 'ECMWF', 'CENAOS', 'NOAA'."
        )
    )

    @field_validator("threshold_operator")
    @classmethod
    def validate_operator(cls, v: str) -> str:
        allowed = {">=", "<=", ">", "<", "=", "between"}
        if v not in allowed:
            raise ValueError(f"threshold_operator must be one of {allowed}, got '{v}'")
        return v

    @field_validator("forecast_variable")
    @classmethod
    def validate_forecast_variable(cls, v: str) -> str:
        value = str(v).strip()
        if not value:
            raise ValueError("forecast_variable cannot be empty")

        value_norm = value.lower()
        forbidden_terms = {
            "lead time",
            "forecast lead time",
            "lead_time",
            "lead-time",
            "duration",
            "conditions duration",
            "forecast horizon",
            "forecast period",
            "timeframe",
            "time window",
        }
        if value_norm in forbidden_terms:
            raise ValueError(
                "forecast_variable cannot be temporal metadata such as lead time/duration/timeframe"
            )
        return value

    @field_validator("timeframe_unit")
    @classmethod
    def validate_timeframe(cls, v: str) -> str:
        allowed = {"hours", "days", "weeks", "months"}
        v_lower = v.lower().strip()
        if v_lower not in allowed:
            raise ValueError(f"timeframe_unit must be one of {allowed}, got '{v}'")
        return v_lower


# ---------------------------------------------------------------------------
# System prompt (shared across both LLMs)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert data normalization AI for a humanitarian disaster response platform.
Your objective is to extract precise quantitative parameters from messy, raw EAP (Early Action Protocol)
threshold strings into a strict JSON structure.

This data spans multiple hazard types: meteorological (rain, wind), hydrological (river discharge, cusecs),
epidemiological (cholera cases), and slow-onset (droughts, terciles).

INPUT DATA:
- Threshold String: The specific condition to be met.
- Source Authority: The agency issuing the forecast/data.
- Lead Time: The advanced warning time.
- Hazard Context: The type of disaster (e.g., Floods, Cholera, Drought).

EXTRACT AND NORMALIZE INTO THIS EXACT JSON SCHEMA (return ONLY valid JSON, no markdown):
{
  "forecast_variable": "String. The core metric. Standardize synonyms. (e.g., 'Rainfall total', 'River discharge', 'Confirmed cholera cases', 'Affected population', 'Inundation extent', 'Seasonal precipitation').",
  "threshold_operator": "String. The mathematical condition: '>=', '<=', '>', '<', '=', or 'between'. If 'lower tercile', use '<='.",
  "threshold_value": "Number. The numerical tipping point. If a range (e.g., 800 to 1200), extract the minimum activation value (800). If text says 'one', output 1.",
  "threshold_unit": "String. The unit of measurement. (e.g., 'mm', 'cusecs', 'cases', 'people', '%', 'years', 'tercile', 'percentile').",
  "probability_value": "Number or null. The percentage likelihood of the forecast, stripped of '%'. If none mentioned, return null.",
  "lead_time_value": "Number. The numerical warning time. If a range (3-5), extract the maximum value.",
  "timeframe_unit": "String. Must be exactly one of: 'hours', 'days', 'weeks', 'months'. Normalize 'dekads' to 'days' (1 dekad = 10 days).",
  "normalized_source": "String. The clean, primary acronym of the forecasting agency (e.g., 'FFD', 'EPHI', 'GloFAS', 'DMH')."
}

HANDLING EDGE CASES:
- Treat lead time, forecast horizon, duration, and timeframe as contextual metadata only. They must populate lead_time_value/timeframe_unit and must never be forecast_variable.
- If a threshold sentence is mainly a lead-time/duration condition, infer forecast_variable from the hazard metric context in the same sentence (e.g., flood flow/rainfall/wind/cases), not from time words.
- If dealing with human populations or cases (e.g., "32000 people", "one confirmed case"), map 'forecast_variable' to 'Affected population' or 'Confirmed cholera cases', and 'threshold_unit' to 'people' or 'cases'.
- If dealing with abstract climate probabilities like "lower tercile", map 'threshold_unit' to 'percentile' and 'forecast_variable' to 'Seasonal precipitation'.
- If no numeric lead time is present (e.g., "real-time"), set lead_time_value to 0 and timeframe_unit to 'hours'.
- If no source acronym is identifiable, use the first meaningful word from the source_authority field.
- Return ONLY the JSON object. No explanation, no markdown fences.

EXAMPLE 1 (Hydrology):
Input: Threshold="High flood inflows in Indus at Chashma of 550,000 cusecs or above in next 24 hours", Source="Flood Forecast Division (FFD)", Lead Time="48 hours", Hazard="Flood"
Output: {"forecast_variable":"River discharge","threshold_operator":">=","threshold_value":550000,"threshold_unit":"cusecs","probability_value":null,"lead_time_value":48,"timeframe_unit":"hours","normalized_source":"FFD"}

EXAMPLE 2 (Epidemiology):
Input: Threshold="One confirmed cholera case reported by EPHI in a neighboring woreda", Source="Ethiopian Public Health Institute (EPHI)", Lead Time="7 days", Hazard="Cholera"
Output: {"forecast_variable":"Confirmed cholera cases","threshold_operator":">=","threshold_value":1,"threshold_unit":"cases","probability_value":null,"lead_time_value":7,"timeframe_unit":"days","normalized_source":"EPHI"}

EXAMPLE 3 (Probabilistic Drought):
Input: Threshold="50% or greater probability that rainfall will remain in the lower tercile", Source="Directorate of Meteorology and Hydrology (DMH)", Lead Time="3 months", Hazard="Drought"
Output: {"forecast_variable":"Seasonal precipitation","threshold_operator":"<=","threshold_value":33,"threshold_unit":"percentile","probability_value":50,"lead_time_value":3,"timeframe_unit":"months","normalized_source":"DMH"}
"""


def _build_user_message(row: dict) -> str:
    """Format the per-record user message for the LLM."""
    return (
        f"Threshold: \"{row['threshold_text']}\"\n"
        f"Source Authority: \"{row['source_authority']}\"\n"
        f"Lead Time: \"{row['lead_time']}\"\n"
        f"Hazard Context: \"{row['hazard_type']}\"\n"
        f"Document: \"{row['document_name']}\"\n\n"
        "Return ONLY the JSON object."
    )


# ---------------------------------------------------------------------------
# Azure OpenAI client (lazy-initialised)
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


def _call_openai(user_message: str) -> str:
    """Call Azure OpenAI and return the raw text response."""
    client = _get_openai_client()
    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        temperature=LLM_TEMPERATURE,
        max_tokens=LLM_MAX_TOKENS,
        response_format={"type": "json_object"},   # GPT-3.5-Turbo JSON mode
    )
    return response.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# Gemini client (lazy-initialised, used as fallback)
# ---------------------------------------------------------------------------

_gemini_client = None

def _get_gemini_client():
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        _gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    return _gemini_client


def _call_gemini(user_message: str) -> str:
    """Call Gemini Flash and return the raw text response."""
    client = _get_gemini_client()
    full_prompt = f"{SYSTEM_PROMPT}\n\n---\n\n{user_message}"
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=full_prompt,
    )
    raw = response.text.strip()
    # Strip markdown fences if Gemini wraps the JSON
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return raw.strip()


def _call_openai_generic(system_prompt: str, user_message: str, max_tokens: int = 256) -> str:
    """Call Azure OpenAI with a custom system prompt and JSON output."""
    client = _get_openai_client()
    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        temperature=0.0,
        max_tokens=max_tokens,
        response_format={"type": "json_object"},
    )
    return response.choices[0].message.content.strip()


def _call_gemini_generic(system_prompt: str, user_message: str) -> str:
    """Call Gemini with a custom prompt and return raw text."""
    client = _get_gemini_client()
    full_prompt = f"{system_prompt}\n\n---\n\n{user_message}"
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=full_prompt,
    )
    raw = response.text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return raw.strip()


@dataclass
class TaxonomyContext:
    lookup: dict[str, dict[str, Any]]
    canonical_options: list[str]
    approved: bool
    source_path: Path | None
    source_type: str


def _extract_from_proposal(payload: dict[str, Any], source_path: Path) -> TaxonomyContext:
    metadata = payload.get("metadata", {})
    proposal = payload.get("proposal", {})
    mappings = proposal.get("mappings", [])
    canonical_raw = proposal.get("proposed_canonical_variables", [])

    canonical_options: list[str] = []
    for item in canonical_raw:
        name = str(item).strip()
        if name and name.lower() != "other" and name not in canonical_options:
            canonical_options.append(name)

    lookup: dict[str, dict[str, Any]] = {}
    for m in mappings:
        fv = str(m.get("forecast_variable", "")).strip()
        if fv:
            lookup[fv.lower()] = m

        for alias in m.get("alias_list", []):
            alias_name = str(alias).strip().lower()
            if alias_name:
                lookup[alias_name] = m

    approved = str(metadata.get("approval_status", "")).strip().lower() == "approved"
    return TaxonomyContext(
        lookup=lookup,
        canonical_options=canonical_options,
        approved=approved,
        source_path=source_path,
        source_type="proposal",
    )


def _extract_from_reference(payload: dict[str, Any], source_path: Path) -> TaxonomyContext | None:
    """Parse approved reference taxonomy if it uses canonical_variables schema."""
    canonical_block = payload.get("canonical_variables")
    if not isinstance(canonical_block, list):
        return None

    lookup: dict[str, dict[str, Any]] = {}
    canonical_options: list[str] = []

    for item in canonical_block:
        if not isinstance(item, dict):
            continue
        canonical_name = str(item.get("name", "")).strip()
        if not canonical_name:
            continue

        if canonical_name.lower() != "other" and canonical_name not in canonical_options:
            canonical_options.append(canonical_name)

        base_mapping = {
            "canonical_variable": canonical_name,
            "subcategory_name": canonical_name,
        }
        lookup[canonical_name.lower()] = base_mapping

        for alias in item.get("aliases", []):
            alias_name = str(alias).strip().lower()
            if alias_name:
                lookup[alias_name] = base_mapping

        for sub in item.get("subcategories", []):
            if isinstance(sub, dict):
                sub_name = str(sub.get("name", "")).strip()
                sub_aliases = sub.get("aliases", [])
            else:
                sub_name = str(sub).strip()
                sub_aliases = []

            if not sub_name:
                continue
            sub_mapping = {
                "canonical_variable": canonical_name,
                "subcategory_name": sub_name,
            }
            lookup[sub_name.lower()] = sub_mapping

            for alias in sub_aliases:
                alias_name = str(alias).strip().lower()
                if alias_name:
                    lookup[alias_name] = sub_mapping

    if not canonical_options:
        return None

    metadata = payload.get("metadata", {})
    approved_flag = str(metadata.get("approval_status", "approved")).strip().lower()
    approved = approved_flag in {"approved", "locked", "locked_approved"}
    return TaxonomyContext(
        lookup=lookup,
        canonical_options=canonical_options,
        approved=approved,
        source_path=source_path,
        source_type="reference",
    )


def _load_taxonomy_context(llm_choice: str, allow_unapproved_taxonomy: bool = False) -> TaxonomyContext:
    """Load taxonomy context with governance-aware approval checks."""
    proposal_path = TAXONOMY_PROPOSAL_JSON_OPENAI
    if llm_choice == "gemini":
        proposal_path = TAXONOMY_PROPOSAL_JSON_GEMINI

    candidates = [FORECAST_VARIABLES_REFERENCE_JSON, proposal_path]
    context: TaxonomyContext | None = None

    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Could not parse taxonomy JSON at %s: %s", path, exc)
            continue

        parsed = _extract_from_reference(payload, path)
        if parsed is None and "proposal" in payload:
            parsed = _extract_from_proposal(payload, path)

        if parsed is not None:
            context = parsed
            break

    if context is None:
        logger.warning("No taxonomy mapping found. All unmatched variables will route to Other.")
        return TaxonomyContext(lookup={}, canonical_options=[], approved=False, source_path=None, source_type="none")

    if REQUIRE_APPROVED_TAXONOMY and not allow_unapproved_taxonomy and not context.approved:
        raise RuntimeError(
            f"Taxonomy at {context.source_path} is not approved. "
            "Approve/lock taxonomy first or rerun with allow_unapproved_taxonomy=True for development."
        )

    logger.info(
        "Loaded taxonomy (%s) from %s with %d canonical options and %d alias entries.",
        context.source_type,
        context.source_path,
        len(context.canonical_options),
        len(context.lookup),
    )
    return context


def _build_disambiguation_prompt(row: dict, forecast_variable: str, canonical_options: list[str]) -> str:
    payload = {
        "task": "Map normalized variable to canonical taxonomy.",
        "rules": [
            "Pick exactly one canonical option or Other.",
            "Use hazard and threshold semantics, not lead time semantics.",
            "Return JSON only."
        ],
        "canonical_options": canonical_options + ["Other"],
        "input": {
            "forecast_variable": forecast_variable,
            "hazard_type": row.get("hazard_type"),
            "threshold_text": row.get("threshold_text"),
            "threshold_unit": row.get("threshold_unit"),
        },
        "required_output_schema": {
            "canonical_variable": "string",
            "subcategory": "string",
            "confidence": 0.0,
        },
    }
    return json.dumps(payload, ensure_ascii=True)


def _disambiguate_mapping_with_llm(
    row: dict,
    forecast_variable: str,
    canonical_options: list[str],
    llm_choice: str,
) -> tuple[str, str, float]:
    if not canonical_options or not forecast_variable:
        return "Other", "Other", 0.0

    system_prompt = (
        "You are a taxonomy disambiguation assistant for EAP forecast variables. "
        "Return only compact JSON with canonical_variable, subcategory, confidence."
    )
    user_prompt = _build_disambiguation_prompt(row=row, forecast_variable=forecast_variable, canonical_options=canonical_options)

    try:
        if llm_choice == "gemini":
            raw = _call_gemini_generic(system_prompt, user_prompt)
        elif llm_choice == "openai":
            raw = _call_openai_generic(system_prompt, user_prompt, max_tokens=220)
        else:
            try:
                raw = _call_openai_generic(system_prompt, user_prompt, max_tokens=220)
            except Exception:
                raw = _call_gemini_generic(system_prompt, user_prompt)

        parsed = json.loads(raw)
        canonical = str(parsed.get("canonical_variable", "Other")).strip()
        subcategory = str(parsed.get("subcategory", forecast_variable)).strip() or forecast_variable
        confidence = float(parsed.get("confidence", 0.0))
    except Exception as exc:
        logger.warning("LLM disambiguation failed for '%s': %s", forecast_variable, exc)
        return "Other", "Other", 0.0

    if canonical not in canonical_options and canonical != "Other":
        canonical = "Other"
    confidence = max(0.0, min(1.0, confidence))
    return canonical, subcategory, confidence


# ---------------------------------------------------------------------------
# Core normalization function
# ---------------------------------------------------------------------------

def normalize_record(row: dict) -> dict:
    """
    Send one exploded threshold record to the LLM and return a dict
    containing the original row fields PLUS the 8 normalized fields.

    Tries Azure OpenAI first; falls back to Gemini on failure.
    Returns a dict with error fields populated if all retries fail.
    """
    user_message = _build_user_message(row)
    last_error   = None

    for attempt in range(1, MAX_RETRIES + 1):
        # --- Primary: Azure OpenAI ---
        try:
            raw_json = _call_openai(user_message)
            parsed   = NormalizedThreshold(**json.loads(raw_json))
            result   = {**row, **parsed.model_dump(), "llm_used": "azure_openai", "llm_error": None}
            logger.debug("OpenAI OK  [attempt %d] – %s", attempt, row["threshold_text"][:60])
            return result
        except Exception as e:
            last_error = e
            logger.warning("OpenAI attempt %d failed: %s", attempt, e)

        # --- Fallback: Gemini ---
        try:
            raw_json = _call_gemini(user_message)
            parsed   = NormalizedThreshold(**json.loads(raw_json))
            result   = {**row, **parsed.model_dump(), "llm_used": "gemini", "llm_error": None}
            logger.debug("Gemini OK  [attempt %d] – %s", attempt, row["threshold_text"][:60])
            return result
        except Exception as e:
            last_error = e
            logger.warning("Gemini attempt %d failed: %s", attempt, e)

        time.sleep(API_DELAY_SECONDS * attempt)   # back-off

    # All retries exhausted – return row with error marker
    logger.error("All retries failed for: %s | Error: %s", row["threshold_text"][:80], last_error)
    error_fields = {
        "forecast_variable":  None,
        "threshold_operator": None,
        "threshold_value":    None,
        "threshold_unit":     None,
        "probability_value":  None,
        "lead_time_value":    None,
        "timeframe_unit":     None,
        "normalized_source":  None,
        "llm_used":           "none",
        "llm_error":          str(last_error),
    }
    return {**row, **error_fields}


def normalize_record_openai(row: dict) -> dict:
    """
    Send one record to Azure OpenAI ONLY (no fallback).
    Returns dict with normalized fields + llm_used="azure_openai".
    """
    user_message = _build_user_message(row)
    last_error   = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            raw_json = _call_openai(user_message)
            parsed   = NormalizedThreshold(**json.loads(raw_json))
            result   = {**row, **parsed.model_dump(), "llm_used": "azure_openai", "llm_error": None}
            logger.debug("OpenAI OK  [attempt %d] – %s", attempt, row["threshold_text"][:60])
            return result
        except Exception as e:
            last_error = e
            logger.warning("OpenAI attempt %d failed: %s", attempt, e)
            time.sleep(API_DELAY_SECONDS * attempt)

    logger.error("OpenAI failed for: %s | Error: %s", row["threshold_text"][:80], last_error)
    error_fields = {
        "forecast_variable":  None,
        "threshold_operator": None,
        "threshold_value":    None,
        "threshold_unit":     None,
        "probability_value":  None,
        "lead_time_value":    None,
        "timeframe_unit":     None,
        "normalized_source":  None,
        "llm_used":           "azure_openai",
        "llm_error":          str(last_error),
    }
    return {**row, **error_fields}


def normalize_record_gemini(row: dict) -> dict:
    """
    Send one record to Gemini ONLY (no fallback).
    Returns dict with normalized fields + llm_used="gemini".
    """
    user_message = _build_user_message(row)
    last_error   = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            raw_json = _call_gemini(user_message)
            parsed   = NormalizedThreshold(**json.loads(raw_json))
            result   = {**row, **parsed.model_dump(), "llm_used": "gemini", "llm_error": None}
            logger.debug("Gemini OK  [attempt %d] – %s", attempt, row["threshold_text"][:60])
            return result
        except Exception as e:
            last_error = e
            logger.warning("Gemini attempt %d failed: %s", attempt, e)
            time.sleep(API_DELAY_SECONDS * attempt)

    logger.error("Gemini failed for: %s | Error: %s", row["threshold_text"][:80], last_error)
    error_fields = {
        "forecast_variable":  None,
        "threshold_operator": None,
        "threshold_value":    None,
        "threshold_unit":     None,
        "probability_value":  None,
        "lead_time_value":    None,
        "timeframe_unit":     None,
        "normalized_source":  None,
        "llm_used":           "gemini",
        "llm_error":          str(last_error),
    }
    return {**row, **error_fields}


# ---------------------------------------------------------------------------
# Batch normalization
# ---------------------------------------------------------------------------

def normalize_dataframe(
    df: pd.DataFrame,
    output_path: Path | None = None,
    llm_choice: str = "auto",
    checkpoint_every: int = 10,
    allow_unapproved_taxonomy: bool = False,
) -> pd.DataFrame:
    """
    Normalize every row in the exploded DataFrame.

    Args:
        df: Exploded DataFrame from Step 1
        output_path: Optional custom output path
        llm_choice: "openai", "gemini", or "auto" (auto = OpenAI primary, Gemini fallback)
        checkpoint_every: Save checkpoint every N records

    Returns:
        Normalized DataFrame with only the chosen LLM's results
    """
    if output_path is None:
        if llm_choice == "openai":
            output_path = NORMALIZED_CSV_OPENAI
        elif llm_choice == "gemini":
            output_path = NORMALIZED_CSV_GEMINI
        else:
            output_path = NORMALIZED_CSV_OPENAI  # default

    taxonomy_context = _load_taxonomy_context(
        llm_choice=llm_choice,
        allow_unapproved_taxonomy=allow_unapproved_taxonomy,
    )

    results = []
    total   = len(df)

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        logger.info("Normalizing record %d / %d …", i, total)

        # Call LLM based on choice
        if llm_choice == "openai":
            result = normalize_record_openai(row.to_dict())
        elif llm_choice == "gemini":
            result = normalize_record_gemini(row.to_dict())
        else:  # auto mode
            result = normalize_record(row.to_dict())

        # deterministic taxonomy lookup
        fv = str(result.get("forecast_variable") or "").strip()
        result["original_variable"] = fv
        
        mapping = taxonomy_context.lookup.get(fv.lower())
        if mapping:
            result["canonical_variable"] = mapping.get("canonical_variable")
            result["subcategory"] = mapping.get("subcategory_name")
            result["taxonomy_match_method"] = "deterministic_alias"
            result["taxonomy_match_confidence"] = 1.0
        else:
            canonical, subcategory, confidence = _disambiguate_mapping_with_llm(
                row=result,
                forecast_variable=fv,
                canonical_options=taxonomy_context.canonical_options,
                llm_choice=llm_choice,
            )
            result["canonical_variable"] = canonical
            result["subcategory"] = subcategory
            result["taxonomy_match_method"] = "llm_disambiguation" if canonical != "Other" else "other_fallback"
            result["taxonomy_match_confidence"] = confidence

        results.append(result)

        # Polite rate-limiting
        time.sleep(API_DELAY_SECONDS)

        # Checkpoint save
        if i % checkpoint_every == 0:
            pd.DataFrame(results).to_csv(output_path, index=False, encoding="utf-8")
            logger.info("  ↳ Checkpoint saved (%d / %d)", i, total)

    normalized_df = pd.DataFrame(results)
    normalized_df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Saved normalized CSV → %s", output_path)
    return normalized_df


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from pathlib import Path
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    parser = argparse.ArgumentParser(description="Step 2 – LLM Normalization")
    parser.add_argument(
        "--llm",
        choices=["openai", "gemini", "auto"],
        default="auto",
        help="Which LLM to use: 'openai' (Azure OpenAI only), 'gemini' (Gemini only), or 'auto' (OpenAI with Gemini fallback)"
    )
    parser.add_argument(
        "--allow-unapproved-taxonomy",
        action="store_true",
        help="Allow Phase 2 mapping with non-approved taxonomy artifacts (development mode).",
    )
    args = parser.parse_args()

    if not EXPLODED_CSV.exists():
        print(f"❌  Exploded CSV not found at {EXPLODED_CSV}.")
        print("   Run step1_exploder.py first.")
        sys.exit(1)

    df_exploded = pd.read_csv(EXPLODED_CSV)
    print(f"📥  Loaded {len(df_exploded)} exploded records.")
    print(f"🤖  Using LLM: {args.llm.upper()}")

    df_normalized = normalize_dataframe(
        df_exploded,
        llm_choice=args.llm,
        allow_unapproved_taxonomy=args.allow_unapproved_taxonomy,
    )
    errors = df_normalized["llm_error"].notna().sum()
    print(f"\n✅  Step 2 complete.  {len(df_normalized)} records normalized.")
    if errors:
        print(f"⚠️   {errors} records had LLM errors (see 'llm_error' column).")
    
    if args.llm == "openai":
        print(f"   Output → {NORMALIZED_CSV_OPENAI}")
    elif args.llm == "gemini":
        print(f"   Output → {NORMALIZED_CSV_GEMINI}")
    else:
        print(f"   Output → {NORMALIZED_CSV_OPENAI} (auto mode)")
