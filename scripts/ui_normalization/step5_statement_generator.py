"""
Phase 5.3 — LLM-Based Trigger Statement Generator.

Generates natural-language EAP trigger statements from structured condition fields.
Uses 3–5 real corpus examples from the same canonical variable as style references,
so output reads like actual EAP text rather than machine-filled templates.

Input (per call):
  A dict of structured trigger condition fields:
    canonical_variable      e.g. "Precipitation"
    subcategory             e.g. "Total rainfall"
    threshold_operator      e.g. ">="
    threshold_value         e.g. 150.0
    threshold_unit          e.g. "mm"
    accumulation_window     e.g. "72 hours"   (optional)
    persistence             e.g. "3 days"     (optional)
    probability_value       e.g. 70.0         (optional)
    lead_time               e.g. "5 days"     (optional)
    geographic_scope_type   e.g. "watershed_basin"  (optional)
    geographic_scope_label  e.g. "Shire Basin"      (optional)
    normalized_source       e.g. "ECMWF"            (optional)
    is_observational        e.g. False               (optional)

Algorithm:
  1. Load the corpus (taxonomy_match_results_v2.csv) and retrieve up to 5
     threshold_text examples from the same canonical_variable.
  2. Build an LLM prompt that shows the examples + the structured fields.
  3. Call the LLM and return the generated statement string.

Fallback (when no corpus examples are available or LLM unavailable):
  Return a template-filled string using the canonical fallback template
  specified in the plan.

Usage:
    # Standalone test
    python -m scripts.ui_normalization.step5_statement_generator

    # As a module
    from scripts.ui_normalization.step5_statement_generator import generate_statement
    stmt = generate_statement({
        "canonical_variable": "Precipitation",
        "subcategory": "Total rainfall",
        "threshold_operator": ">=",
        "threshold_value": 150.0,
        "threshold_unit": "mm",
        "accumulation_window": "72 hours",
        "lead_time": "5 days",
        "geographic_scope_type": "watershed_basin",
        "geographic_scope_label": "Shire Basin",
        "normalized_source": "ECMWF",
    })
"""

from __future__ import annotations

import json
import logging
import random
import time
from pathlib import Path
from typing import Any

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
        TAXONOMY_MATCH_RESULTS_CSV,
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
        TAXONOMY_MATCH_RESULTS_CSV,
        OUTPUT_DIR,
    )

logger = logging.getLogger(__name__)

# Max corpus examples to include in prompt
_MAX_EXAMPLES = 5

# ---------------------------------------------------------------------------
# Fallback template (plan section 5.3)
# ---------------------------------------------------------------------------
_FALLBACK_TEMPLATE = (
    "When {source}forecasts {canonical_variable} ({subcategory}) "
    "{operator} {value} {unit}"
    "{accum_part}{persist_part}{scope_part}"
    " within {lead_time}"
    "{prob_part}."
)


def _fmt_operator(op: str) -> str:
    return {
        ">=": "greater than or equal to",
        ">":  "greater than",
        "<=": "less than or equal to",
        "<":  "less than",
        "=":  "equal to",
        "between": "between",
    }.get(op, op)


def build_fallback_statement(fields: dict[str, Any]) -> str:
    """Return a template-filled trigger statement when LLM is unavailable."""
    op     = _fmt_operator(str(fields.get("threshold_operator", ">=")))
    value  = fields.get("threshold_value", "")
    unit   = fields.get("threshold_unit", "")
    source = fields.get("normalized_source", "")
    source_part = f"{source} " if source else ""

    lead   = fields.get("lead_time", "")
    lead_part = str(lead) if lead else "the forecast period"

    accum  = fields.get("accumulation_window", "")
    accum_part = f" accumulated over {accum}" if accum else ""

    persist = fields.get("persistence", "")
    persist_part = f" sustained for {persist}" if persist else ""

    geo_type  = fields.get("geographic_scope_type", "")
    geo_label = fields.get("geographic_scope_label", "")
    if geo_label:
        scope_part = f" in {geo_label}"
    elif geo_type and geo_type != "national":
        scope_part = f" in the specified {geo_type.replace('_', ' ')}"
    else:
        scope_part = ""

    prob = fields.get("probability_value", "")
    prob_part = f", with a probability of {prob}%" if prob else ""

    canonical = fields.get("canonical_variable", "forecast variable")
    subcat    = fields.get("subcategory", "")
    subcat_part = f"{canonical} ({subcat})" if subcat else canonical

    return (
        f"When {source_part}forecasts {subcat_part} {op} "
        f"{value} {unit}{accum_part}{persist_part}{scope_part} "
        f"within {lead_part}{prob_part}."
    ).strip()


# ---------------------------------------------------------------------------
# Corpus example loader
# ---------------------------------------------------------------------------

def load_corpus_examples(
    canonical_variable: str,
    corpus_csv: Path = TAXONOMY_MATCH_RESULTS_CSV,
    max_examples: int = _MAX_EXAMPLES,
) -> list[str]:
    """Return up to max_examples real threshold_text strings from the corpus
    for the given canonical_variable.

    Falls back to an empty list when the corpus CSV is absent.
    """
    if not corpus_csv.exists():
        logger.debug("Corpus CSV not found at %s; no examples available.", corpus_csv)
        return []

    try:
        df = pd.read_csv(corpus_csv)
    except Exception as exc:
        logger.warning("Could not read corpus CSV: %s", exc)
        return []

    # Match on matched_canonical (Phase 2 output column), then canonical_variable fallback
    col = "matched_canonical" if "matched_canonical" in df.columns else "canonical_variable"
    subset = df[df[col].str.strip().str.lower() == canonical_variable.strip().lower()]

    texts = (
        subset["threshold_text"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    # Prefer diverse examples: shuffle to avoid always picking the first EAP
    random.shuffle(texts)
    return texts[:max_examples]


# ---------------------------------------------------------------------------
# LLM prompt builder
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are an expert writer for humanitarian Emergency Action Plan (EAP) trigger statements.

Your task is to generate a single, natural-sounding trigger statement from structured
condition fields. The statement must:
  - Sound like it was written by an EAP practitioner, not a machine
  - Match the style, vocabulary, and sentence structure of the provided corpus examples
  - Include all non-null structured fields (value, unit, lead time, probability, etc.)
  - Be a single complete sentence ending with a full stop
  - NOT include any preamble, explanation, or JSON — only the statement itself

If no examples are provided, follow this fallback template style:
"When [Source] forecasts [Variable] [Operator] [Value] [Unit] [modifiers] within [Lead Time][, with [Probability]%]."
"""


def _build_user_message(fields: dict[str, Any], examples: list[str]) -> str:
    """Build the user message for the statement generation prompt."""
    lines = ["## Structured condition fields"]
    lines.append(json.dumps(fields, indent=2, default=str))
    lines.append("")

    if examples:
        lines.append(f"## Corpus examples ({len(examples)}) — match this style")
        for i, ex in enumerate(examples, 1):
            lines.append(f"{i}. {ex}")
        lines.append("")

    lines.append(
        "Write a single natural-language trigger statement for the structured fields above. "
        "Output ONLY the statement — no explanation, no JSON."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM callers
# ---------------------------------------------------------------------------

def _call_openai(user_message: str) -> str | None:
    try:
        from openai import AzureOpenAI  # type: ignore
    except ImportError:
        logger.warning("openai package not installed; cannot use Azure OpenAI.")
        return None

    if not AZURE_OPENAI_ENDPOINT or not AZURE_OPENAI_API_KEY:
        logger.warning("Azure OpenAI credentials not configured.")
        return None

    client = AzureOpenAI(
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_API_KEY,
        api_version=AZURE_OPENAI_API_VERSION,
    )
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(
                model=AZURE_OPENAI_DEPLOYMENT,
                temperature=0.3,     # slight creativity for natural phrasing
                max_tokens=200,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user",   "content": user_message},
                ],
            )
            return response.choices[0].message.content.strip()
        except Exception as exc:
            logger.warning("OpenAI attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(API_DELAY_SECONDS)
    return None


def _call_gemini(user_message: str) -> str | None:
    try:
        import google.generativeai as genai  # type: ignore
    except ImportError:
        logger.warning("google-generativeai package not installed; cannot use Gemini.")
        return None

    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not configured.")
        return None

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(GEMINI_MODEL)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            prompt = f"{_SYSTEM_PROMPT}\n\n{user_message}"
            response = model.generate_content(
                prompt,
                generation_config={"temperature": 0.3, "max_output_tokens": 200},
            )
            return response.text.strip()
        except Exception as exc:
            logger.warning("Gemini attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(API_DELAY_SECONDS)
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_statement(
    fields: dict[str, Any],
    llm: str = "openai",
    corpus_csv: Path = TAXONOMY_MATCH_RESULTS_CSV,
    max_examples: int = _MAX_EXAMPLES,
) -> str:
    """Generate a natural-language EAP trigger statement from structured fields.

    Args:
        fields:       Structured trigger condition fields (see module docstring).
        llm:          "openai", "gemini", or "fallback" to skip LLM calls.
        corpus_csv:   Path to taxonomy_match_results_v2.csv for example lookup.
        max_examples: Maximum corpus examples to include in the prompt.

    Returns:
        A natural-language trigger statement string.
    """
    canonical = fields.get("canonical_variable", "")

    # 1. Gather corpus examples for style reference
    examples = load_corpus_examples(canonical, corpus_csv, max_examples)
    if not examples:
        logger.debug("No corpus examples found for canonical '%s'.", canonical)

    # 2. Try LLM generation
    if llm != "fallback":
        user_msg = _build_user_message(fields, examples)
        result = None

        if llm == "openai":
            result = _call_openai(user_msg)
        elif llm == "gemini":
            result = _call_gemini(user_msg)

        if result:
            return result
        logger.warning("LLM generation failed; falling back to template.")

    # 3. Fallback template
    return build_fallback_statement(fields)


def batch_generate(
    records: list[dict[str, Any]],
    llm: str = "openai",
    corpus_csv: Path = TAXONOMY_MATCH_RESULTS_CSV,
) -> list[dict[str, Any]]:
    """Generate statements for a list of condition records.

    Each record is a dict of structured fields. Returns a copy of each record
    with a `generated_statement` key added.

    Args:
        records:     List of condition field dicts.
        llm:         LLM to use ("openai", "gemini", "fallback").
        corpus_csv:  Corpus CSV for example lookup.

    Returns:
        List of dicts, each with `generated_statement` added.
    """
    results = []
    for i, record in enumerate(records, 1):
        logger.info("Generating statement %d/%d …", i, len(records))
        stmt = generate_statement(record, llm=llm, corpus_csv=corpus_csv)
        result = dict(record)
        result["generated_statement"] = stmt
        results.append(result)
        if i < len(records):
            time.sleep(API_DELAY_SECONDS)
    return results


# ---------------------------------------------------------------------------
# Standalone demo entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Phase 5.3: LLM-based EAP trigger statement generator (demo mode)"
    )
    parser.add_argument(
        "--llm",
        choices=["openai", "gemini", "fallback"],
        default="openai",
        help="LLM backend to use (default: openai)",
    )
    args = parser.parse_args()

    # Demo inputs covering several canonical types
    demo_cases: list[dict[str, Any]] = [
        {
            "canonical_variable": "Precipitation",
            "subcategory": "Total rainfall",
            "threshold_operator": ">=",
            "threshold_value": 150.0,
            "threshold_unit": "mm",
            "accumulation_window": "72 hours",
            "lead_time": "5 days",
            "geographic_scope_type": "watershed_basin",
            "geographic_scope_label": "Shire Basin",
            "normalized_source": "ECMWF",
            "is_observational": False,
        },
        {
            "canonical_variable": "Hydrological Flow",
            "subcategory": "Return period exceedance",
            "threshold_operator": ">=",
            "threshold_value": 2.0,
            "threshold_unit": "years",
            "probability_value": 70.0,
            "lead_time": "10 days",
            "geographic_scope_type": "station_gauge",
            "geographic_scope_label": "Nowshera gauging station",
            "normalized_source": "GloFAS",
            "is_observational": False,
        },
        {
            "canonical_variable": "Temperature",
            "subcategory": "Min temperature",
            "threshold_operator": "<=",
            "threshold_value": 7.0,
            "threshold_unit": "°C",
            "persistence": "4 days",
            "lead_time": "4 days",
            "geographic_scope_type": "count_threshold",
            "geographic_scope_label": "3 or more districts",
            "normalized_source": "BMD",
            "is_observational": False,
        },
        {
            "canonical_variable": "Infectious Disease",
            "subcategory": "Cholera cases",
            "threshold_operator": ">",
            "threshold_value": 1.0,
            "threshold_unit": "cases",
            "geographic_scope_type": "administrative_unit",
            "geographic_scope_label": "Harare",
            "is_observational": True,
        },
    ]

    print(f"\nStatement Generator — using {args.llm.upper()}\n{'=' * 60}")
    for case in demo_cases:
        print(f"\nCanonical: {case['canonical_variable']} → {case.get('subcategory', '')}")
        stmt = generate_statement(case, llm=args.llm)
        print(f"Generated: {stmt}")

    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    main()
