"""
Phase 3 — Connector Classification Pass.

For each EAP document, classifies the logical relationships between all trigger
statements (connector_map) and between activation phases (inter_phase_connector).

Input:
  ui_normalization_output/taxonomy_match_results_v2.csv  (Phase 2 output)

Algorithm:
  1. Group rows by document_id; build one context block per document containing
     the ordered trigger statements with all available structural metadata.
  2. LLM call per document: returns phase_map, connector_map, inter_phase_connector,
     stop_mechanism_present, stop_connector.
  3. Validate LLM output against the connector vocabularies defined in config.py.
  4. Write one JSON entry per document to connector_map.json.
  5. Checkpoint: existing entries in connector_map.json are skipped on re-run.

Output schema per document:
  {
    "document_id":          "16416",
    "document_name":        "Pakistan - Riverine Floods EAP (MDRPK029)",
    "hazard_type":          "Flood",
    "activation_type":      "multi-stage",
    "phase_map": {
      "trigger_statement_1": "pre_activation",
      "trigger_statement_2": "activation",
      "trigger_statement_3": "activation"
    },
    "connector_map": {
      "T1→T2": "THEN",
      "T2→T3": "AND"
    },
    "inter_phase_connector": "ENABLES",
    "stop_mechanism_present": false,
    "stop_connector": null,
    "classification_method": "llm",
    "classification_confidence": 0.9,
    "notes": "<one-sentence summary>"
  }

Usage (from project root):
    python -m scripts.ui_normalization.step2c_connector_classifier
    python -m scripts.ui_normalization.step2c_connector_classifier --llm openai
    python -m scripts.ui_normalization.step2c_connector_classifier --llm openai --limit 3
"""

from __future__ import annotations

import argparse
import json
import logging
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
        TAXONOMY_MATCH_RESULTS_CSV,
        CONNECTOR_MAP_JSON,
        CROSS_STATEMENT_CONNECTORS,
        INTER_PHASE_CONNECTORS,
        WITHIN_STATEMENT_CONNECTORS,
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
        CONNECTOR_MAP_JSON,
        CROSS_STATEMENT_CONNECTORS,
        INTER_PHASE_CONNECTORS,
        WITHIN_STATEMENT_CONNECTORS,
    )

logger = logging.getLogger(__name__)

_RATE_LIMIT_WAIT_SECONDS = 65

# ---------------------------------------------------------------------------
# Phase enum values
# ---------------------------------------------------------------------------
VALID_PHASES = {"pre_activation", "activation", "stop", "readiness", "monitoring"}

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = """\
You are a connector-classification specialist for humanitarian Emergency Action Plan (EAP) \
trigger analysis.

You are given a JSON object describing one EAP document with its ordered trigger statements. \
Each statement includes:
  - statement_key    : unique key (e.g. "trigger_statement_1")
  - activation_type  : single-trigger | dual-trigger | multi-stage
  - is_conditional   : whether this statement depends on a prior one
  - condition_dependency : free-text description of the dependency (may be null)
  - has_stop_mechanism  : whether the document includes an explicit stop rule
  - threshold_conditions : list of individual threshold texts with connectors

Your task: analyse the structure and return a JSON object with these fields:

1. "phase_map" — dict mapping each statement_key to one of:
     "pre_activation"  : readiness / early warning / watch phase (before main trigger)
     "activation"      : the primary trigger phase that activates EAP actions
     "stop"            : explicit de-activation / stand-down criteria
     "readiness"       : standing readiness (no numeric threshold, always-on watch)
     "monitoring"      : ongoing surveillance (distinct from a timed-trigger activation)

2. "connector_map" — dict mapping consecutive statement pairs to a cross-statement
   connector (e.g. "T1→T2": "THEN").
   Valid connectors: OR | AND | THEN | IF_THEN | INDEPENDENT | MIN_N_OF_M
   Rules:
     - THEN      : Statement B opens only after Statement A fires (sequential activation)
     - IF_THEN   : Statement B fires only if A was met AND new threshold is reached
     - AND       : Both must be met simultaneously
     - OR        : Either one alone is sufficient
     - INDEPENDENT : Each activates on its own (simultaneously monitored, different hazards)
     - MIN_N_OF_M  : At least N of M conditions must be met (use when activation_type
                     implies a counting threshold)

3. "inter_phase_connector" — the relationship from pre_activation to activation.
   One of: PRECEDES | ENABLES | OPTIONAL_PRECURSOR | null
     - ENABLES            : pre_activation is a hard prerequisite for activation
     - PRECEDES           : pre_activation fires first but activation can proceed independently
     - OPTIONAL_PRECURSOR : pre_activation exists but activation can happen without it
     - null               : no pre_activation phase found

4. "stop_mechanism_present" — boolean; true if any statement is a stop phase.

5. "stop_connector" — how the stop phase relates to activation, one of:
   CANCELS | SUSPENDS | null
     - CANCELS  : stop terminates all active EAP actions
     - SUSPENDS : stop pauses actions; reactivation possible if conditions worsen

6. "classification_confidence" — float 0.0–1.0 reflecting overall certainty.

7. "notes" — one sentence summarising the trigger logic structure.

Return ONLY a valid JSON object with these 7 fields (no markdown, no extra text).
"""


# ---------------------------------------------------------------------------
# LLM clients (lazy singletons)
# ---------------------------------------------------------------------------
_openai_client = None
_gemini_client = None


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import AzureOpenAI
        _openai_client = AzureOpenAI(
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=AZURE_OPENAI_API_KEY,
            api_version=AZURE_OPENAI_API_VERSION,
            max_retries=0,
        )
    return _openai_client


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


def _safe_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return None if s.lower() in ("nan", "", "none") else s


def _is_rate_limit_error(exc: Exception) -> bool:
    try:
        from openai import RateLimitError
        if isinstance(exc, RateLimitError):
            return True
    except ImportError:
        pass
    msg = str(exc).lower()
    return "429" in msg or "rate limit" in msg or "too many requests" in msg


# ---------------------------------------------------------------------------
# Document context builder
# ---------------------------------------------------------------------------

def _build_document_context(doc_rows: pd.DataFrame) -> dict:
    """Build a structured context dict for one document from its match result rows."""
    doc_id   = _safe_str(doc_rows["document_id"].iloc[0])
    doc_name = _safe_str(doc_rows["document_name"].iloc[0])
    hazard   = _safe_str(doc_rows["hazard_type"].iloc[0])

    # Collect unique statement keys in their natural order
    # (they are named trigger_statement_1, trigger_statement_2 etc.)
    stmt_keys = sorted(
        doc_rows["statement_key"].dropna().unique().tolist(),
        key=lambda k: int(k.split("_")[-1]) if k.split("_")[-1].isdigit() else 0,
    )

    statements = []
    for sk in stmt_keys:
        sk_rows = doc_rows[doc_rows["statement_key"] == sk]
        first = sk_rows.iloc[0]

        # Gather all threshold texts for this statement
        threshold_conditions = []
        for _, row in sk_rows.iterrows():
            tc: dict[str, Any] = {
                "threshold_text": _safe_str(row.get("threshold_text")),
            }
            wsc = _safe_str(row.get("within_statement_connector"))
            if wsc:
                tc["within_statement_connector"] = wsc
            csc = _safe_str(row.get("cross_statement_connector"))
            if csc:
                tc["cross_statement_connector"] = csc
            matched_canonical = _safe_str(row.get("matched_canonical"))
            if matched_canonical:
                tc["matched_canonical"] = matched_canonical
            threshold_conditions.append(tc)

        stmt_block: dict[str, Any] = {
            "statement_key":       sk,
            "activation_type":     _safe_str(first.get("activation_type")),
            "is_conditional":      bool(first.get("is_conditional", False)),
            "condition_dependency": _safe_str(first.get("condition_dependency")),
            "has_stop_mechanism":  bool(first.get("has_stop_mechanism", False)),
            "threshold_conditions": threshold_conditions,
        }
        statements.append(stmt_block)

    return {
        "document_id":   doc_id,
        "document_name": doc_name,
        "hazard_type":   hazard,
        "statements":    statements,
    }


# ---------------------------------------------------------------------------
# LLM classification calls
# ---------------------------------------------------------------------------

def _call_openai(user_message: str) -> dict:
    client = _get_openai_client()
    resp = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        temperature=LLM_TEMPERATURE,
        max_tokens=600,
        response_format={"type": "json_object"},
    )
    raw = (resp.choices[0].message.content or "").strip()
    return json.loads(_clean_json(raw))


def _call_gemini(user_message: str) -> dict:
    client = _get_gemini_client()
    full_prompt = f"{_SYSTEM_PROMPT}\n\n{user_message}"
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=full_prompt,
        config={"temperature": LLM_TEMPERATURE, "max_output_tokens": 600},
    )
    raw = (resp.text or "").strip()
    return json.loads(_clean_json(raw))


def _classify_document(doc_context: dict, llm: str = "openai") -> dict:
    """Call the LLM to classify connectors for one document.

    Returns the raw LLM dict (phase_map, connector_map, inter_phase_connector,
    stop_mechanism_present, stop_connector, classification_confidence, notes).
    Raises on all-retry failure.
    """
    user_message = json.dumps(doc_context, ensure_ascii=False, indent=2)

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if llm == "openai":
                result = _call_openai(user_message)
            else:
                result = _call_gemini(user_message)
            return result
        except Exception as exc:
            last_exc = exc
            is_rate_limit = _is_rate_limit_error(exc)
            if is_rate_limit:
                logger.warning(
                    "LLM connector classify attempt %d/%d — rate limit. Waiting %ds …",
                    attempt, MAX_RETRIES, _RATE_LIMIT_WAIT_SECONDS,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(_RATE_LIMIT_WAIT_SECONDS)
            else:
                logger.warning(
                    "LLM connector classify attempt %d/%d failed: %s",
                    attempt, MAX_RETRIES, exc,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(API_DELAY_SECONDS * attempt)

    raise RuntimeError(f"All LLM attempts failed for document. Last error: {last_exc}")


# ---------------------------------------------------------------------------
# Output validation + normalization
# ---------------------------------------------------------------------------

def _validate_and_normalize(
    raw: dict,
    doc_context: dict,
) -> dict:
    """Validate LLM output and fill/clamp any invalid values."""
    stmt_keys = [s["statement_key"] for s in doc_context["statements"]]

    # phase_map — ensure all statement_keys present; clamp to valid phases
    phase_map = raw.get("phase_map") or {}
    if not isinstance(phase_map, dict):
        phase_map = {}
    for sk in stmt_keys:
        if sk not in phase_map or phase_map[sk] not in VALID_PHASES:
            phase_map[sk] = "activation"  # safe default
    # Remove spurious extra keys
    phase_map = {k: v for k, v in phase_map.items() if k in stmt_keys}

    # connector_map — ensure consecutive pairs present; clamp to valid values
    connector_map = raw.get("connector_map") or {}
    if not isinstance(connector_map, dict):
        connector_map = {}
    valid_cross = set(CROSS_STATEMENT_CONNECTORS)
    for i in range(len(stmt_keys) - 1):
        pair_key = f"T{i+1}→T{i+2}"
        if pair_key not in connector_map or connector_map[pair_key] not in valid_cross:
            # Use the cross_statement_connector extracted in Phase 1 if available
            existing = _safe_str(
                doc_context["statements"][i]["threshold_conditions"][0].get("cross_statement_connector")
            )
            connector_map[pair_key] = existing if existing in valid_cross else "THEN"
    # Remove spurious keys
    expected_pairs = {f"T{i+1}→T{i+2}" for i in range(len(stmt_keys) - 1)}
    connector_map = {k: v for k, v in connector_map.items() if k in expected_pairs}

    # inter_phase_connector
    ipc = _safe_str(raw.get("inter_phase_connector"))
    valid_ipc = set(INTER_PHASE_CONNECTORS) | {None}
    if ipc not in valid_ipc:
        ipc = None

    # stop fields
    stop_present = bool(raw.get("stop_mechanism_present", False))
    stop_conn = _safe_str(raw.get("stop_connector"))
    if stop_conn not in ("CANCELS", "SUSPENDS", None):
        stop_conn = None

    # confidence
    conf = raw.get("classification_confidence")
    try:
        conf = float(conf)
        conf = max(0.0, min(1.0, conf))
    except (TypeError, ValueError):
        conf = 0.7

    notes = _safe_str(raw.get("notes")) or ""

    return {
        "phase_map":                phase_map,
        "connector_map":            connector_map,
        "inter_phase_connector":    ipc,
        "stop_mechanism_present":   stop_present,
        "stop_connector":           stop_conn,
        "classification_method":    "llm",
        "classification_confidence": round(conf, 4),
        "notes":                    notes,
    }


# ---------------------------------------------------------------------------
# Single-document pipeline for single-statement documents
# ---------------------------------------------------------------------------

def _handle_single_statement(doc_context: dict) -> dict:
    """Return deterministic connector result for docs with only one trigger statement."""
    sk = doc_context["statements"][0]["statement_key"]
    activation_type = _safe_str(doc_context["statements"][0].get("activation_type")) or ""
    has_stop = doc_context["statements"][0].get("has_stop_mechanism", False)

    phase = "activation"
    if "readiness" in activation_type.lower():
        phase = "readiness"

    return {
        "phase_map":                {sk: phase},
        "connector_map":            {},
        "inter_phase_connector":    None,
        "stop_mechanism_present":   bool(has_stop),
        "stop_connector":           None,
        "classification_method":    "deterministic",
        "classification_confidence": 1.0,
        "notes":                    "Single-statement document; no connector classification needed.",
    }


# ---------------------------------------------------------------------------
# Main document-level classifier
# ---------------------------------------------------------------------------

def classify_document(doc_context: dict, llm: str = "openai") -> dict:
    """Classify connectors for one document.

    For single-statement documents: returns deterministic result (no LLM call).
    For multi-statement documents: calls the LLM.
    """
    if len(doc_context["statements"]) <= 1:
        return _handle_single_statement(doc_context)

    raw = _classify_document(doc_context, llm=llm)
    return _validate_and_normalize(raw, doc_context)


# ---------------------------------------------------------------------------
# DataFrame-level runner (all documents)
# ---------------------------------------------------------------------------

def classify_all_documents(
    input_csv: Path,
    output_json: Path,
    llm: str = "openai",
    limit: Optional[int] = None,
) -> list[dict]:
    """Classify connectors for all documents in input_csv.

    Args:
        input_csv:   Phase 2 output CSV (taxonomy_match_results_v2.csv).
        output_json: Path to write connector_map.json.
        llm:         "openai" or "gemini".
        limit:       Only process first N documents (for testing).

    Returns:
        List of document-level connector dicts.
    """
    logger.info("Loading Phase 2 results: %s", input_csv)
    df = pd.read_csv(input_csv)
    logger.info("  %d rows across %d documents", len(df), df["document_id"].nunique())

    # Checkpoint: load already-classified documents
    existing_results: list[dict] = []
    already_done: set[str] = set()
    if output_json.exists():
        try:
            with open(output_json, encoding="utf-8") as f:
                existing_results = json.load(f)
            already_done = {str(r["document_id"]) for r in existing_results}
            logger.info("  Checkpoint: %d documents already classified", len(already_done))
        except Exception as exc:
            logger.warning("Could not load existing connector_map.json: %s — starting fresh", exc)
            existing_results = []
            already_done = set()

    # Get ordered list of document_ids
    all_doc_ids = df["document_id"].dropna().unique().tolist()
    if limit:
        logger.info("  ⚠️  --limit %d: processing first %d documents", limit, limit)
        all_doc_ids = all_doc_ids[:limit]

    results = list(existing_results)
    total = len(all_doc_ids)

    for i, doc_id in enumerate(all_doc_ids):
        doc_id_str = str(doc_id)
        if doc_id_str in already_done:
            logger.debug("Skipping already-classified document %s", doc_id_str)
            continue

        doc_rows = df[df["document_id"].astype(str) == doc_id_str]
        if doc_rows.empty:
            continue

        doc_name = _safe_str(doc_rows["document_name"].iloc[0]) or doc_id_str
        logger.info(
            "Classifying document %d/%d: %s (%d statements)",
            i + 1, total, doc_name,
            doc_rows["statement_key"].nunique(),
        )

        doc_context = _build_document_context(doc_rows)

        try:
            classification = classify_document(doc_context, llm=llm)
        except Exception as exc:
            logger.error("Classification failed for %s: %s — using fallback", doc_id_str, exc)
            classification = {
                "phase_map":                {sk: "activation" for sk in [s["statement_key"] for s in doc_context["statements"]]},
                "connector_map":            {},
                "inter_phase_connector":    None,
                "stop_mechanism_present":   False,
                "stop_connector":           None,
                "classification_method":    "error",
                "classification_confidence": 0.0,
                "notes":                    f"Classification failed: {exc}",
            }

        entry = {
            "document_id":   doc_id_str,
            "document_name": doc_name,
            "hazard_type":   _safe_str(doc_rows["hazard_type"].iloc[0]),
            "activation_type": _safe_str(doc_rows["activation_type"].iloc[0]) if "activation_type" in doc_rows.columns else None,
            **classification,
        }
        results.append(entry)

        # Write checkpoint after each document
        output_json.parent.mkdir(parents=True, exist_ok=True)
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        # Rate-limit polite delay (skip for single-statement deterministic results)
        if classification.get("classification_method") == "llm":
            time.sleep(API_DELAY_SECONDS)

    logger.info("Phase 3 complete. %d documents classified → %s", len(results), output_json)

    # Quality summary
    llm_count  = sum(1 for r in results if r.get("classification_method") == "llm")
    det_count  = sum(1 for r in results if r.get("classification_method") == "deterministic")
    err_count  = sum(1 for r in results if r.get("classification_method") == "error")
    avg_conf   = (
        sum(r.get("classification_confidence", 0.0) for r in results) / len(results)
        if results else 0.0
    )
    logger.info(
        "  LLM: %d  |  Deterministic: %d  |  Error: %d  |  Avg confidence: %.2f",
        llm_count, det_count, err_count, avg_conf,
    )

    return results


# ---------------------------------------------------------------------------
# Public entry point (called from run_pipeline.py)
# ---------------------------------------------------------------------------

def run_phase3_connector_classification(
    llm: str = "openai",
    limit: Optional[int] = None,
) -> list[dict]:
    """Entry point for Phase 3 from run_pipeline.py.

    Args:
        llm:   "openai" or "gemini".
        limit: Only process first N documents (for testing).

    Returns:
        List of document connector classification dicts.
    """
    if not TAXONOMY_MATCH_RESULTS_CSV.exists():
        raise FileNotFoundError(
            f"Phase 2 output not found at {TAXONOMY_MATCH_RESULTS_CSV}. "
            "Run --phase2-matching first."
        )
    return classify_all_documents(
        input_csv=TAXONOMY_MATCH_RESULTS_CSV,
        output_json=CONNECTOR_MAP_JSON,
        llm=llm,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Phase 3 — Connector Classification Pass",
    )
    p.add_argument(
        "--llm",
        choices=["openai", "gemini"],
        default="openai",
        help="LLM backend to use (default: openai).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only classify first N documents (for testing).",
    )
    p.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Override input CSV path (default: taxonomy_match_results_v2.csv).",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Override output JSON path (default: connector_map.json).",
    )
    return p.parse_args()


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    args = _parse_args()

    input_csv  = args.input  or TAXONOMY_MATCH_RESULTS_CSV
    output_json = args.output or CONNECTOR_MAP_JSON

    if not input_csv.exists():
        logger.error("Input CSV not found: %s", input_csv)
        sys.exit(1)

    classify_all_documents(
        input_csv=input_csv,
        output_json=output_json,
        llm=args.llm,
        limit=args.limit,
    )
