"""
Step 1 – Deep Parsing & Exploding the JSON.

Reads extracted_triggers_openai.json and produces one flat record per
threshold string, inheriting all relevant parent metadata.

Output: exploded_thresholds.csv  (also returned as a DataFrame)
"""

from __future__ import annotations

import json
import re
import logging
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Allow running as a standalone script OR as part of the package
# ---------------------------------------------------------------------------
try:
    from .config import (
        INPUT_JSON, EXPLODED_CSV, HAZARD_KEYWORDS
    )
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import INPUT_JSON, EXPLODED_CSV, HAZARD_KEYWORDS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def infer_hazard_type(document_name: str) -> str:
    """
    Infer a standardised hazard type from the document name.
    Falls back to 'Unknown' if no keyword matches.
    """
    name_lower = document_name.lower()
    for hazard, keywords in HAZARD_KEYWORDS.items():
        if any(kw in name_lower for kw in keywords):
            return hazard
    return "Unknown"


def _iter_trigger_statements(triggers: dict[str, Any]):
    """
    Yield (statement_key, statement_dict) pairs from the triggers object.
    Handles both 'trigger_statement_1' style keys and any future variants.
    """
    for key, value in triggers.items():
        if isinstance(value, dict) and "thresholds" in value:
            yield key, value


def explode_document(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Explode a single EAP document into one record per threshold string.

    Each record carries:
      - threshold_text        : the raw isolated condition string
      - threshold_index       : position within the thresholds array (0-based)
      - statement_key         : e.g. 'trigger_statement_2'
      - total_thresholds      : how many thresholds are in this statement
      - document_id
      - document_name
      - hazard_type           : inferred
      - source_authority
      - lead_time             : raw string from JSON
      - is_conditional        : bool
      - condition_dependency  : text or None
      - activation_type       : from trigger_mechanism
      - has_stop_mechanism    : bool
    """
    records: list[dict[str, Any]] = []

    doc_id   = doc.get("document_id", "")
    doc_name = doc.get("document_name", "")
    hazard   = infer_hazard_type(doc_name)

    trigger_mechanism = doc.get("trigger_mechanism", {})
    activation_type   = trigger_mechanism.get("activation_type", "unknown")
    has_stop          = trigger_mechanism.get("has_stop_mechanism", False)

    triggers = doc.get("triggers", {})
    if not triggers:
        logger.warning("Document '%s' has no triggers – skipping.", doc_name)
        return records

    for stmt_key, stmt in _iter_trigger_statements(triggers):
        thresholds         = stmt.get("thresholds", [])
        source_authority   = stmt.get("source_authority", "")
        lead_time          = stmt.get("lead_time", "")
        is_conditional     = bool(stmt.get("is_conditional", False))
        condition_dep      = stmt.get("condition_dependency", None)
        total_thresholds   = len(thresholds)

        for idx, threshold_text in enumerate(thresholds):
            if not isinstance(threshold_text, str) or not threshold_text.strip():
                continue

            records.append({
                "document_id":         doc_id,
                "document_name":       doc_name,
                "hazard_type":         hazard,
                "statement_key":       stmt_key,
                "threshold_index":     idx,
                "total_thresholds":    total_thresholds,
                "threshold_text":      threshold_text.strip(),
                "source_authority":    source_authority,
                "lead_time":           lead_time,
                "is_conditional":      is_conditional,
                "condition_dependency": condition_dep,
                "activation_type":     activation_type,
                "has_stop_mechanism":  has_stop,
            })

    return records


def explode_json(input_path: Path = INPUT_JSON) -> pd.DataFrame:
    """
    Load the full JSON array and explode every document.

    Returns a DataFrame with one row per threshold string.
    """
    logger.info("Loading JSON from: %s", input_path)
    with open(input_path, encoding="utf-8") as fh:
        documents = json.load(fh)

    if not isinstance(documents, list):
        raise ValueError("Expected a JSON array at the top level.")

    all_records: list[dict[str, Any]] = []
    for doc in documents:
        if doc.get("status") != "success":
            logger.debug("Skipping non-success document: %s", doc.get("file"))
            continue
        all_records.extend(explode_document(doc))

    df = pd.DataFrame(all_records)
    logger.info(
        "Exploded %d documents → %d threshold records.",
        len(documents), len(df)
    )
    return df


def save_exploded(df: pd.DataFrame, output_path: Path = EXPLODED_CSV) -> None:
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Saved exploded CSV → %s", output_path)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )
    df = explode_json()
    save_exploded(df)
    print(f"\n✅  Step 1 complete.  {len(df)} records written to:\n   {EXPLODED_CSV}")
    print("\nSample (first 5 rows):")
    print(df[["document_name", "hazard_type", "statement_key",
              "threshold_index", "threshold_text"]].head().to_string(index=False))
