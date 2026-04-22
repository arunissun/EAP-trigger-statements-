"""
Phase 1 - LLM-driven canonical taxonomy proposal and quality gates.

Inputs:
  - Phase 0 flattened CSV (preferred) OR normalized CSV

Outputs:
  - taxonomy_proposal_*.json
  - taxonomy_quality_*.json
  - taxonomy_review_*.md

The proposal enforces:
  - Exactly 10 canonical variables
  - Every forecast variable mapped to one of the 10 or to Other
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

try:
    from .config import (
        AZURE_OPENAI_API_KEY,
        AZURE_OPENAI_API_VERSION,
        AZURE_OPENAI_DEPLOYMENT,
        AZURE_OPENAI_ENDPOINT,
        GEMINI_API_KEY,
        GEMINI_MODEL,
        LLM_MAX_TOKENS,
        PHASE0_FLATTENED_CSV_GEMINI,
        PHASE0_FLATTENED_CSV_OPENAI,
        PHASE1_CANONICAL_TARGET,
        PHASE1_COVERAGE_TARGET,
        PHASE1_MAX_ALIAS_OVERLAP,
        PHASE1_MAX_ITERATIONS,
        PHASE1_MIN_SUPPORT_RECORDS,
        TAXONOMY_PROPOSAL_JSON_GEMINI,
        TAXONOMY_PROPOSAL_JSON_OPENAI,
        TAXONOMY_QUALITY_JSON_GEMINI,
        TAXONOMY_QUALITY_JSON_OPENAI,
        TAXONOMY_REVIEW_MD_GEMINI,
        TAXONOMY_REVIEW_MD_OPENAI,
    )
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import (
        AZURE_OPENAI_API_KEY,
        AZURE_OPENAI_API_VERSION,
        AZURE_OPENAI_DEPLOYMENT,
        AZURE_OPENAI_ENDPOINT,
        GEMINI_API_KEY,
        GEMINI_MODEL,
        LLM_MAX_TOKENS,
        PHASE0_FLATTENED_CSV_GEMINI,
        PHASE0_FLATTENED_CSV_OPENAI,
        PHASE1_CANONICAL_TARGET,
        PHASE1_COVERAGE_TARGET,
        PHASE1_MAX_ALIAS_OVERLAP,
        PHASE1_MAX_ITERATIONS,
        PHASE1_MIN_SUPPORT_RECORDS,
        TAXONOMY_PROPOSAL_JSON_GEMINI,
        TAXONOMY_PROPOSAL_JSON_OPENAI,
        TAXONOMY_QUALITY_JSON_GEMINI,
        TAXONOMY_QUALITY_JSON_OPENAI,
        TAXONOMY_REVIEW_MD_GEMINI,
        TAXONOMY_REVIEW_MD_OPENAI,
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


class TaxonomyMapping(BaseModel):
    forecast_variable: str
    canonical_variable: str
    subcategory_name: str
    alias_list: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    examples: list[str] = Field(default_factory=list)


class TaxonomyProposal(BaseModel):
    proposed_canonical_variables: list[str]
    mappings: list[TaxonomyMapping]


class CanonicalProposal(BaseModel):
    proposed_canonical_variables: list[str]


@dataclass
class ResolvedPaths:
    phase0_csv: Path
    proposal_json: Path
    quality_json: Path
    review_md: Path


def _resolve_paths(llm: str) -> ResolvedPaths:
    if llm == "openai":
        return ResolvedPaths(
            phase0_csv=PHASE0_FLATTENED_CSV_OPENAI,
            proposal_json=TAXONOMY_PROPOSAL_JSON_OPENAI,
            quality_json=TAXONOMY_QUALITY_JSON_OPENAI,
            review_md=TAXONOMY_REVIEW_MD_OPENAI,
        )
    return ResolvedPaths(
        phase0_csv=PHASE0_FLATTENED_CSV_GEMINI,
        proposal_json=TAXONOMY_PROPOSAL_JSON_GEMINI,
        quality_json=TAXONOMY_QUALITY_JSON_GEMINI,
        review_md=TAXONOMY_REVIEW_MD_GEMINI,
    )


def _clean_json_text(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("```"):
        parts = text.split("```")
        if len(parts) >= 2:
            text = parts[1]
            if text.lstrip().startswith("json"):
                text = text.lstrip()[4:]
    return text.strip()


def _tokenize(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", str(text).lower()))


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 0.0
    inter = len(a.intersection(b))
    union = len(a.union(b))
    if union == 0:
        return 0.0
    return inter / union


def build_variable_profiles(df: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, int], int]:
    grouped = df.groupby("forecast_variable", dropna=True)

    profiles: list[dict[str, Any]] = []
    counts: dict[str, int] = {}

    for var, group in grouped:
        var_name = str(var).strip()
        if not var_name:
            continue

        hazards = sorted({str(v).strip() for v in group["hazard_type"].dropna().tolist() if str(v).strip()})
        samples = [
            str(v).strip()[:140]
            for v in group["trigger_statement"].dropna().astype(str).tolist()
            if str(v).strip()
        ][:1]

        count = int(len(group))
        counts[var_name] = count

        profiles.append(
            {
                "forecast_variable": var_name,
                "record_count": count,
                "hazard_contexts": hazards,
                "examples": samples,
            }
        )

    profiles = sorted(profiles, key=lambda x: (-x["record_count"], x["forecast_variable"]))
    total_records = int(sum(counts.values()))
    return profiles, counts, total_records


def _taxonomy_system_prompt() -> str:
    return (
        "You are a taxonomy architect for humanitarian trigger forecasting data. "
        "Build a canonical taxonomy for forecast variables with hazard-aware semantics. "
        "Lead time and timeframe are context metadata, not forecast variables. "
        "Do not use timeframe as a canonical-matching feature. "
        "Always return strict JSON only."
    )


def _build_taxonomy_user_prompt(
    variable_profiles: list[dict[str, Any]],
    canonical_target: int,
    previous_attempt: dict[str, Any] | None,
    feedback: list[str] | None,
) -> str:
    # Deprecated in favor of the two-stage batched mode
    pass


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


def _call_openai_taxonomy(user_message: str, temperature: float = 0.0) -> str:
    client = _get_openai_client()
    resp = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": _taxonomy_system_prompt()},
            {"role": "user", "content": user_message},
        ],
        temperature=temperature,
        max_tokens=min(4000, max(1500, LLM_MAX_TOKENS * 6)),
        response_format={"type": "json_object"},
    )
    return (resp.choices[0].message.content or "").strip()


_gemini_client = None


def _get_gemini_client():
    global _gemini_client
    if _gemini_client is None:
        from google import genai

        _gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    return _gemini_client


def _call_gemini_taxonomy(user_message: str, temperature: float = 0.0) -> str:
    client = _get_gemini_client()
    full_prompt = f"{_taxonomy_system_prompt()}\n\n{user_message}"
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=full_prompt,
        config={"temperature": temperature},
    )
    return (response.text or "").strip()


def _llm_call(llm: str, user_prompt: str, temperature: float) -> dict[str, Any]:
    if llm == "openai":
        raw = _call_openai_taxonomy(user_prompt, temperature=temperature)
    else:
        raw = _call_gemini_taxonomy(user_prompt, temperature=temperature)

    clean = _clean_json_text(raw)
    return json.loads(clean)


def _call_json_with_retries(
    llm: str,
    user_prompt: str,
    temperature: float,
    retries: int = 3,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for _ in range(retries):
        try:
            return _llm_call(llm=llm, user_prompt=user_prompt, temperature=temperature)
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError("LLM call failed without explicit error")


def _build_canonical_seed_prompt(
    variable_profiles: list[dict[str, Any]],
    canonical_target: int,
    feedback: list[str] | None = None,
) -> str:
    compact_profiles = [
        {
            "forecast_variable": p["forecast_variable"],
            "record_count": p["record_count"],
            "hazard_contexts": p["hazard_contexts"][:4],
        }
        for p in variable_profiles[:60]
    ]

    payload = {
        "task": "Propose canonical variable set only.",
        "canonical_target": canonical_target,
        "hard_rules": [
            "Return exactly canonical_target items.",
            "Names must be semantically broad and stable.",
            "Do not include lead time, duration, timeframe, or forecast horizon as canonical names.",
            "No duplicates.",
        ],
        "required_output_schema": {
            "proposed_canonical_variables": ["string", "... exactly canonical_target"]
        },
        "input_variable_profiles": compact_profiles,
    }
    if feedback:
        payload["feedback_from_previous_attempt"] = feedback
    return json.dumps(payload, ensure_ascii=True)


def _build_batch_mapping_prompt(
    batch_profiles: list[dict[str, Any]],
    canonical_options: list[str],
) -> str:
    payload = {
        "task": "Map input variables to the canonical options.",
        "canonical_options": canonical_options + ["Other"],
        "instructions": [
            "Map each input variable to exactly one canonical option.",
            "If it does not fit well, map it to 'Other'.",
            "Provide a short subcategory name."
        ],
        "inputs_to_map": [p["forecast_variable"] for p in batch_profiles],
        "required_schema": {
            "mappings": [
                {
                    "v": "input variable name",
                    "c": "selected canonical option",
                    "s": "subcategory name",
                    "conf": 0.9
                }
            ]
        }
    }
    return json.dumps(payload, ensure_ascii=True)

def _generate_two_stage_proposal(
    llm: str,
    variable_profiles: list[dict[str, Any]],
    canonical_target: int,
    feedback: list[str] | None = None,
) -> TaxonomyProposal:
    # Stage A: Seed
    seed_prompt = _build_canonical_seed_prompt(variable_profiles, canonical_target, feedback=feedback)
    
    canonical_list: list[str] = []
    try:
        seed_obj = _call_json_with_retries(llm=llm, user_prompt=seed_prompt, temperature=0.1, retries=3)
        seed = CanonicalProposal(**seed_obj)
        for item in seed.proposed_canonical_variables:
            name = str(item).strip()
            if name and name.lower() != "other" and name not in canonical_list:
                canonical_list.append(name)
    except Exception as exc:
        logger.warning("Decomposed canonical seed failed, using frequency fallback: %s", exc)

    if len(canonical_list) < canonical_target:
        for p in variable_profiles:
            name = str(p["forecast_variable"]).strip()
            if name and name not in canonical_list and name.lower() != "other":
                canonical_list.append(name)
            if len(canonical_list) >= canonical_target:
                break
    canonical_list = canonical_list[:canonical_target]

    # Stage B: Map in batches
    batch_size = 20
    mappings: list[TaxonomyMapping] = []
    
    for i in range(0, len(variable_profiles), batch_size):
        batch = variable_profiles[i:i+batch_size]
        prompt = _build_batch_mapping_prompt(batch, canonical_list)
        try:
            map_obj = _call_json_with_retries(llm=llm, user_prompt=prompt, temperature=0.0, retries=2)
            for item in map_obj.get("mappings", []):
                var_name = str(item.get("v", "")).strip()
                can_name = str(item.get("c", "")).strip()
                sub_name = str(item.get("s", "")).strip()
                conf = float(item.get("conf", 0.0))
                
                if can_name not in canonical_list and can_name != "Other":
                    can_name = "Other"
                    
                # Restore full mapping object structure natively
                examples = []
                for p in batch:
                    if p["forecast_variable"] == var_name:
                        examples = p.get("examples", [])[:2]
                        break
                        
                mappings.append(
                    TaxonomyMapping(
                        forecast_variable=var_name,
                        canonical_variable=can_name,
                        subcategory_name=sub_name or var_name,
                        alias_list=[var_name],
                        confidence=max(0.0, min(1.0, conf)),
                        rationale="Mapped via batch clustering.",
                        examples=examples,
                    )
                )
        except Exception as exc:
            logger.warning("Batch mapping failed, falling back to Other. Error: %s", exc)
            for p in batch:
                mappings.append(
                    TaxonomyMapping(
                        forecast_variable=p["forecast_variable"],
                        canonical_variable="Other",
                        subcategory_name=p["forecast_variable"],
                        alias_list=[p["forecast_variable"]],
                        confidence=0.0,
                        rationale="Fallback mapping due to parse failure.",
                        examples=p.get("examples", [])[:2],
                    )
                )
                
    # Ensure every input variable is represented exactly once
    input_vars = {p["forecast_variable"] for p in variable_profiles}
    mapped_vars = {m.forecast_variable for m in mappings}
    
    # Handle missing items
    missing = input_vars - mapped_vars
    for p in variable_profiles:
        if p["forecast_variable"] in missing:
            mappings.append(
                TaxonomyMapping(
                    forecast_variable=p["forecast_variable"],
                    canonical_variable="Other",
                    subcategory_name=p["forecast_variable"],
                    alias_list=[p["forecast_variable"]],
                    confidence=0.0,
                    rationale="Added by validator due to missing mapping.",
                    examples=p.get("examples", [])[:2],
                )
            )
            
    # Handle duplicates by taking the first seen mapping
    final_mappings = []
    seen = set()
    for m in mappings:
        if m.forecast_variable not in seen and m.forecast_variable in input_vars:
            seen.add(m.forecast_variable)
            final_mappings.append(m)

    return TaxonomyProposal(
        proposed_canonical_variables=canonical_list,
        mappings=final_mappings
    )


def _normalize_proposal(
    proposal_dict: dict[str, Any],
    variable_profiles: list[dict[str, Any]],
    canonical_target: int,
) -> TaxonomyProposal:
    proposal = TaxonomyProposal(**proposal_dict)

    canonical: list[str] = []
    for name in proposal.proposed_canonical_variables:
        item = str(name).strip()
        if item and item.lower() != "other" and item not in canonical:
            canonical.append(item)

    while len(canonical) < canonical_target:
        canonical.append(f"Canonical Group {len(canonical) + 1}")
    canonical = canonical[:canonical_target]

    valid_targets = set(canonical)

    profiles_by_variable = {p["forecast_variable"]: p for p in variable_profiles}
    input_variables = set(profiles_by_variable.keys())

    selected: dict[str, TaxonomyMapping] = {}
    for mapping in proposal.mappings:
        var = str(mapping.forecast_variable).strip()
        if not var or var in selected:
            continue

        canonical_variable = str(mapping.canonical_variable).strip() or "Other"
        if canonical_variable not in valid_targets and canonical_variable != "Other":
            canonical_variable = "Other"

        subcategory = str(mapping.subcategory_name).strip() or var
        aliases = [a.strip() for a in mapping.alias_list if str(a).strip()]
        if var not in aliases:
            aliases.insert(0, var)

        examples = [e.strip() for e in mapping.examples if str(e).strip()]
        if not examples and var in profiles_by_variable:
            examples = profiles_by_variable[var].get("examples", [])[:2]

        selected[var] = TaxonomyMapping(
            forecast_variable=var,
            canonical_variable=canonical_variable,
            subcategory_name=subcategory,
            alias_list=aliases,
            confidence=max(0.0, min(1.0, mapping.confidence)),
            rationale=str(mapping.rationale).strip() or "Mapped by semantic similarity.",
            examples=examples,
        )

    for var in sorted(input_variables):
        if var in selected:
            continue
        selected[var] = TaxonomyMapping(
            forecast_variable=var,
            canonical_variable="Other",
            subcategory_name=var,
            alias_list=[var],
            confidence=0.0,
            rationale="Added by validator due to missing mapping.",
            examples=profiles_by_variable[var].get("examples", [])[:2],
        )

    return TaxonomyProposal(
        proposed_canonical_variables=canonical,
        mappings=[selected[v] for v in sorted(selected.keys())],
    )


def evaluate_quality(
    proposal: TaxonomyProposal,
    variable_counts: dict[str, int],
    total_records: int,
    canonical_target: int,
    coverage_target: float,
    min_support_records: int,
    max_alias_overlap: float,
) -> dict[str, Any]:
    canonical = proposal.proposed_canonical_variables
    canonical_set = set(canonical)

    mapped_variables = {m.forecast_variable for m in proposal.mappings}
    input_variables = set(variable_counts.keys())

    canonical_count_ok = len(canonical) == len(set(canonical)) == canonical_target
    mapping_complete_ok = mapped_variables == input_variables

    valid_target_ok = all(
        (m.canonical_variable in canonical_set or m.canonical_variable == "Other")
        for m in proposal.mappings
    )

    coverage_records = 0
    canonical_support = {c: 0 for c in canonical}

    for m in proposal.mappings:
        count = int(variable_counts.get(m.forecast_variable, 0))
        if m.canonical_variable != "Other":
            coverage_records += count
        if m.canonical_variable in canonical_support:
            canonical_support[m.canonical_variable] += count

    coverage = (coverage_records / total_records) if total_records else 0.0
    coverage_ok = coverage >= coverage_target

    min_support_ok = all(v >= min_support_records for v in canonical_support.values())

    canonical_tokens = {c: _tokenize(c) for c in canonical}
    max_canonical_similarity = 0.0
    for i, c1 in enumerate(canonical):
        for c2 in canonical[i + 1 :]:
            sim = _jaccard(canonical_tokens[c1], canonical_tokens[c2])
            max_canonical_similarity = max(max_canonical_similarity, sim)
    semantic_distinctness_ok = max_canonical_similarity < 0.6

    aliases_by_canonical: dict[str, set[str]] = {c: set() for c in canonical}
    for m in proposal.mappings:
        if m.canonical_variable in aliases_by_canonical:
            for alias in m.alias_list:
                norm = str(alias).strip().lower()
                if norm:
                    aliases_by_canonical[m.canonical_variable].add(norm)

    max_alias_overlap_observed = 0.0
    for i, c1 in enumerate(canonical):
        for c2 in canonical[i + 1 :]:
            overlap = _jaccard(aliases_by_canonical[c1], aliases_by_canonical[c2])
            max_alias_overlap_observed = max(max_alias_overlap_observed, overlap)

    alias_overlap_ok = max_alias_overlap_observed <= max_alias_overlap

    checks = {
        "canonical_count_ok": canonical_count_ok,
        "mapping_complete_ok": mapping_complete_ok,
        "valid_target_ok": valid_target_ok,
        "coverage_ok": coverage_ok,
        "semantic_distinctness_ok": semantic_distinctness_ok,
        "alias_overlap_ok": alias_overlap_ok,
        "min_support_ok": min_support_ok,
    }

    overall_pass = all(checks.values())
    failed_checks = [k for k, v in checks.items() if not v]

    return {
        "overall_pass": overall_pass,
        "checks": checks,
        "failed_checks": failed_checks,
        "metrics": {
            "coverage": round(coverage, 4),
            "coverage_target": coverage_target,
            "max_canonical_similarity": round(max_canonical_similarity, 4),
            "max_alias_overlap": round(max_alias_overlap_observed, 4),
            "max_alias_overlap_allowed": max_alias_overlap,
            "min_support_records": min_support_records,
            "canonical_support": canonical_support,
            "other_mapped_records": int(total_records - coverage_records),
            "total_records": int(total_records),
        },
    }


def _feedback_from_quality(quality: dict[str, Any]) -> list[str]:
    feedback: list[str] = []
    for check in quality["failed_checks"]:
        if check == "canonical_count_ok":
            feedback.append("Output must contain exactly 10 unique canonical variables.")
        elif check == "mapping_complete_ok":
            feedback.append("Map every input forecast_variable exactly once.")
        elif check == "valid_target_ok":
            feedback.append("Each canonical_variable must be one of the 10 canonical names or Other.")
        elif check == "coverage_ok":
            m = quality["metrics"]
            feedback.append(
                f"Increase mapped coverage above target; current={m['coverage']}, target={m['coverage_target']}."
            )
        elif check == "semantic_distinctness_ok":
            feedback.append("Canonical names are too similar; increase semantic separation.")
        elif check == "alias_overlap_ok":
            feedback.append("Reduce alias overlap between canonical groups.")
        elif check == "min_support_ok":
            feedback.append("Rebalance groups so each canonical group has minimum support.")
    return feedback


def _score_quality(quality: dict[str, Any]) -> float:
    checks = quality["checks"]
    pass_count = sum(1 for v in checks.values() if v)
    return pass_count + quality["metrics"]["coverage"]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)


def _build_review_markdown(
    proposal: TaxonomyProposal,
    quality: dict[str, Any],
    variable_counts: dict[str, int],
    llm: str,
) -> str:
    canonical_support = quality["metrics"]["canonical_support"]
    total = max(1, quality["metrics"]["total_records"])

    lines: list[str] = []
    lines.append(f"# Phase 1 Taxonomy Review ({llm.upper()})")
    lines.append("")
    lines.append("## Gate Status")
    lines.append("")
    lines.append(f"- overall_pass: **{quality['overall_pass']}**")
    lines.append("- approval_status: **pending_human_review**")
    lines.append("")

    lines.append("## Canonical Variables (Top 10)")
    lines.append("")
    lines.append("| Canonical Variable | Support Records | Share |")
    lines.append("|---|---:|---:|")
    for name in proposal.proposed_canonical_variables:
        support = int(canonical_support.get(name, 0))
        pct = (support / total) * 100
        lines.append(f"| {name} | {support} | {pct:.1f}% |")
    lines.append("")

    lines.append("## Quality Checks")
    lines.append("")
    for check_name, passed in quality["checks"].items():
        lines.append(f"- {check_name}: {'PASS' if passed else 'FAIL'}")
    lines.append("")

    m = quality["metrics"]
    lines.append("## Metrics")
    lines.append("")
    lines.append(f"- coverage: **{m['coverage']:.2%}** (target: {m['coverage_target']:.0%})")
    lines.append(f"- max_canonical_similarity: **{m['max_canonical_similarity']}**")
    lines.append(
        f"- max_alias_overlap: **{m['max_alias_overlap']}** (allowed: {m['max_alias_overlap_allowed']})"
    )
    lines.append(f"- other_mapped_records: **{m['other_mapped_records']}** / {m['total_records']}")
    lines.append("")

    lines.append("## Variable Mapping")
    lines.append("")
    lines.append("| Forecast Variable | Count | Canonical | Subcategory | Confidence |")
    lines.append("|---|---:|---|---|---:|")

    by_variable = {m.forecast_variable: m for m in proposal.mappings}
    for variable, count in sorted(variable_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        mapping = by_variable.get(variable)
        if mapping is None:
            continue
        lines.append(
            f"| {variable} | {count} | {mapping.canonical_variable} | {mapping.subcategory_name} | {mapping.confidence:.2f} |"
        )

    lines.append("")
    lines.append("---")
    lines.append("Generated by scripts.ui_normalization.phase1_taxonomy")
    return "\n".join(lines)


def run_phase1_taxonomy(
    input_csv: Path,
    llm: str,
    proposal_json: Path,
    quality_json: Path,
    review_md: Path,
    canonical_target: int = PHASE1_CANONICAL_TARGET,
    coverage_target: float = PHASE1_COVERAGE_TARGET,
    min_support_records: int = PHASE1_MIN_SUPPORT_RECORDS,
    max_alias_overlap: float = PHASE1_MAX_ALIAS_OVERLAP,
    max_iterations: int = PHASE1_MAX_ITERATIONS,
) -> tuple[dict[str, Any], dict[str, Any]]:
    logger.info("Loading Phase 1 input from %s", input_csv)
    df = pd.read_csv(input_csv)

    required_cols = {
        "forecast_variable",
        "hazard_type",
        "trigger_statement",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for Phase 1: {sorted(missing)}")

    df = df.copy()
    df["forecast_variable"] = df["forecast_variable"].astype(str).str.strip()
    df = df[df["forecast_variable"] != ""]

    temporal_mask = df["forecast_variable"].apply(_is_temporal_variable_name)
    temporal_count = int(temporal_mask.sum())
    if temporal_count:
        logger.warning(
            "Filtered %d temporal pseudo-variable rows from Phase 1 input.",
            temporal_count,
        )
        df = df[~temporal_mask].copy()

    profiles, variable_counts, total_records = build_variable_profiles(df)
    if not profiles:
        raise ValueError("No variable profiles available for taxonomy generation")

    attempts: list[dict[str, Any]] = []
    feedback: list[str] | None = None

    best_proposal: TaxonomyProposal | None = None
    best_quality: dict[str, Any] | None = None
    best_score = -1.0

    for idx in range(1, max_iterations + 1):
        logger.info("Phase 1 taxonomy attempt %d/%d", idx, max_iterations)
        
        proposal = _generate_two_stage_proposal(
            llm=llm,
            variable_profiles=profiles,
            canonical_target=canonical_target,
            feedback=feedback,
        )

        quality = evaluate_quality(
            proposal=proposal,
            variable_counts=variable_counts,
            total_records=total_records,
            canonical_target=canonical_target,
            coverage_target=coverage_target,
            min_support_records=min_support_records,
            max_alias_overlap=max_alias_overlap,
        )

        attempts.append(
            {
                "attempt": idx,
                "proposal": proposal.model_dump(),
                "quality": quality,
            }
        )

        score = _score_quality(quality)
        if score > best_score:
            best_score = score
            best_proposal = proposal
            best_quality = quality

        if quality["overall_pass"]:
            logger.info("Phase 1 quality gate passed on attempt %d", idx)
            break

        feedback = _feedback_from_quality(quality)

    if best_proposal is None or best_quality is None:
        raise RuntimeError("Phase 1 failed to produce a usable proposal")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    proposal_payload = {
        "metadata": {
            "generated_at_utc": timestamp,
            "llm": llm,
            "canonical_target": canonical_target,
            "approval_status": "pending_human_review",
            "taxonomy_version_candidate": f"candidate-{timestamp}",
            "selected_attempt": next(
                (a["attempt"] for a in attempts if a.get("proposal") == best_proposal.model_dump()),
                None,
            ),
            "attempt_count": len(attempts),
        },
        "proposal": best_proposal.model_dump(),
        "attempts": attempts,
    }

    quality_payload = {
        "metadata": {
            "generated_at_utc": timestamp,
            "llm": llm,
            "approval_status": "pending_human_review",
        },
        "quality": best_quality,
    }

    _write_json(proposal_json, proposal_payload)
    _write_json(quality_json, quality_payload)

    review_text = _build_review_markdown(
        proposal=best_proposal,
        quality=best_quality,
        variable_counts=variable_counts,
        llm=llm,
    )
    review_md.parent.mkdir(parents=True, exist_ok=True)
    review_md.write_text(review_text, encoding="utf-8")

    logger.info("Saved taxonomy proposal to %s", proposal_json)
    logger.info("Saved taxonomy quality report to %s", quality_json)
    logger.info("Saved taxonomy review markdown to %s", review_md)

    return proposal_payload, quality_payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1 canonical taxonomy proposer")
    parser.add_argument("--llm", choices=["openai", "gemini"], default="openai")
    parser.add_argument("--input-csv", type=str, default=None, help="Phase 1 input CSV path")
    parser.add_argument("--canonical-target", type=int, default=PHASE1_CANONICAL_TARGET)
    parser.add_argument("--coverage-target", type=float, default=PHASE1_COVERAGE_TARGET)
    parser.add_argument("--min-support-records", type=int, default=PHASE1_MIN_SUPPORT_RECORDS)
    parser.add_argument("--max-alias-overlap", type=float, default=PHASE1_MAX_ALIAS_OVERLAP)
    parser.add_argument("--max-iterations", type=int, default=PHASE1_MAX_ITERATIONS)
    args = parser.parse_args()

    paths = _resolve_paths(args.llm)
    input_csv = Path(args.input_csv) if args.input_csv else paths.phase0_csv

    if not input_csv.exists():
        raise FileNotFoundError(f"Phase 1 input CSV not found: {input_csv}")

    run_phase1_taxonomy(
        input_csv=input_csv,
        llm=args.llm,
        proposal_json=paths.proposal_json,
        quality_json=paths.quality_json,
        review_md=paths.review_md,
        canonical_target=args.canonical_target,
        coverage_target=args.coverage_target,
        min_support_records=args.min_support_records,
        max_alias_overlap=args.max_alias_overlap,
        max_iterations=args.max_iterations,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    main()
