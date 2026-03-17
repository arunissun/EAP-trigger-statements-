"""
Analyze forecast variable frequency to identify common vs. rare variables.

This script helps identify which forecast variables should be included in UI dropdowns
by analyzing their frequency across all EAP documents. Variables that appear rarely
(only in a few specialized EAPs) can be marked as "special cases" or excluded from
common dropdowns.

Usage:
    python analyze_variable_frequency.py [--llm openai|gemini] [--save] [--min-frequency N]
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Config import
# ---------------------------------------------------------------------------
try:
    from .config import NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import NORMALIZED_CSV_OPENAI, NORMALIZED_CSV_GEMINI

logger = logging.getLogger(__name__)


def analyze_frequency(normalized_csv: Path) -> dict[str, Any]:
    """
    Analyze forecast variable frequency distribution.

    Returns:
        Dictionary with frequency analysis and recommendations
    """
    df = pd.read_csv(normalized_csv)

    # Filter valid forecast variables
    variables = df["forecast_variable"].dropna().astype(str).str.strip()
    variables = variables[variables.notna() & (variables != "") & (variables != "nan")]

    if len(variables) == 0:
        raise ValueError("No valid forecast variables found in CSV")

    # Count frequency
    counts = variables.value_counts()

    # Calculate cumulative stats
    total_records = len(variables)
    stats = {
        "total_threshold_records": total_records,
        "unique_variables": len(counts),
        "variable_frequency": counts.to_dict(),
    }

    # Calculate coverage percentages
    cumsum = 0
    coverage_data = []
    for var, count in counts.items():
        cumsum += count
        pct = (cumsum / total_records) * 100
        coverage_data.append({
            "rank": len(coverage_data) + 1,
            "variable": var,
            "count": int(count),
            "cumulative_count": cumsum,
            "cumulative_pct": round(pct, 2),
        })

    stats["coverage_analysis"] = coverage_data

    # Generate filtering recommendations
    recommendations = _generate_recommendations(counts, total_records)
    stats["recommendations"] = recommendations

    return stats


def _generate_recommendations(counts: pd.Series, total: int) -> dict[str, Any]:
    """Generate filtering recommendations based on frequency analysis."""

    recommendations = {}

    # Find threshold for 95% coverage
    cumsum = 0
    for i, (var, cnt) in enumerate(counts.items()):
        cumsum += cnt
        if (cumsum / total) >= 0.95:
            recommendations["for_95_percent_coverage"] = {
                "count": i + 1,
                "variables": list(counts.index[:i+1]),
                "coverage_pct": round((cumsum / total) * 100, 2),
            }
            break

    # Find threshold for 90% coverage
    cumsum = 0
    for i, (var, cnt) in enumerate(counts.items()):
        cumsum += cnt
        if (cumsum / total) >= 0.90:
            recommendations["for_90_percent_coverage"] = {
                "count": i + 1,
                "coverage_pct": round((cumsum / total) * 100, 2),
            }
            break

    # Variables appearing only once (likely special cases)
    rare_vars = counts[counts == 1]
    recommendations["rare_variables"] = {
        "count": len(rare_vars),
        "variables": list(rare_vars.index),
        "note": "These appear in only one EAP document - may be special/niche cases",
    }

    # Variables appearing 2-3 times (uncommon)
    uncommon_vars = counts[(counts > 1) & (counts <= 3)]
    recommendations["uncommon_variables"] = {
        "count": len(uncommon_vars),
        "variables": list(uncommon_vars.index),
        "note": "These appear in 2-3 EAP documents - may be specific to certain regions/hazards",
    }

    # Common variables (appearing 4+ times)
    common_vars = counts[counts >= 4]
    recommendations["common_variables"] = {
        "count": len(common_vars),
        "variables": list(common_vars.index),
        "pct_of_total": round((common_vars.sum() / len(counts)) * 100, 2),
        "note": "These are good candidates for standard UI dropdown",
    }

    return recommendations


def print_report(stats: dict[str, Any], llm_used: str) -> None:
    """Print a formatted analysis report."""

    print("\n" + "=" * 80)
    print(f"FORECAST VARIABLE FREQUENCY ANALYSIS - {llm_used.upper()}")
    print("=" * 80)

    print(f"\nTotal threshold records: {stats['total_threshold_records']}")
    print(f"Unique forecast variables: {stats['unique_variables']}")

    print("\n" + "-" * 80)
    print("TOP 15 MOST COMMON VARIABLES")
    print("-" * 80)

    coverage = stats["coverage_analysis"]
    for entry in coverage[:15]:
        pct_of_all = (entry["count"] / stats["total_threshold_records"]) * 100
        print(f"  {entry['rank']:2d}. {entry['variable']:45s} | "
              f"count: {entry['count']:3d} ({pct_of_all:5.1f}%) | "
              f"cumulative: {entry['cumulative_pct']:5.1f}%")

    if len(coverage) > 15:
        print(f"  ... and {len(coverage) - 15} more variables")

    recs = stats["recommendations"]

    print("\n" + "-" * 80)
    print("FILTERING RECOMMENDATIONS")
    print("-" * 80)

    if "for_95_percent_coverage" in recs:
        rec = recs["for_95_percent_coverage"]
        print(f"\n[IMPORTANT] For 95% coverage of all triggers:")
        print(f"  Include top {rec['count']} variables => {rec['coverage_pct']}% coverage")

    if "for_90_percent_coverage" in recs:
        rec = recs["for_90_percent_coverage"]
        print(f"\n[IMPORTANT] For 90% coverage of all triggers:")
        print(f"  Include top {rec['count']} variables => {rec['coverage_pct']}% coverage")

    print(f"\n[INFO] Variable distribution:")
    print(f"  Common (≥4 uses):      {recs['common_variables']['count']:3d} variables "
          f"({recs['common_variables']['pct_of_total']:.1f}% of records)")
    print(f"  Uncommon (2-3 uses):   {recs['uncommon_variables']['count']:3d} variables")
    print(f"  Rare (1 use):          {recs['rare_variables']['count']:3d} variables (special/niche cases)")

    print("\n" + "-" * 80)
    print("RECOMMENDED UI DROPDOWN CONFIGURATION")
    print("-" * 80)

    common = recs["common_variables"]["variables"]
    print(f"\n[UI DROPDOWN] Suggested common dropdown ({len(common)} variables):")
    for i, var in enumerate(common, 1):
        count = stats["variable_frequency"][var]
        print(f"   {i:2d}. {var}")

    print(f"\n[SPECIAL] Optional 'Other/Special' section ({recs['rare_variables']['count'] + recs['uncommon_variables']['count']} variables):")
    print(f"   Include these only if user selects 'Other' or for advanced form:")
    uncommon = recs["uncommon_variables"]["variables"] + recs["rare_variables"]["variables"]
    for i, var in enumerate(uncommon[:10], 1):
        count = stats["variable_frequency"][var]
        print(f"   {i:2d}. {var} (appears {count}x)")
    if len(uncommon) > 10:
        print(f"   ... and {len(uncommon) - 10} more")


def _build_markdown_report(stats: dict[str, Any], llm_used: str) -> str:
    """Build a Markdown version of the console report."""

    coverage = stats["coverage_analysis"]
    recs = stats["recommendations"]

    lines: list[str] = []
    lines.append(f"# Forecast Variable Frequency Analysis - {llm_used.upper()}")
    lines.append("")
    lines.append(f"- Total threshold records: **{stats['total_threshold_records']}**")
    lines.append(f"- Unique forecast variables: **{stats['unique_variables']}**")
    lines.append("")

    lines.append("## All Forecast Variables (Ranked)")
    lines.append("")
    lines.append("| Rank | Variable | Count | % of Total | Cumulative % |")
    lines.append("|---|---|---:|---:|---:|")
    for entry in coverage:
        pct_of_all = (entry["count"] / stats["total_threshold_records"]) * 100
        lines.append(
            f"| {entry['rank']} | {entry['variable']} | {entry['count']} | {pct_of_all:.1f}% | {entry['cumulative_pct']:.1f}% |"
        )
    lines.append("")

    lines.append("## Filtering Recommendations")
    lines.append("")

    if "for_95_percent_coverage" in recs:
        rec = recs["for_95_percent_coverage"]
        lines.append(
            f"- For 95% coverage: include top **{rec['count']}** variables ({rec['coverage_pct']}% coverage)."
        )
    if "for_90_percent_coverage" in recs:
        rec = recs["for_90_percent_coverage"]
        lines.append(
            f"- For 90% coverage: include top **{rec['count']}** variables ({rec['coverage_pct']}% coverage)."
        )

    lines.append(
        f"- Common (>=4 uses): **{recs['common_variables']['count']}** variables "
        f"({recs['common_variables']['pct_of_total']:.1f}% of records)."
    )
    lines.append(
        f"- Uncommon (2-3 uses): **{recs['uncommon_variables']['count']}** variables."
    )
    lines.append(
        f"- Rare (1 use): **{recs['rare_variables']['count']}** variables."
    )
    lines.append("")

    common = recs["common_variables"]["variables"]
    lines.append("## Suggested Common Dropdown Variables")
    lines.append("")
    for i, var in enumerate(common, 1):
        lines.append(f"{i}. {var}")
    lines.append("")

    uncommon = recs["uncommon_variables"]["variables"] + recs["rare_variables"]["variables"]
    lines.append("## Optional Other/Special Variables (Sample)")
    lines.append("")
    for i, var in enumerate(uncommon[:10], 1):
        count = stats["variable_frequency"][var]
        lines.append(f"{i}. {var} (appears {count}x)")
    if len(uncommon) > 10:
        lines.append(f"- ... and {len(uncommon) - 10} more")

    lines.append("")
    lines.append("---")
    lines.append("Generated by `scripts.ui_normalization.analyze_variable_frequency`")

    return "\n".join(lines)


def save_markdown_report(stats: dict[str, Any], llm_used: str, output_path: Path) -> None:
    """Save a Markdown report for easy sharing and review."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown = _build_markdown_report(stats, llm_used)
    output_path.write_text(markdown, encoding="utf-8")
    logger.info(f"Saved Markdown report to {output_path}")


def save_filtered_schema(
    stats: dict[str, Any],
    normalized_csv: Path,
    output_path: Path,
    min_frequency: int = 2,
) -> None:
    """
    Save a filtered UI schema that includes frequency metadata.

    This creates an enhanced schema with frequency analysis for each variable.
    """
    df = pd.read_csv(normalized_csv)

    # Filter variables by minimum frequency
    valid_vars = {
        var: count
        for var, count in stats["variable_frequency"].items()
        if count >= min_frequency
    }

    schema = {
        "metadata": {
            "description": "EAP Trigger UI Schema with frequency analysis",
            "analysis_date": pd.Timestamp.now().isoformat(),
            "min_frequency": min_frequency,
            "total_records": stats["total_threshold_records"],
            "unique_variables": stats["unique_variables"],
            "filtered_variables": len(valid_vars),
        },
        "frequency_analysis": {
            "all_variables": stats["variable_frequency"],
            "common_variables": {
                var: count for var, count in stats["variable_frequency"].items()
                if count >= 4
            },
            "coverage": stats["coverage_analysis"][:20],  # Top 20
        },
        "recommendations": stats["recommendations"],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved filtered schema to {output_path}")


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Analyze forecast variable frequency for UI dropdown selection"
    )
    parser.add_argument(
        "--llm",
        choices=["openai", "gemini"],
        default="openai",
        help="Which LLM's output to analyze",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save analysis results to JSON",
    )
    parser.add_argument(
        "--min-frequency",
        type=int,
        default=2,
        help="Minimum frequency threshold for saving filtered schema",
    )
    args = parser.parse_args()

    # Select input based on LLM choice
    normalized_csv = NORMALIZED_CSV_OPENAI if args.llm == "openai" else NORMALIZED_CSV_GEMINI

    if not normalized_csv.exists():
        print(f"❌ Normalized CSV not found at {normalized_csv}")
        exit(1)

    # Run analysis
    stats = analyze_frequency(normalized_csv)
    print_report(stats, args.llm)

    # Optionally save results
    if args.save:
        output_dir = normalized_csv.parent
        markdown_output_file = output_dir / f"variable_frequency_{args.llm}.md"

        save_markdown_report(stats, args.llm, markdown_output_file)

        print(f"\n✅ Analysis Markdown saved to {markdown_output_file}")
