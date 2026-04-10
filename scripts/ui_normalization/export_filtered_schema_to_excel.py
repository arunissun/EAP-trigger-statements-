"""Export filtered UI schema JSON to Excel.

Rules implemented for export:
1. Exclude top-level `trigger_logic_mapping`.
2. Exclude top-level `normalized_records`.
3. From `dropdown_masters._metadata`, exclude:
   - `forecast_variables_common`
   - `forecast_variables_rare`
   - `min_frequency_threshold`
4. Include `variable_frequency` information in a dedicated sheet.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


EXCLUDED_TOP_LEVEL = {"trigger_logic_mapping", "normalized_records"}
EXCLUDED_METADATA_KEYS = {
    "forecast_variables_common",
    "forecast_variables_rare",
    "min_frequency_threshold",
}


def _to_scalar(value: Any) -> Any:
    """Convert nested values to JSON strings for readable scalar tables."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _sheet_name(name: str) -> str:
    """Excel sheet name safety (31 char hard limit)."""
    return name[:31]


def _write_list_sheet(writer: pd.ExcelWriter, sheet_name: str, column_name: str, values: list[Any]) -> None:
    pd.DataFrame({column_name: values}).to_excel(
        writer, index=False, sheet_name=_sheet_name(sheet_name)
    )


def _write_dict_of_lists_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    data: dict[str, Any],
    key_col: str,
    value_col: str,
) -> None:
    rows: list[dict[str, Any]] = []
    for key, values in data.items():
        if isinstance(values, list):
            for item in values:
                rows.append({key_col: key, value_col: item})
        else:
            rows.append({key_col: key, value_col: _to_scalar(values)})

    pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=_sheet_name(sheet_name))


def _write_nested_dict_of_lists_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    data: dict[str, Any],
    parent_col: str,
    child_col: str,
    value_col: str,
) -> None:
    rows: list[dict[str, Any]] = []
    for parent, child_map in data.items():
        if not isinstance(child_map, dict):
            rows.append(
                {
                    parent_col: parent,
                    child_col: "",
                    value_col: _to_scalar(child_map),
                }
            )
            continue

        for child, values in child_map.items():
            if isinstance(values, list):
                for item in values:
                    rows.append(
                        {
                            parent_col: parent,
                            child_col: child,
                            value_col: item,
                        }
                    )
            else:
                rows.append(
                    {
                        parent_col: parent,
                        child_col: child,
                        value_col: _to_scalar(values),
                    }
                )

    pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=_sheet_name(sheet_name))


def export_schema_to_excel(input_json: Path, output_excel: Path) -> None:
    with input_json.open("r", encoding="utf-8") as f:
        schema = json.load(f)

    output_excel.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        # 1) Top-level metadata
        metadata = schema.get("metadata", {})
        if isinstance(metadata, dict) and metadata:
            md_df = pd.DataFrame(
                [{"field": k, "value": _to_scalar(v)} for k, v in metadata.items()]
            )
            md_df.to_excel(writer, index=False, sheet_name=_sheet_name("metadata"))

        # 2) dropdown_masters: flattened sheets for designer readability
        dropdown_masters = schema.get("dropdown_masters", {})
        if isinstance(dropdown_masters, dict):
            primary = dropdown_masters.get("primary_dropdown_top10", [])
            if isinstance(primary, list) and primary:
                _write_list_sheet(writer, "primary_canonicals", "canonical_variable", primary)

            secondary = dropdown_masters.get("secondary_subcategory_dropdown", {})
            if isinstance(secondary, dict) and secondary:
                _write_dict_of_lists_sheet(
                    writer,
                    "secondary_subcategories",
                    secondary,
                    "canonical_variable",
                    "subcategory",
                )

            advanced = dropdown_masters.get("advanced_other_search", [])
            if isinstance(advanced, list) and advanced:
                _write_list_sheet(
                    writer,
                    "advanced_other_search",
                    "forecast_variable",
                    advanced,
                )

            canon_units = dropdown_masters.get("canonical_to_units", {})
            if isinstance(canon_units, dict) and canon_units:
                _write_dict_of_lists_sheet(
                    writer,
                    "canonical_to_units",
                    canon_units,
                    "canonical_variable",
                    "unit",
                )

            canon_ops = dropdown_masters.get("canonical_to_operators", {})
            if isinstance(canon_ops, dict) and canon_ops:
                _write_dict_of_lists_sheet(
                    writer,
                    "canonical_to_operators",
                    canon_ops,
                    "canonical_variable",
                    "operator",
                )

            subcat_units = dropdown_masters.get("subcategory_to_units_overrides", {})
            if isinstance(subcat_units, dict) and subcat_units:
                _write_nested_dict_of_lists_sheet(
                    writer,
                    "subcat_units_override",
                    subcat_units,
                    "canonical_variable",
                    "subcategory",
                    "unit",
                )

            subcat_ops = dropdown_masters.get("subcategory_to_operators_overrides", {})
            if isinstance(subcat_ops, dict) and subcat_ops:
                _write_nested_dict_of_lists_sheet(
                    writer,
                    "subcat_ops_override",
                    subcat_ops,
                    "canonical_variable",
                    "subcategory",
                    "operator",
                )

            for list_key, column_name in (
                ("sources", "source"),
                ("timeframe_units", "timeframe_unit"),
                ("hazard_types", "hazard_type"),
            ):
                values = dropdown_masters.get(list_key, [])
                if isinstance(values, list) and values:
                    _write_list_sheet(writer, list_key, column_name, values)

        # 3) dropdown_masters._metadata (filtered)
        dm_meta = dropdown_masters.get("_metadata", {}) if isinstance(dropdown_masters, dict) else {}
        if isinstance(dm_meta, dict):
            retained_meta = {
                k: v for k, v in dm_meta.items() if k not in EXCLUDED_METADATA_KEYS and k != "variable_frequency"
            }

            if retained_meta:
                pd.DataFrame(
                    [{"field": k, "value": _to_scalar(v)} for k, v in retained_meta.items()]
                ).to_excel(writer, index=False, sheet_name=_sheet_name("metadata_retained"))

            variable_frequency = dm_meta.get("variable_frequency", {})
            if isinstance(variable_frequency, dict) and variable_frequency:
                vf_df = pd.DataFrame(
                    [
                        {"forecast_variable": var, "count": count}
                        for var, count in variable_frequency.items()
                    ]
                ).sort_values(by=["count", "forecast_variable"], ascending=[False, True])
                vf_df.to_excel(writer, index=False, sheet_name=_sheet_name("variable_frequency"))

        # 4) Optional overview of retained top-level keys
        top_level_overview = {
            k: v for k, v in schema.items() if k not in EXCLUDED_TOP_LEVEL
        }
        overview_df = pd.DataFrame(
            [
                {
                    "top_level_key": k,
                    "type": type(v).__name__,
                    "notes": "included",
                }
                for k, v in top_level_overview.items()
            ]
        )
        overview_df.to_excel(writer, index=False, sheet_name=_sheet_name("overview"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export filtered UI schema JSON to Excel with custom exclusions."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("ui_normalization_output/ui_schema_openai_filtered.json"),
        help="Path to input filtered schema JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("ui_normalization_output/ui_schema_openai_filtered.xlsx"),
        help="Path to output Excel file",
    )
    args = parser.parse_args()

    export_schema_to_excel(args.input, args.output)
    print(f"Created Excel: {args.output}")


if __name__ == "__main__":
    main()
