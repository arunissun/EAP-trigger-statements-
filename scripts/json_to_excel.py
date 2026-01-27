"""
JSON to Excel Converter for Extracted Trigger Statements.

Transforms the extracted triggers JSON output into a structured Excel file
with one row per trigger statement.

Usage:
    uv run python -m scripts.json_to_excel
    OR
    python scripts/json_to_excel.py
"""

import json
import sys
from pathlib import Path

import pandas as pd

# Add the project root to path so imports work when run directly
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import OUTPUT_FOLDER


def load_triggers_json(json_path: Path) -> list:
    """Load the extracted triggers JSON file."""
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def flatten_triggers_to_rows(data: list) -> list[dict]:
    """
    Flatten the nested JSON structure into a list of rows for Excel.
    
    Each trigger statement becomes its own row with document context.
    
    Args:
        data: List of document extraction results
        
    Returns:
        List of dictionaries, one per trigger statement
    """
    rows = []
    
    for doc in data:
        document_name = doc.get("document_name", "")
        document_id = doc.get("document_id", "")
        document_language = doc.get("document_language", "")
        model = doc.get("model", "gemini")  # Track which model was used
        
        triggers = doc.get("triggers", {})
        
        if not triggers:
            # Document with no triggers - still include it with empty trigger fields
            rows.append({
                #"document_id": document_id,
                "document_name": document_name,
                #"document_language": document_language,
                #"model": model,
                "trigger_number": None,
                "statement": None,
                "statement_english": None,
                "thresholds": None,
                "lead_time": None,
                "source_authority": None,
                "geographic_scope": None,
                "is_conditional": None,
                "condition_dependency": None,
                "preliminary_actions": None,
                #"page_ref": None
            })
            continue
        
        # Process each trigger statement
        for trigger_key, trigger_data in triggers.items():
            # Extract trigger number from key (e.g., "trigger_statement_1" -> 1)
            try:
                trigger_num = int(trigger_key.split("_")[-1])
            except (ValueError, IndexError):
                trigger_num = trigger_key
            
            # Convert thresholds list to string for Excel
            thresholds = trigger_data.get("thresholds", [])
            if isinstance(thresholds, list):
                thresholds_str = " | ".join(str(t) for t in thresholds if t)
            else:
                thresholds_str = str(thresholds) if thresholds else None
            
            row = {
                #"document_id": document_id,
                "document_name": document_name,
                #"document_language": document_language,
                #"model": model,
                "trigger_number": trigger_num,
                "statement": trigger_data.get("statement"),
                "statement_english": trigger_data.get("statement_english"),
                "thresholds": thresholds_str,
                "lead_time": trigger_data.get("lead_time"),
                "source_authority": trigger_data.get("source_authority"),
                "geographic_scope": trigger_data.get("geographic_scope"),
                "is_conditional": trigger_data.get("is_conditional"),
                "condition_dependency": trigger_data.get("condition_dependency"),
                "preliminary_actions": trigger_data.get("preliminary_actions"),
                #"page_ref": trigger_data.get("page_ref")
            }
            
            rows.append(row)
    
    return rows


def export_to_excel(rows: list[dict], output_path: Path) -> None:
    """
    Export the flattened trigger data to an Excel file.
    
    Args:
        rows: List of dictionaries with trigger data
        output_path: Path to the output Excel file
    """
    from openpyxl.styles import Alignment
    
    df = pd.DataFrame(rows)
    
    # Reorder columns for better readability
    column_order = [
        #"document_id",
        "document_name",
        #"document_language",
        #"model",
        "trigger_number",
        "statement",
        "statement_english",
        "thresholds",
        "lead_time",
        "source_authority",
        "geographic_scope",
        "is_conditional",
        "condition_dependency",
        "preliminary_actions",
        #"page_ref"
    ]
    
    # Only include columns that exist
    column_order = [col for col in column_order if col in df.columns]
    df = df[column_order]
    
    # Export to Excel with formatting
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Triggers')
        
        worksheet = writer.sheets['Triggers']
        
        # Apply text wrapping to all cells and auto-adjust column widths
        for idx, col in enumerate(df.columns):
            col_letter = chr(65 + idx) if idx < 26 else chr(64 + idx // 26) + chr(65 + idx % 26)
            
            # Calculate max width
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(col)
            )
            # Cap width at 50 characters
            adjusted_width = min(max_length + 2, 50)
            worksheet.column_dimensions[col_letter].width = adjusted_width
        
        # Apply text wrapping to all cells (including header)
        for row in worksheet.iter_rows():
            for cell in row:
                cell.alignment = Alignment(wrap_text=True, vertical='top')
    
    print(f"Exported {len(df)} rows to: {output_path}")


def main():
    """Main execution function."""
    print("\n" + "=" * 70)
    print("JSON TO EXCEL CONVERTER - Trigger Statements")
    print("=" * 70)
    
    # Process Azure OpenAI output (you can change this to process Gemini output)
    json_files = [
        ("extracted_triggers_openai.json", "triggers_openai.xlsx"),
        ("extracted_triggers.json", "triggers_gemini.xlsx"),
    ]
    
    for json_filename, excel_filename in json_files:
        json_path = OUTPUT_FOLDER / json_filename
        
        if not json_path.exists():
            print(f"\nSkipping {json_filename} - file not found")
            continue
        
        print(f"\nProcessing: {json_filename}")
        
        # Load JSON data
        data = load_triggers_json(json_path)
        print(f"  Loaded {len(data)} documents")
        
        # Flatten to rows
        rows = flatten_triggers_to_rows(data)
        print(f"  Flattened to {len(rows)} trigger rows")
        
        # Export to Excel
        excel_path = OUTPUT_FOLDER / excel_filename
        export_to_excel(rows, excel_path)
    
    print("\n" + "=" * 70)
    print("CONVERSION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
