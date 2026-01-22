"""
Trigger Statement Extraction Pipeline - Main Entry Point

A cost-optimized pipeline to extract "Trigger Statements" from PDF reports.

Pipeline Steps:
1. PDF Processing - Extract text and tables from all pages
2. Page Scoring - Calculate relevance scores using keyword heuristics  
3. Page Selection - Select top pages + neighbors for context
4. Payload Construction - Build token-optimized content
5. LLM Extraction - Extract triggers via Gemini API

Usage:
    uv run python -m scripts.main
    OR
    python scripts/main.py
"""

import sys
import json
import re
import time
from pathlib import Path

import pandas as pd

# Add the project root to path so imports work when run directly
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import DOCUMENTS_FOLDER, OUTPUT_FOLDER, API_DELAY_SECONDS
from scripts.pdf_processor import process_pdf
from scripts.page_selector import score_all_pages, select_relevant_pages
from scripts.payload_builder import construct_payload
from scripts.llm_extractor import extract_triggers_with_llm


# =============================================================================
# Document Metadata Functions
# =============================================================================

def load_document_metadata() -> dict:
    """
    Load document metadata from appeal_documents.xlsx.
    
    Returns:
        Dictionary mapping document ID to document name
    """
    excel_path = DOCUMENTS_FOLDER / "appeal_documents.xlsx"
    
    if not excel_path.exists():
        print(f"Warning: {excel_path} not found. Document names will not be included.")
        return {}
    
    try:
        df = pd.read_excel(excel_path)
        # Create a mapping of id -> name
        metadata = {}
        for _, row in df.iterrows():
            doc_id = row.get("id")
            doc_name = row.get("name")
            if doc_id is not None:
                metadata[int(doc_id)] = doc_name
        print(f"Loaded metadata for {len(metadata)} documents")
        return metadata
    except Exception as e:
        print(f"Warning: Could not load document metadata: {e}")
        return {}


def extract_document_id(filename: str) -> int | None:
    """
    Extract document ID from filename.
    
    The filename format is typically: {id}_{rest_of_name}.pdf
    For example: 16389_EAP2025HN05-Summary.pdf -> 16389
    
    Args:
        filename: The PDF filename
        
    Returns:
        The document ID as integer, or None if not found
    """
    # Match digits at the start of the filename
    match = re.match(r'^(\d+)_', filename)
    if match:
        return int(match.group(1))
    return None


# =============================================================================
# PDF Processing Functions
# =============================================================================


def prepare_pdf_payload(pdf_path: Path, document_metadata: dict = None) -> tuple[str, dict]:
    """
    Process a PDF to extract text, select pages, and construct LLM payload.
    Does NOT call the LLM.
    
    Args:
        pdf_path: Path to PDF file
        document_metadata: Optional dict mapping document IDs to names
        
    Returns:
        tuple: (payload_string, result_metadata_dict)
    """
    print(f"\n{'='*60}")
    print(f"Processing: {pdf_path.name}")
    print(f"{'='*60}")
    
    # Extract document ID and name from metadata
    doc_id = extract_document_id(pdf_path.name)
    doc_name = None
    if document_metadata and doc_id:
        doc_name = document_metadata.get(doc_id)
    
    result = {
        "file": pdf_path.name,
        "document_id": doc_id,
        "document_name": doc_name,
        "document_language": None,
        "status": "success",
        "pages_analyzed": 0,
        "pages_selected": 0,
        "trigger_mechanism": None,
        "triggers": {}
    }
    
    # Step 1: Extract content from all pages
    print("\n[Step 1] Extracting content from PDF...")
    pages_data = process_pdf(pdf_path)
    
    if not pages_data:
        result["status"] = "error"
        result["error"] = "Failed to extract pages from PDF"
        return None, result
    
    result["pages_analyzed"] = len(pages_data)
    
    # Step 2: Score all pages by relevance
    print("\n[Step 2] Calculating relevance scores...")
    pages_data = score_all_pages(pages_data)
    
    # Show score distribution
    scored_count = sum(1 for p in pages_data if p.relevance_score > 0)
    print(f"  Pages with score > 0: {scored_count}/{len(pages_data)}")
    
    # Step 3: Select most relevant pages + neighbors
    print("\n[Step 3] Selecting relevant pages...")
    selected_pages = select_relevant_pages(pages_data)
    
    if not selected_pages:
        result["status"] = "no_matches"
        result["error"] = "No pages matched the relevance criteria"
        return None, result
    
    result["pages_selected"] = len(selected_pages)
    
    # Step 4: Construct payload from selected pages
    print("\n[Step 4] Constructing payload...")
    payload = construct_payload(selected_pages)
    
    if not payload:
        result["status"] = "error"
        result["error"] = "Failed to construct payload"
        return None, result
        
    return payload, result


def process_single_pdf(pdf_path: Path, document_metadata: dict = None) -> dict:
    """
    Process a single PDF file and extract trigger statements.
    THIS IS THE SYNCHRONOUS VERSION (Direct API Call).
    """
    payload, result = prepare_pdf_payload(pdf_path, document_metadata)
    
    if not payload:
        return result
    
    # Step 5: Extract triggers via LLM
    print("\n[Step 5] Extracting triggers via LLM...")
    llm_result = extract_triggers_with_llm(payload)
    
    # Store document language and trigger mechanism
    result["document_language"] = llm_result.get("document_language")
    result["trigger_mechanism"] = llm_result.get("trigger_mechanism")
    
    # Format triggers with sequential labels and preserve all fields
    raw_triggers = llm_result.get("triggers", [])
    formatted_triggers = {}
    for i, trigger in enumerate(raw_triggers, 1):
        trigger_key = f"trigger_statement_{i}"
        # Handle both possible field names (trigger_statement or statement)
        statement = trigger.get("trigger_statement") or trigger.get("statement", "")
        formatted_triggers[trigger_key] = {
            "statement": statement,
            "statement_english": trigger.get("statement_english"),
            "thresholds": trigger.get("thresholds", []),
            "source_authority": trigger.get("source_authority"),
            "lead_time": trigger.get("lead_time"),
            "geographic_scope": trigger.get("geographic_scope"),
            "is_conditional": trigger.get("is_conditional"),
            "condition_dependency": trigger.get("condition_dependency"),
            "preliminary_actions": trigger.get("preliminary_actions"),
            "page_ref": trigger.get("page_ref")
        }
    
    result["triggers"] = formatted_triggers
    
    # Store notes
    #result["notes"] = llm_result.get("notes", [])
    
    if "error" in llm_result:
        result["status"] = "partial"
        result["warning"] = llm_result["error"]
    
    return result


def main():
    """
    Main execution function.
    
    Processes first 2 PDFs for testing purposes.
    """
    print("\n" + "=" * 70)
    print("TRIGGER STATEMENT EXTRACTION PIPELINE")
    print("=" * 70)
    
    # Ensure output folder exists
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    
    # Load document metadata from Excel
    print("\nLoading document metadata...")
    document_metadata = load_document_metadata()
    
    # Get PDF files (excluding excel/csv files)
    pdf_files = [
        f for f in DOCUMENTS_FOLDER.iterdir() 
        if f.is_file() and f.suffix.lower() not in ['.xlsx', '.xls', '.csv']
    ]
    
    if not pdf_files:
        print("No PDF files found in the documents folder!")
        return
    
    print(f"\nFound {len(pdf_files)} PDF files")
    print(f"Testing with first 2 files...\n")
    
    # Process only first 2 PDFs for testing
    test_files = pdf_files[:65]
    #all_files = pdf_files
    all_results = []
    
    for pdf_path in test_files:
        result = process_single_pdf(pdf_path, document_metadata)
        all_results.append(result)
        
        # Save progress after each file (checkpoint)
        output_file = OUTPUT_FOLDER / "extracted_triggers.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"  [Saved to JSON]")
          
        # Rate limiting delay
        print(f"Waiting {API_DELAY_SECONDS}s for rate limit...")
        time.sleep(API_DELAY_SECONDS)
        
        # Print summary for this file
        triggers = result['triggers']
        mechanism = result.get('trigger_mechanism')
        #notes = result.get('notes', [])
        
        print(f"\n  Summary: {len(triggers)} triggers found")
        print(f"  Document ID: {result.get('document_id', 'N/A')}")
        print(f"  Document Name: {result.get('document_name', 'N/A')}")
        if mechanism:
            print(f"  Activation Type: {mechanism.get('activation_type', 'N/A')}")
            print(f"  Has Stop Mechanism: {mechanism.get('has_stop_mechanism', 'N/A')}")
        for trigger_key, trigger_data in triggers.items():
            statement = trigger_data.get('statement', 'N/A')
            threshold = trigger_data.get('threshold', 'N/A')
            page = trigger_data.get('page_ref', '?')
            print(f"    {trigger_key}: [Page {page}] {statement[:50]}...")
    
    # Save results to JSON
    output_file = OUTPUT_FOLDER / "extracted_triggers.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*70}")
    print("EXTRACTION COMPLETE")
    print(f"{'='*70}")
    print(f"Results saved to: {output_file}")
    print(f"Total files processed: {len(all_results)}")
    print(f"Total triggers extracted: {sum(len(r['triggers']) for r in all_results)}")


if __name__ == "__main__":
    main()
