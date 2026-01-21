"""
Payload Builder module for the Trigger Statement Extraction Pipeline.

Constructs token-optimized payloads from selected pages for LLM processing.
"""

from .pdf_processor import PageData


def construct_payload(selected_pages: list[PageData]) -> str:
    """
    Construct a token-optimized payload from selected pages.
    
    Format:
    - Page header: --- PAGE {n} ---
    - Tables (markdown format) if present
    - Regular text content
    
    Args:
        selected_pages: List of selected PageData objects
        
    Returns:
        Constructed payload string
    """
    if not selected_pages:
        return ""
    
    payload_parts = []
    
    for page in selected_pages:
        # Page header
        payload_parts.append(f"\n--- PAGE {page.page_num} ---\n")
        
        # Tables (markdown format for token efficiency)
        if page.tables:
            payload_parts.append("\n[TABLES]\n")
            for table in page.tables:
                payload_parts.append(table)
                payload_parts.append("\n")
        
        # Text content
        if page.text.strip():
            payload_parts.append("\n[TEXT]\n")
            payload_parts.append(page.text.strip())
            payload_parts.append("\n")
    
    payload = "".join(payload_parts)
    print(f"  Payload constructed: {len(payload)} characters")
    
    return payload
