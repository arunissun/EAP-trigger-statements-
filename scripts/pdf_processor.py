"""
PDF Processing module for the Trigger Statement Extraction Pipeline.

Handles PDF text and table extraction using PyMuPDF.
"""

import pymupdf  # PyMuPDF library
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class PageData:
    """Represents extracted data from a single PDF page."""
    page_num: int
    text: str
    tables: list[str] = field(default_factory=list)
    relevance_score: int = 0
    has_tables: bool = False


def table_to_markdown(table) -> str:
    """
    Convert a PyMuPDF table to markdown format for token efficiency.
    
    Args:
        table: PyMuPDF table object
        
    Returns:
        Markdown-formatted table string
    """
    try:
        data = table.extract()
        
        if not data or len(data) == 0:
            return ""
        
        lines = []
        
        # Header row
        header = data[0]
        header_cells = [str(cell).strip() if cell else "" for cell in header]
        lines.append("| " + " | ".join(header_cells) + " |")
        
        # Separator
        lines.append("| " + " | ".join(["---"] * len(header)) + " |")
        
        # Data rows
        for row in data[1:]:
            cells = [str(cell).strip() if cell else "" for cell in row]
            while len(cells) < len(header):
                cells.append("")
            lines.append("| " + " | ".join(cells[:len(header)]) + " |")
        
        return "\n".join(lines)
    except Exception:
        return ""


def extract_page_content(page, page_num: int) -> PageData:
    """
    Extract text and tables from a single PDF page.
    
    Args:
        page: PyMuPDF page object
        page_num: 1-indexed page number
        
    Returns:
        PageData object with extracted content
    """
    # Extract text
    text = page.get_text()
    
    # Extract tables
    tables = []
    try:
        page_tables = page.find_tables()
        for table in page_tables:
            markdown_table = table_to_markdown(table)
            if markdown_table:
                tables.append(markdown_table)
    except Exception as e:
        print(f"    Warning: Could not extract tables from page {page_num}: {e}")
    
    return PageData(
        page_num=page_num,
        text=text,
        tables=tables,
        has_tables=len(tables) > 0
    )


def process_pdf(pdf_path: Path) -> list[PageData]:
    """
    Process a PDF file and extract content from all pages.
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        List of PageData objects for all pages
    """
    pages_data = []
    
    try:
        doc = pymupdf.open(pdf_path)
        print(f"  Processing {doc.page_count} pages...")
        
        for page_num in range(doc.page_count):
            page = doc[page_num]
            page_data = extract_page_content(page, page_num + 1)  # 1-indexed
            pages_data.append(page_data)
        
        doc.close()
        
    except Exception as e:
        print(f"  Error processing PDF: {e}")
    
    return pages_data
