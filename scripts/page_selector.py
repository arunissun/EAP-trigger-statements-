"""
Page Selection module for the Trigger Statement Extraction Pipeline.

Handles relevance scoring and page selection using coarse-to-fine filtering.
"""

from .pdf_processor import PageData
from .config import (
    RELEVANCE_KEYWORDS,
    KEYWORD_SCORE,
    TABLE_BONUS_SCORE,
    TOP_PAGES_TO_SELECT,
    INCLUDE_NEIGHBOR_PAGES
)


def calculate_relevance_score(page_data: PageData) -> int:
    """
    Calculate relevance score for a page based on keyword occurrences and table presence.
    
    Scoring:
    - +1 point for each keyword occurrence
    - +2 points bonus if page contains tables
    
    Args:
        page_data: PageData object
        
    Returns:
        Relevance score as integer
    """
    score = 0
    
    # Combine text and tables for keyword search
    full_text = page_data.text.lower()
    for table in page_data.tables:
        full_text += " " + table.lower()
    
    # Count keyword occurrences
    for keyword in RELEVANCE_KEYWORDS:
        count = full_text.count(keyword.lower())
        score += count * KEYWORD_SCORE
    
    # Table bonus (reduced - triggers may or may not be in tables)
    if page_data.has_tables:
        score += TABLE_BONUS_SCORE
    
    page_data.relevance_score = score
    return score


def score_all_pages(pages_data: list[PageData]) -> list[PageData]:
    """
    Calculate relevance scores for all pages.
    
    Args:
        pages_data: List of PageData objects
        
    Returns:
        Same list with scores calculated
    """
    for page_data in pages_data:
        calculate_relevance_score(page_data)
    
    return pages_data


def select_relevant_pages(pages_data: list[PageData]) -> list[PageData]:
    """
    Select the most relevant pages using the coarse-to-fine approach.
    
    Strategy:
    1. Filter pages with score > 0
    2. Sort pages by relevance score (descending)
    3. Select top N pages
    4. Include neighbor pages (N-1 and N+1) for context
    
    Args:
        pages_data: List of all PageData objects (already scored)
        
    Returns:
        List of selected PageData objects (sorted by page number)
    """
    # Filter pages with score > 0
    scored_pages = [p for p in pages_data if p.relevance_score > 0]
    
    if not scored_pages:
        print("  Warning: No pages matched the relevance criteria")
        return []
    
    # Sort by relevance score (descending)
    scored_pages.sort(key=lambda p: p.relevance_score, reverse=True)
    
    # Select top pages
    top_pages = scored_pages[:TOP_PAGES_TO_SELECT]
    
    # Get page numbers to include (with neighbors)
    selected_page_nums = set()
    total_pages = len(pages_data)
    
    for page in top_pages:
        selected_page_nums.add(page.page_num)
        
        if INCLUDE_NEIGHBOR_PAGES:
            # Add previous page
            if page.page_num > 1:
                selected_page_nums.add(page.page_num - 1)
            # Add next page
            if page.page_num < total_pages:
                selected_page_nums.add(page.page_num + 1)
    
    # Get the actual page data objects (sorted by page number)
    selected_pages = [p for p in pages_data if p.page_num in selected_page_nums]
    selected_pages.sort(key=lambda p: p.page_num)
    
    print(f"  Selected {len(selected_pages)} pages: {[p.page_num for p in selected_pages]}")
    print(f"  Top scoring pages: {[(p.page_num, p.relevance_score) for p in top_pages]}")
    
    return selected_pages
