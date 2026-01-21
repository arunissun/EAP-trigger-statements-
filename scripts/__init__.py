"""
Scripts package for the Trigger Statement Extraction Pipeline.
"""

from .config import (
    DOCUMENTS_FOLDER,
    OUTPUT_FOLDER,
    GEMINI_API_KEY,
    GEMINI_MODEL
)

from .pdf_processor import (
    PageData,
    process_pdf,
    extract_page_content
)

from .page_selector import (
    score_all_pages,
    select_relevant_pages,
    calculate_relevance_score
)

from .payload_builder import construct_payload

from .llm_extractor import extract_triggers_with_llm

__all__ = [
    # Config
    'DOCUMENTS_FOLDER',
    'OUTPUT_FOLDER',
    'GEMINI_API_KEY',
    'GEMINI_MODEL',
    # PDF Processing
    'PageData',
    'process_pdf',
    'extract_page_content',
    # Page Selection
    'score_all_pages',
    'select_relevant_pages',
    'calculate_relevance_score',
    # Payload
    'construct_payload',
    # LLM
    'extract_triggers_with_llm',
]
