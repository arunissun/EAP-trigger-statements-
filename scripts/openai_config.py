"""
Azure OpenAI Configuration module for the Trigger Statement Extraction Pipeline.

Contains Azure OpenAI-specific configuration constants and settings.
Imports shared settings from the existing config module.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Import shared configuration from existing config
from .config import (
    SCRIPT_DIR,
    PROJECT_ROOT,
    DOCUMENTS_FOLDER,
    OUTPUT_FOLDER,
    RELEVANCE_KEYWORDS,
    KEYWORD_SCORE,
    TABLE_BONUS_SCORE,
    TOP_PAGES_TO_SELECT,
    INCLUDE_NEIGHBOR_PAGES
)

# Load environment variables from project root
load_dotenv(PROJECT_ROOT / ".env")

# =============================================================================
# Azure OpenAI API Configuration
# =============================================================================

AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")

# API rate limiting (Azure OpenAI typically allows more requests per minute)
API_DELAY_SECONDS = 3  # Adjust based on your tier

# =============================================================================
# LLM Settings
# =============================================================================

LLM_TEMPERATURE = 0.1     # Low for consistent extraction
LLM_MAX_TOKENS = 4096     # GPT-3.5 Turbo has a smaller context window than Gemini
