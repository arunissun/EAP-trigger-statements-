"""
Configuration module for the Trigger Statement Extraction Pipeline.

Contains all configuration constants, paths, and settings.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# =============================================================================
# Path Configuration
# =============================================================================

# Get the project root directory
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Load environment variables from project root
load_dotenv(PROJECT_ROOT / ".env")

# Folder paths
DOCUMENTS_FOLDER = PROJECT_ROOT / "downloaded_documents"
OUTPUT_FOLDER = PROJECT_ROOT / "extracted_triggers"

# =============================================================================
# API Configuration
# =============================================================================

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = "gemini-3-flash-preview"
API_DELAY_SECONDS = 5  # 4 requests per minute

# =============================================================================
# Extraction Configuration
# =============================================================================

# Keywords to search for when scoring pages (English, French, Spanish)
RELEVANCE_KEYWORDS = [
    # English
    'trigger', 'activation', 'threshold', 'condition', 'forecast',
    'probability', 'lead time', 'early action', 'eap', 's-eap', 'seap',
    'activated', 'mechanism', 'stop', 'deactivate', 'return period',
    'model', 'alert', 'warning', 'preliminary',
    # French
    'déclencheur', 'déclenchement', 'seuil', 'condition', 'prévision',
    'probabilité', 'délai', 'action anticipée', 'activé', 'mécanisme',
    'arrêt', 'désactiver', 'période de retour', 'modèle', 'alerte', 'avertissement',
    # Spanish
    'disparador', 'activación', 'umbral', 'condición', 'pronóstico',
    'probabilidad', 'tiempo de anticipación', 'acción temprana', 'activado',
    'mecanismo', 'parar', 'desactivar', 'período de retorno', 'modelo', 'alerta'
]

# Scoring weights
KEYWORD_SCORE = 1       # Points per keyword occurrence
TABLE_BONUS_SCORE = 2   # Bonus for pages with tables (reduced from 5)

# Page selection settings
TOP_PAGES_TO_SELECT = 3
INCLUDE_NEIGHBOR_PAGES = True

# LLM settings
LLM_TEMPERATURE = 0.1   # Low for consistent extraction
LLM_MAX_TOKENS = 20000
