"""
Configuration for the UI Normalization Pipeline.

Inherits shared paths from the parent scripts/config.py and adds
pipeline-specific settings.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Resolve project root so this module works whether run directly or imported
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent          # scripts/ui_normalization/
SCRIPTS_DIR = SCRIPT_DIR.parent             # scripts/
PROJECT_ROOT = SCRIPTS_DIR.parent           # project root

# Add scripts/ to sys.path so we can import sibling modules if needed
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Input / Output paths
# ---------------------------------------------------------------------------
INPUT_JSON = PROJECT_ROOT / "extracted_triggers" / "extracted_triggers_openai.json"
OUTPUT_DIR = PROJECT_ROOT / "ui_normalization_output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EXPLODED_CSV          = OUTPUT_DIR / "exploded_thresholds.csv"
NORMALIZED_CSV_OPENAI = OUTPUT_DIR / "normalized_thresholds_openai.csv"
NORMALIZED_CSV_GEMINI = OUTPUT_DIR / "normalized_thresholds_gemini.csv"
UI_SCHEMA_JSON_OPENAI = OUTPUT_DIR / "ui_schema_openai.json"
UI_SCHEMA_JSON_GEMINI = OUTPUT_DIR / "ui_schema_gemini.json"

# Canonical taxonomy reference (approved lock target)
FORECAST_VARIABLES_REFERENCE_JSON = PROJECT_ROOT / "FORECAST_VARIABLES_REFERENCE.json"

# Phase 0 / 1 artifacts
PHASE0_FLATTENED_CSV_OPENAI = OUTPUT_DIR / "phase0_flattened_candidates_openai.csv"
PHASE0_FLATTENED_CSV_GEMINI = OUTPUT_DIR / "phase0_flattened_candidates_gemini.csv"

TAXONOMY_PROPOSAL_JSON_OPENAI = OUTPUT_DIR / "taxonomy_proposal_openai.json"
TAXONOMY_PROPOSAL_JSON_GEMINI = OUTPUT_DIR / "taxonomy_proposal_gemini.json"
TAXONOMY_QUALITY_JSON_OPENAI = OUTPUT_DIR / "taxonomy_quality_openai.json"
TAXONOMY_QUALITY_JSON_GEMINI = OUTPUT_DIR / "taxonomy_quality_gemini.json"
TAXONOMY_REVIEW_MD_OPENAI = OUTPUT_DIR / "taxonomy_review_openai.md"
TAXONOMY_REVIEW_MD_GEMINI = OUTPUT_DIR / "taxonomy_review_gemini.md"

# ---------------------------------------------------------------------------
# Azure OpenAI (primary LLM)
# ---------------------------------------------------------------------------
AZURE_OPENAI_ENDPOINT   = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_KEY    = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "IFRC_PROD_GPT35T_GO_001")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")

# ---------------------------------------------------------------------------
# Gemini (fallback LLM)
# ---------------------------------------------------------------------------
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL     = "gemini-2.0-flash"          # stable flash model

# ---------------------------------------------------------------------------
# LLM behaviour
# ---------------------------------------------------------------------------
LLM_TEMPERATURE  = 0.0      # deterministic extraction
LLM_MAX_TOKENS   = 512      # schema output is small
API_DELAY_SECONDS = 1.5     # polite rate-limiting between calls
MAX_RETRIES       = 3       # per-record retry attempts

# Phase 1 taxonomy governance defaults
PHASE1_CANONICAL_TARGET = 10
PHASE1_COVERAGE_TARGET = 0.90
PHASE1_MIN_SUPPORT_RECORDS = 3
PHASE1_MAX_ALIAS_OVERLAP = 0.50
PHASE1_MAX_ITERATIONS = 2

# Phase 2/3 governance behavior
REQUIRE_APPROVED_TAXONOMY = True
MAX_PRIMARY_CANONICALS = 13

# ---------------------------------------------------------------------------
# Hazard type inference keywords (matched against document_name)
# ---------------------------------------------------------------------------
HAZARD_KEYWORDS = {
    # Specific hazards first to avoid broad-category collisions.
    "Volcanic Ash": ["volcanic ash", "ash fall", "volcanic"],
    "Wildfire": ["wildfire", "forest fire", "fire weather", "ignition"],
    "Population Movement": ["population movement", "forced displacement", "displacement", "migration"],
    "Dengue": ["dengue"],
    "Cholera": ["cholera", "epidemic", "outbreak", "epidem"],
    "Heatwave": ["heatwave", "heat wave", "extreme heat", "heat index"],
    "Cold Wave": ["cold wave", "coldwave", "cold spell"],
    "Drought": ["drought", "dry", "tercile", "precipitation deficit", "el nino", "el niño"],
    "Flood": ["flood", "floods", "flooding", "inundation", "riverine", "pluvial", "flash flood"],
    "Extreme Rainfall": ["extreme rainfall", "heavy rainfall"],
    "Storm": ["storm", "tropical storm", "hurricane", "typhoon", "cyclone", "tropical depression", "windstorm"],
    "Landslide": ["landslide", "mudslide"],
}
