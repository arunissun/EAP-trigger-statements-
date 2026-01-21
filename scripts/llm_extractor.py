"""
LLM Extractor module for the Trigger Statement Extraction Pipeline.

Handles Gemini API interaction and trigger extraction.
"""

import json
from google import genai
from google.genai import types

from .config import (
    GEMINI_API_KEY,
    GEMINI_MODEL,
    LLM_TEMPERATURE,
    LLM_MAX_TOKENS
)


# =============================================================================
# Prompts
# =============================================================================

SYSTEM_PROMPT = """You are an expert analyst specializing in disaster risk management and Early Action Protocols (EAP/sEAP). 
Your task is to extract COMPREHENSIVE trigger information from the provided document content.

## MULTILINGUAL SUPPORT
The document may be in English, French, Spanish, or other languages. You MUST:
1. Recognize trigger statements regardless of language
2. In FRENCH, look for: "déclencheur", "énoncé de déclenchement", "seuil", "activation", "prévision", "probabilité"
3. In SPANISH, look for: "disparador", "umbral", "activación", "pronóstico", "probabilidad"
4. Extract the trigger statement in its ORIGINAL language, then provide an English translation if not in English
5. ALL other fields (threshold, source, lead_time, etc.) should be extracted/translated to English

## What to Extract

### 1. Trigger Mechanism Overview
- How the trigger system works (single trigger, dual-trigger, multi-stage)
- Whether triggers are conditional (e.g., Trigger 1 must be met before Trigger 2)
- Whether there is a stop/deactivation mechanism

### 2. Individual Trigger Statements
For EACH DISTINCT trigger found, extract:
- The complete trigger statement text (in original language)
- English translation of the statement (if original is not English)
- ALL threshold values if there are multiple conditions (as an array)
- Source/Authority (e.g., "CENAOS", "NOAA", "ECMWF", "ONM", "SONADER")
- Lead time (e.g., "3 to 5 days", "2 months ahead", "5 days")
- Geographic scope (e.g., "Mauritania", "flood zones", "river basins")
- Whether this trigger is conditional on another trigger
- Any preliminary actions that can be taken when this specific trigger is met

## CRITICAL GUIDELINES - AVOIDING DUPLICATES
1. Do NOT create separate trigger entries for bullet points that are part of the SAME trigger
2. If a trigger has multiple conditions (e.g., "based on: • condition A • condition B • condition C"), 
   keep them as ONE trigger with multiple thresholds in the thresholds array
3. Each trigger statement text should appear ONLY ONCE in your output
4. A new trigger entry should only be created for a COMPLETELY DIFFERENT trigger mechanism

## CONSOLIDATION OF TRIGGER STAGES
IMPORTANT: Many EAPs describe "Monitoring", "Pre-Activation", and "Activation" as separate phases.
These are STAGES of the SAME trigger mechanism, NOT different triggers.
You MUST consolidate them into a SINGLE trigger entry:
- Combine all thresholds from all stages into the thresholds array
- Use the MOST SPECIFIC threshold (usually from Activation stage) as the primary statement
- Mention other stages (Monitoring, Pre-activation) in the "preliminary_actions" field
- Only create SEPARATE trigger entries for DIFFERENT hazards (e.g., Flood trigger vs Cyclone trigger)

## OTHER GUIDELINES
1. Do NOT miss ANY trigger-related information
2. Extract threshold values EXACTLY as stated in the document
3. Capture ALL context about how triggers work together
4. Include information about stop mechanisms if mentioned
5. Look in BOTH regular text AND tables
6. If trigger information spans multiple paragraphs, combine it

Respond ONLY with valid JSON in the exact format specified."""


USER_PROMPT_TEMPLATE = """Analyze the following document content and extract ALL trigger-related information.
The document may be in English, French, Spanish, or another language - extract triggers regardless of language.

DOCUMENT CONTENT:
{content}

---

Extract comprehensive trigger information and return as JSON with this structure:
{{
  "document_language": "detected language of the document (e.g., English, French, Spanish)",
  "trigger_mechanism": {{
    "description": "Brief description of how the overall EAP trigger mechanism works (in English)",
    "activation_type": "single-trigger" | "dual-trigger" | "multi-stage",
    "has_stop_mechanism": true | false,
    "stop_mechanism_description": "Description of stop mechanism if exists, null if none"
  }},
  "triggers": [
    {{
      "trigger_statement": "The COMPLETE trigger statement as found in the document (in ORIGINAL language)",
      "statement_english": "English translation of the trigger statement (null if already in English)",
      "thresholds": ["threshold 1 (in English)", "threshold 2", "..."],
      "source_authority": "The source or model used (e.g., CENAOS, NOAA, ECMWF, ONM)",
      "lead_time": "How far in advance this trigger activates (e.g., 3-5 days)",
      "geographic_scope": "Geographic area this applies to",
      "is_conditional": true | false,
      "condition_dependency": "If conditional, describe dependency",
      "preliminary_actions": "Any actions that can be taken when just this trigger is met, null if none",
      "page_ref": <page number as integer>
    }}
  ]
}}

CRITICAL - AVOIDING DUPLICATES:
- Do NOT create separate trigger entries for each bullet point that is part of the same trigger
- If a trigger says "triggered based on: • A • B • C", that is ONE trigger with thresholds: ["A", "B", "C"]
- Each unique trigger statement should appear ONLY ONCE
- Only create a new trigger entry for a COMPLETELY DIFFERENT trigger in the document

MULTILINGUAL - TRANSLATION REQUIREMENTS:
- For French documents, look for: "déclencheur", "énoncé de déclenchement", "seuil", "pré-saison", "prévision"
- For Spanish documents, look for: "disparador", "umbral", "activación"
- ONLY trigger_statement keeps the ORIGINAL language text
- statement_english provides the English translation of trigger_statement
- ALL OTHER FIELDS MUST BE IN ENGLISH:
  * thresholds: translate each threshold to English (e.g., "probabilité de 40%" → "40% probability")
  * source_authority: keep names but translate descriptions to English
  * lead_time: translate to English (e.g., "5 jours" → "5 days")
  * geographic_scope: translate to English (e.g., "bassins fluviaux" → "river basins")
  * condition_dependency: translate to English
  * preliminary_actions: translate to English

OTHER INSTRUCTIONS:
- Extract ALL trigger statements, do not miss any
- Include ALL relevant context about thresholds, conditions, and mechanisms
- If a field's information is not found, use null
- If no triggers found, return: {{"trigger_mechanism": null, "triggers": [], "notes": []}}

Return ONLY the JSON object, no additional text."""


# =============================================================================
# Extraction Functions
# =============================================================================

def extract_triggers_with_llm(payload: str) -> dict:
    """
    Send payload to Gemini API and extract trigger statements.
    
    Args:
        payload: Constructed content payload
        
    Returns:
        Dictionary with extracted triggers
    """
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY not found in environment variables!")
    
    if not payload or not payload.strip():
        print("  Warning: Empty payload, skipping LLM call")
        return {"triggers": []}
    
    # Initialize client
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    # Prepare the prompt
    user_prompt = USER_PROMPT_TEMPLATE.format(content=payload)
    
    print(f"  Sending {len(payload)} characters to LLM...")
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                temperature=LLM_TEMPERATURE,
                max_output_tokens=LLM_MAX_TOKENS,
                response_mime_type="application/json",
            )
        )
        
        # Parse response
        response_text = response.text.strip()
        
        # Clean up response (remove markdown code blocks if present)
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        
        response_text = response_text.strip()
        
        # Parse JSON
        result = json.loads(response_text)
        
        # Handle case where LLM returns a list (triggers) instead of a dict
        if isinstance(result, list):
            result = {"triggers": result}
        
        triggers_count = len(result.get("triggers", []))
        print(f"  Extracted {triggers_count} trigger statements")
        
        return result
        
    except json.JSONDecodeError as e:
        print(f"  Error parsing JSON: {e}")
        print(f"  Raw response text preview: {response_text[:500]}...")
        return {"error": "JSON parsing failed"}
    except Exception as e:
        print(f"  Error during LLM extraction: {e}")
        return {"error": str(e)}
