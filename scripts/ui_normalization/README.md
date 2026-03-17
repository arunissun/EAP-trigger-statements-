# EAP UI Normalization Pipeline

This pipeline processes extracted EAP (Early Action Protocol) trigger statements and normalizes them into a structured UI schema with cascading dropdown options.

## Overview

The pipeline has 3 steps:

1. **Step 1 – Exploder**: Parses raw JSON and creates one row per threshold string
2. **Step 2 – LLM Normalization**: Calls LLM APIs to extract structured parameters (Azure OpenAI and/or Gemini)
3. **Step 3 – Aggregator**: Builds UI dropdown master lists and trigger logic mapping

## Quick Start

### Run the full pipeline with both LLMs (sequential)

```bash
# From project root
uv run python -m scripts.ui_normalization.run_pipeline
```

This will:
- Generate `normalized_thresholds_openai.csv` and `normalized_thresholds_gemini.csv`
- Generate `ui_schema_openai.json` and `ui_schema_gemini.json`
- Run LLMs one after the other (not in parallel)

### Run with only one LLM

```bash
# Only Azure OpenAI
uv run python -m scripts.ui_normalization.run_pipeline --llm openai

# Only Gemini
uv run python -m scripts.ui_normalization.run_pipeline --llm gemini
```

### Test with limited records

```bash
# Test with first 10 records using both LLMs
uv run python -m scripts.ui_normalization.run_pipeline --limit 10

# Test OpenAI only with 5 records
uv run python -m scripts.ui_normalization.run_pipeline --llm openai --limit 5
```

## Individual Steps

### Step 1 only (no API calls)

```bash
uv run python -m scripts.ui_normalization.run_pipeline --step1-only
# or directly
uv run python -m scripts.ui_normalization.step1_exploder
```

Output: `ui_normalization_output/exploded_thresholds.csv`

### Step 2 only (LLM normalization)

```bash
# Both LLMs
uv run python -m scripts.ui_normalization.run_pipeline --step2-only

# OpenAI only
uv run python -m scripts.ui_normalization.run_pipeline --step2-only --llm openai
uv run python -m scripts.ui_normalization.step2_llm --llm openai

# Gemini only
uv run python -m scripts.ui_normalization.run_pipeline --step2-only --llm gemini
uv run python -m scripts.ui_normalization.step2_llm --llm gemini
```

### Step 3 only (aggregation)

```bash
# Both LLMs
uv run python -m scripts.ui_normalization.run_pipeline --step3-only

# Both LLMs (filtered forecast variables)
uv run python -m scripts.ui_normalization.run_pipeline --step3-only --step3-mode filtered --min-frequency 4

# OpenAI only
uv run python -m scripts.ui_normalization.run_pipeline --step3-only --llm openai
uv run python -m scripts.ui_normalization.step3_aggregator --llm openai

# Gemini only
uv run python -m scripts.ui_normalization.run_pipeline --step3-only --llm gemini
uv run python -m scripts.ui_normalization.step3_aggregator --llm gemini
```

## Output Files

All outputs go to `ui_normalization_output/`:

| File | Description |
|------|-------------|
| `exploded_thresholds.csv` | Step 1 output – one row per threshold string (270 records) |
| `normalized_thresholds_openai.csv` | Step 2 OpenAI – normalized fields from Azure OpenAI |
| `normalized_thresholds_gemini.csv` | Step 2 Gemini – normalized fields from Gemini |
| `ui_schema_openai.json` | Step 3 OpenAI – UI dropdown masters + trigger logic from OpenAI |
| `ui_schema_gemini.json` | Step 3 Gemini – UI dropdown masters + trigger logic from Gemini |
| `ui_schema_openai_filtered.json` | Step 3 filtered OpenAI schema (when `--step3-mode filtered`) |
| `ui_schema_gemini_filtered.json` | Step 3 filtered Gemini schema (when `--step3-mode filtered`) |

## CLI Options

### `run_pipeline.py`

```
--llm {openai,gemini,both}   Which LLM(s) to run (default: both)
--limit N                    Only normalize first N records (for testing)
--step1-only                 Run only Step 1
--step2-only                 Run only Step 2
--step3-only                 Run only Step 3
--skip-step2                 Run Step 1 + Step 3 (requires existing Step 2 output)
--step3-mode {standard,filtered}  Step 3 aggregator mode (default: standard)
--min-frequency N            Min frequency used when --step3-mode filtered (default: 4)
```

### `step2_llm.py`

```
--llm {openai,gemini,auto}   Which LLM to use (default: auto)
```

- `auto` = Azure OpenAI with Gemini fallback (legacy mode)
- `openai` = Azure OpenAI only
- `gemini` = Gemini only

### `step3_aggregator.py`

```
--llm {openai,gemini}        Which LLM's output to aggregate (default: openai)
```

## LLM Configuration

Edit `config.py` to adjust:

- API keys (loaded from `.env`)
- Rate limiting (`API_DELAY_SECONDS`)
- Retry attempts (`MAX_RETRIES`)
- Model names (`AZURE_OPENAI_DEPLOYMENT`, `GEMINI_MODEL`)

## Schema

The normalized output follows this Pydantic schema:

```json
{
  "forecast_variable": "String - the core metric (e.g., 'River discharge', 'Confirmed cholera cases')",
  "threshold_operator": "String - one of: '>=', '<=', '>', '<', '=', 'between'",
  "threshold_value": "Number - the numerical tipping point",
  "threshold_unit": "String - unit of measurement (e.g., 'cusecs', 'mm', 'cases')",
  "probability_value": "Number or null - percentage likelihood without '%'",
  "lead_time_value": "Number - numerical warning time",
  "timeframe_unit": "String - one of: 'hours', 'days', 'weeks', 'months'",
  "normalized_source": "String - agency acronym (e.g., 'FFD', 'GloFAS', 'EPHI')"
}
```

## Trigger Logic Mapping

The pipeline maps each record to one of these UI states:

- **Single**: `activation_type="single-trigger"` AND `is_conditional=false`
- **Conditional (Sequential / AND)**: `is_conditional=true` (dual-trigger or multi-stage)
- **Conditional (Either / OR)**: Multiple thresholds in one statement without conditions

## Example Usage

```bash
# Full production run with both LLMs
uv run python -m scripts.ui_normalization.run_pipeline

# Quick test with OpenAI only (5 records)
uv run python -m scripts.ui_normalization.run_pipeline --llm openai --limit 5

# Compare outputs after running both LLMs
# (manually inspect the two JSON files or CSVs)
```

## Troubleshooting

### API Errors
- Check `.env` file for valid API keys
- Increase `API_DELAY_SECONDS` if hitting rate limits
- Check `llm_error` column in CSV for specific error messages

### Missing Output Files
- Run steps in order: Step 1 → Step 2 → Step 3
- Or use `--skip-step2` if you already have normalized data

### Encoding Issues
- All files use UTF-8 encoding
- On Windows PowerShell, use `chcp 65001` if you see character issues
