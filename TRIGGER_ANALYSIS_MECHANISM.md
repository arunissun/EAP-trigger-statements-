# EAP Trigger Statement Analysis Mechanism

## Overview

This project extracts **Trigger Statements** from Early Action Protocol (EAP/sEAP) PDF documents using a **hybrid approach** combining:
1. **Keyword-based relevance scoring** (rule-based filtering)
2. **LLM-powered extraction** (Gemini or Azure OpenAI)

The pipeline is designed to be **cost-optimized** by minimizing token usage while maintaining extraction quality.

---

## Architecture Diagram

```
PDF Documents
    ↓
[1. PDF Processing] → Extract text & tables from all pages
    ↓
[2. Page Scoring] → Calculate relevance scores using keywords
    ↓
[3. Page Selection] → Select top 3 pages + neighbors
    ↓
[4. Payload Construction] → Build token-optimized content
    ↓
[5. LLM Extraction] → Extract triggers via API (Gemini/OpenAI)
    ↓
[6. Output Generation] → JSON + Excel files
```

---

## Detailed Mechanism

### **Step 1: PDF Processing** (`pdf_processor.py`)

**Purpose**: Extract raw content from PDF files.

**Process**:
1. Load PDF using **PyMuPDF** library
2. For each page:
   - Extract **text content** using `page.get_text()`
   - Extract **tables** and convert to **Markdown format** (token-efficient)
3. Store as `PageData` objects with:
   - `page_num`: Page number
   - `text`: Extracted text
   - `tables`: List of markdown-formatted tables
   - `has_tables`: Boolean flag

**Key Feature**: Tables are converted to Markdown to reduce token count while preserving structure.

---

### **Step 2: Page Scoring** (`page_selector.py`)

**Purpose**: Identify which pages are most likely to contain trigger information.

**Scoring Algorithm**:
```python
score = (keyword_occurrences × 1) + (table_bonus × 2)
```

**Keywords** (from `config.py`):
- **English**: trigger, activation, threshold, forecast, probability, lead time, early action, alert, warning
- **French**: déclencheur, seuil, prévision, probabilité, activation
- **Spanish**: disparador, umbral, pronóstico, probabilidad, activación

**Process**:
1. Convert page text + tables to lowercase
2. Count occurrences of each keyword
3. Add bonus points if page contains tables
4. Store score in `PageData.relevance_score`

**Rationale**: Trigger statements often appear with specific terminology and in tabular format.

---

### **Step 3: Page Selection** (`page_selector.py`)

**Purpose**: Reduce token count by selecting only relevant pages.

**Strategy**: **Coarse-to-Fine Filtering**

1. **Filter**: Keep only pages with score > 0
2. **Sort**: Order by relevance score (descending)
3. **Select**: Take top **3 pages** (configurable: `TOP_PAGES_TO_SELECT`)
4. **Expand**: Include **neighbor pages** (N-1, N+1) for context

**Example**:
- Top scoring pages: [5, 12, 8]
- With neighbors: [4, 5, 6, 7, 8, 9, 11, 12, 13]
- Final selection: ~5-9 pages instead of 20-30 total pages

**Benefit**: Reduces token count by ~70%, lowering API costs.

---

### **Step 4: Payload Construction** (`payload_builder.py`)

**Purpose**: Format selected pages for LLM consumption.

**Format**:
```
--- PAGE 5 ---

[TABLES]
| Column 1 | Column 2 |
|----------|----------|
| Value 1  | Value 2  |

[TEXT]
The EAP will be activated when...
```

**Features**:
- Clear page separators
- Tables in Markdown (compact)
- Text sections labeled
- No redundant formatting

---

### **Step 5: LLM Extraction** (`llm_extractor.py` / `openai_llm_extractor.py`)

**Purpose**: Extract structured trigger data using AI.

**Two Implementations**:
1. **Gemini** (default): Uses Google's Gemini API
2. **Azure OpenAI**: Uses GPT-3.5 Turbo (alternative)

#### **System Prompt Strategy**

The LLM receives detailed instructions to:

1. **Detect Language**: Recognize English, French, Spanish documents
2. **Extract Comprehensively**: Find ALL trigger-related content
3. **Avoid Duplicates**: Consolidate bullet points into single triggers
4. **Handle Multi-Stage Triggers**: Combine Monitoring/Pre-Activation/Activation into one entry
5. **Translate**: Provide English translations for non-English triggers

#### **Extraction Schema**

```json
{
  "document_language": "English",
  "trigger_mechanism": {
    "description": "Dual-trigger mechanism...",
    "activation_type": "dual-trigger",
    "has_stop_mechanism": true,
    "stop_mechanism_description": "..."
  },
  "triggers": {
    "trigger_statement_1": {
      "statement": "Original trigger text...",
      "statement_english": "English translation...",
      "thresholds": ["threshold 1", "threshold 2"],
      "source_authority": "CENAOS, NOAA",
      "lead_time": "3 to 5 days",
      "geographic_scope": "Honduras",
      "is_conditional": false,
      "condition_dependency": null,
      "preliminary_actions": "Readiness activities...",
      "page_ref": 4
    }
  }
}
```

**Key Fields**:
- `thresholds`: Array of ALL conditions (e.g., probability, wind speed, lead time)
- `is_conditional`: Whether trigger depends on another
- `preliminary_actions`: Actions taken at this trigger stage

---

### **Step 6: Output Generation** (`json_to_excel.py`)

**Purpose**: Convert nested JSON to flat Excel format.

**Transformation**:
- **Input**: Hierarchical JSON (document → triggers)
- **Output**: Flat Excel rows (one row per trigger)

**Excel Columns**:
1. `document_name` - EAP document name
2. `trigger_number` - Sequential ID (1, 2, 3...)
3. `statement` - Full trigger text (original language)
4. `statement_english` - English translation
5. `thresholds` - Pipe-separated values (e.g., "60% probability | 34 knots")
6. `lead_time` - Forecast timeframe
7. `source_authority` - Monitoring organization
8. `geographic_scope` - Affected areas
9. `is_conditional` - TRUE/FALSE
10. `condition_dependency` - Dependency description
11. `preliminary_actions` - Actions to implement

**Output Files**:
- `extracted_triggers.json` - Full hierarchical data (Gemini)
- `extracted_triggers_openai.json` - Full hierarchical data (OpenAI)
- `triggers_gemini.xlsx` - Flattened Excel (Gemini)
- `triggers_openai.xlsx` - Flattened Excel (OpenAI)

---

## Supporting Scripts

### **Document Fetching** (`fetch_appeal_documents.py`)
- Connects to **IFRC GO API** (`https://goadmin-stage.ifrc.org/api/v2/appeal_document/`)
- Filters documents where `type == "DREF/EAP Summary"`
- Downloads PDFs to `downloaded_documents/` folder
- Creates `appeal_documents.xlsx` with metadata

### **Batch Processing** (`batch_runner.py`)
- Implements **Google Batch API** for 50% cost discount
- Processes PDFs in batches of 5
- Creates JSONL payload file
- Submits batch job and polls for completion
- Retrieves results and merges into main JSON

### **Configuration** (`config.py`)
Centralized settings:
- API keys (Gemini, OpenAI, IFRC)
- Scoring weights (keyword score, table bonus)
- Page selection count (top 3 pages)
- LLM parameters (temperature: 0.1, max tokens: 20000)

---

## Cost Optimization Strategies

1. **Page Selection**: Only send 5-9 pages instead of full PDF (70% reduction)
2. **Table Formatting**: Convert to Markdown (more compact than HTML)
3. **Low Temperature**: Set to 0.1 for consistent, deterministic output
4. **Batch API**: Use Google Batch mode for 50% discount
5. **Dual Models**: Compare Gemini vs OpenAI for quality/cost tradeoff

---

## Multilingual Support

The system handles **3 languages**:

| Language | Keywords |
|----------|----------|
| **English** | trigger, activation, threshold, forecast, probability |
| **French** | déclencheur, seuil, prévision, probabilité, activation |
| **Spanish** | disparador, umbral, pronóstico, probabilidad, activación |

**Translation Flow**:
1. Extract trigger in **original language**
2. LLM provides **English translation** in `statement_english` field
3. All metadata fields (thresholds, source, etc.) extracted in **English**

---

## Quality Control Mechanisms

### **1. Consolidation Rules**
- **Multi-stage triggers** (Monitoring → Pre-Activation → Activation) merged into ONE entry
- **Bullet points** consolidated into single trigger with multiple thresholds
- **Duplicate detection**: Each trigger statement appears only once

### **2. Validation**
- Check for required fields (statement, thresholds, source)
- Verify JSON structure matches schema
- Flag documents with no triggers found

### **3. Comparison**
- Run both Gemini and OpenAI on same documents
- Compare outputs in separate Excel files
- Manual review of discrepancies

---

## Typical Workflow

```bash
# 1. Fetch documents from IFRC API
uv run python -m scripts.fetch_appeal_documents

# 2. Process all PDFs with Gemini (default)
uv run python -m scripts.main

# OR process with Azure OpenAI
uv run python -m scripts.openai_main

# 3. Convert JSON to Excel
uv run python -m scripts.json_to_excel

# 4. (Optional) Use Batch API for cost savings
uv run python -m scripts.batch_runner
```

---

## Output Statistics

Based on current extraction results:
- **Total Documents**: 65 EAP/sEAP PDFs
- **Total Triggers Extracted**: 
  - Gemini: 100 trigger statements
  - OpenAI: 91 trigger statements
- **Average Pages per PDF**: 14-25 pages
- **Pages Analyzed per PDF**: 5-9 (after selection)
- **Token Savings**: ~70% reduction vs full PDF

---

## Key Design Decisions

### **Why Hybrid Approach?**
- **Pure keyword search**: Would miss context and complex relationships
- **Pure LLM**: Too expensive for full PDFs (20-30 pages × 65 documents)
- **Hybrid**: Best of both - fast filtering + intelligent extraction

### **Why 3 Top Pages?**
- Empirical testing showed most triggers appear on 2-4 pages
- Neighbor pages provide necessary context
- Balance between completeness and cost

### **Why Markdown Tables?**
- 40-50% fewer tokens than HTML
- Preserves structure better than plain text
- LLMs understand Markdown format well

---

## Future Enhancements

1. **Confidence Scoring**: LLM rates confidence in each extraction
2. **Cross-Validation**: Auto-compare Gemini vs OpenAI outputs
3. **Human-in-the-Loop**: Flag low-confidence extractions for review
4. **Table Detection**: Improve table extraction accuracy
5. **Multi-PDF Context**: Link related documents (e.g., EAP + Amendment)

---

## Troubleshooting

### **Common Issues**

**Issue**: No triggers found in document
- **Cause**: Document may not contain trigger information
- **Solution**: Check `pages_selected` count, adjust keywords

**Issue**: Duplicate triggers in output
- **Cause**: LLM created separate entries for bullet points
- **Solution**: Refine system prompt consolidation rules

**Issue**: Missing thresholds
- **Cause**: Thresholds in tables not extracted properly
- **Solution**: Verify table extraction, check Markdown formatting

**Issue**: API rate limits
- **Cause**: Too many requests per minute
- **Solution**: Increase `API_DELAY_SECONDS` in config

---

## Contact & Support

For questions about the extraction mechanism:
1. Check `scripts/config.py` for settings
2. Review system prompts in `llm_extractor.py`
3. Examine output JSON for extraction examples
