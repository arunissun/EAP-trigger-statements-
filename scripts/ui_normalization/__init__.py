"""
UI Normalization Pipeline for EAP Trigger Statements.

Converts raw extracted JSON into a normalized UI schema with
cascading dropdown options for the EAP MVP frontend.

Main pipeline (run in this order via run_pipeline.py):
  step01_explode.py              – Deep-parse & explode JSON into flat records
  step02_normalize.py            – LLM field extraction per threshold (Azure OpenAI / Gemini)
  step03_prep_taxonomy_input.py  – Flatten step02 output for taxonomy clustering
  step04_propose_taxonomy.py     – LLM taxonomy proposal & quality gate
  step05_aggregate_schema.py     – Aggregate unique UI dropdown values + trigger logic (standard)
  step05b_aggregate_filtered.py  – Same with frequency-based variable filtering

Combination matrix sub-pipeline (run after step02, via --phase0-matrix-* flags):
  matrix01_draft_generator.py    – Generate combination_matrix_draft.csv from corpus
  matrix02_llm_enrich.py         – LLM enrichment of draft → combination_matrix_enriched.csv

Utilities (standalone, not part of pipeline):
  util_analyze_frequency.py      – Variable frequency analysis tool
  util_export_excel.py           – Export filtered schema to Excel

Orchestrator & config:
  run_pipeline.py                – End-to-end orchestrator (all CLI flags)
  config.py                      – Paths, model names, pipeline constants
"""
