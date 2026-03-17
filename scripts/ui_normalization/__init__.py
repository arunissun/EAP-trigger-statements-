"""
UI Normalization Pipeline for EAP Trigger Statements.

Converts raw extracted JSON into a normalized UI schema with
cascading dropdown options for the EAP MVP frontend.

Steps:
  1. step1_exploder   – Deep-parse & explode JSON into flat records
  2. step2_llm        – LLM normalization via Pydantic structured output
  3. step3_aggregator – Aggregate unique UI dropdown values + trigger logic
  4. run_pipeline     – End-to-end orchestrator
"""
