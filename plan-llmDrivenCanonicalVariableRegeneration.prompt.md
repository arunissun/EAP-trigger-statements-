## Plan: LLM-Driven Canonical Variable Regeneration

Rerun the pipeline end-to-end from trigger statements, then add a dedicated LLM taxonomy pass that decides the 10 canonical forecast variables and nests subcategories/variants under each canonical parent (for example Precipitation -> Seasonal precipitation, Rainfall total, Cumulative rainfall), with human review gates before publishing dropdowns. Canonical matching should use forecast variable semantics (plus hazard context) and should not depend on timeframe values.

**Steps**
1. Phase 0 - Full rerun input prep: regenerate normalized threshold candidates from extracted trigger statements so taxonomy is built from fresh data, not legacy mappings. Export one flattened table with document_id, hazard_type, trigger_statement, forecast_variable, unit, operator, source, lead_time_days. Enforce a hard rule that lead time/duration/timeframe are metadata and never valid forecast_variable values; when temporal-only rows appear, reassign forecast_variable from sibling statement context or route row to review.
2. Phase 1 - LLM taxonomy proposal (core request): Use a Two-Stage LLM approach to avoid output token truncation. Stage A: LLM proposes the target canonical variables (around 10). Stage B: LLM maps the input variables in small batches to the proposed canonicals, returning a minified JSON schema. Python reconstructs the full objects (alias lists, examples) natively. Keep hard constraint that every variable must map to a canonical group or to Other.
3. Phase 1 - Canonical quality constraints: apply automatic checks on the LLM proposal before acceptance: coverage target, semantic distinctness between canonical groups, low overlap of alias sets, and minimum support counts. If checks fail, rerun the LLM pass with corrective feedback and compare versions.
4. Phase 1 - Human approval checkpoint: present candidate 10 canonical groups + subcategories in a review artifact (markdown/json table) and lock approved taxonomy_version. This is the governance gate before downstream normalization.
5. Phase 2 - Record-level remapping with approved taxonomy: rerun normalization so each threshold row stores canonical_variable + subcategory + original_variable. Use deterministic alias lookup first and LLM disambiguation second only when lookup fails.
6. Phase 2 - Unit/operator learning per canonical group: aggregate observed valid combinations from remapped records and build constraints at two levels: canonical-level defaults and subcategory-specific overrides. Keep lead_time_days as contextual metadata for statement generation, not as a canonical matching key.
7. Phase 3 - Dropdown packaging: output schema with three UI layers: primary_dropdown_top10, secondary_subcategory_dropdown (contextual by canonical), and advanced_other_search/custom for rare or unresolved variables.
8. Phase 3 - Cascading logic contract: emit explicit maps required by UI: canonical_to_units, canonical_to_operators, and subcategory override maps. Ensure invalid unit choices are blocked once canonical is selected. Keep lead_time_days as a user input field with hazard-aware recommendations, not strict canonical filtering.
9. Phase 4 - Trigger statement generator: implement a generator that accepts user inputs (forecast_variable, subcategory, hazard_type, unit, operator, threshold_value, lead_time_days, source, optional probability and condition logic) and outputs a draft EAP trigger statement.
10. Phase 4 - Similarity-to-reference strategy: use extracted_triggers_openai.json as a style and structure reference corpus. Retrieve top-k similar records by hazard_type + canonical_variable + activation_type, then generate with a hybrid approach (template slots + LLM polishing) to stay close to real EAP wording.
11. Phase 4 - Regression testing harness (critical): for sampled historical records, generate statements from structured fields and compare to reference statements from extracted_triggers_openai.json using slot accuracy (source, variable, unit, operator, lead time), semantic similarity, and style checklist scores. Publish pass/fail report.
12. Phase 5 - Human edit and acceptance workflow: keep generated statements editable in UI, store edits, and feed accepted edits back into prompt/template refinement.
13. Phase 6 - Continuous retraining loop: for each new batch, rerun drift analysis and optionally rerun LLM taxonomy proposal in shadow mode. Promote taxonomy changes only after approval and version bump.

**Relevant files**
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/scripts/ui_normalization/step1_exploder.py - Source for fresh trigger-threshold extraction inputs.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/scripts/ui_normalization/step2_llm.py - Add dedicated taxonomy proposal mode and record-level canonical/subcategory remapping.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/scripts/ui_normalization/step3_aggregator.py - Build canonical/subcategory dropdown payload and cascade constraint maps.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/scripts/ui_normalization/step3_aggregator_filtered.py - Enforce top-10 primary list and Advanced/Other routing.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/scripts/ui_normalization/step4_statement_generator.py - Generate EAP trigger statements from structured user selections.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/scripts/ui_normalization/regression_statement_tests.py - Run similarity and slot-accuracy regression tests against reference statements.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/scripts/ui_normalization/analyze_variable_frequency.py - Coverage checks and drift metrics for taxonomy quality.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/scripts/ui_normalization/run_pipeline.py - Orchestrate end-to-end rerun plus taxonomy proposal/approval switches.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/FORECAST_VARIABLES_REFERENCE.json - Store approved canonical taxonomy_version, subcategories, aliases, and UI constraints.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/extracted_triggers/extracted_triggers_openai.json - Reference corpus for style-grounded statement generation and regression comparison.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/ui_normalization_output/variable_frequency_openai.md - Frequency baseline input for LLM prompt context and validation.
- c:/Users/arun.gandhi/Downloads/EAP-trigger-statements-/ui_normalization_output/ui_schema_openai_filtered.json - Target schema contract for form dropdowns and cascading logic.

**Verification**
1. Confirm LLM taxonomy output contains exactly 10 canonical variables and each forecast variable is mapped.
2. Confirm at least 90% coverage of records within the top-10 canonical groups; unresolved variables must route to Advanced/Other.
3. Validate subcategory fidelity on a stratified sample across flood, drought, heatwave, and cholera records.
4. Validate canonical matching independence from timeframe (changing lead_time_days should not remap canonical_variable/subcategory).
5. Validate cascading behavior: selecting a canonical variable filters units/operators correctly, including subcategory overrides.
6. Confirm dropdown contract has three layers (top10, subcategory, Advanced/Other) and no rare item leaks into primary list.
7. Regression test generator quality against extracted_triggers_openai.json with thresholds for slot accuracy and semantic similarity.
8. Verify generated statements are editable in UI and accepted edits are captured for retraining.

**Decisions**
- Included scope: full rerun, LLM-decided top-10 canonical variables, subcategory generation, cascading dropdown output contracts, and regression-tested statement generation.
- Excluded scope: redesign of non-dropdown UI components unrelated to variable taxonomy workflow.
- Locked intent: LLM should propose canonical groups and subcategories from fresh trigger-statement extraction, then human approval locks taxonomy version.
- Locked rule: timeframe (lead_time_days) is contextual for generated text and QA, but not used as a canonical matching feature.
- Locked rule: lead_time/duration/forecast horizon are not forecast variables and must not appear as canonical candidates.

**Further Considerations**
1. Recommendation: run LLM taxonomy with two temperatures (strict and creative) and only keep canonical groups that are stable across both outputs.
2. Recommendation: keep an immutable mapping audit table with forecast_variable -> canonical -> subcategory -> confidence for explainability.
3. Recommendation: maintain backward compatibility by retaining legacy variable labels as aliases during initial rollout.
4. Recommendation: prefer hybrid generation (slot template + retrieved examples + LLM rewrite) instead of pure free-form generation to stay close to extracted_triggers_openai.json style.