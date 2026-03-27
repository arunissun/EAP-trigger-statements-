# UI/UX Guide: EAP Trigger Form

## Executive Summary & Goal
The primary goal of the new "Trigger Rule Builder" for the IFRC GO platform is to transition Early Action Protocol (EAP) triggers from unstructured, free-text paragraphs into a structured, machine-readable format. 

Currently, trigger statements are written as complex narrative sentences, making it difficult to automate monitoring, analyze historical data, or integrate with forecasting APIs. By moving to a structured, conditional logic builder, we enable automated alerts, scalable data analysis, and clearer, less ambiguous activation protocols for National Societies.

## Critique of the Current Mockup
The preliminary mockup provides a solid foundation, but requires structural adjustments to support the full complexity of EAP logic.

**What Works:**
*   **Clean Card Layout:** The visual grouping of individual conditions (Forecast Variable, Probability, Lead Time, Source) into distinct cards is excellent and reduces cognitive load.
*   **Auto-generated Statement:** The real-time, plain-text preview at the bottom is a brilliant UX touch that builds user confidence and ensures their structured inputs match their intent.

**What Needs Improvement:**
*   **Lack of Phase Context:** The mockup simply lists "Trigger 1" and "Trigger 2". EAPs can be multi-stage and require distinct phases. The UI needs a higher-level container to distinguish between *Pre-activation*, *Activation*, and *Stop mechanisms*.
*   **Ambiguous Nested Logic:** The "Trigger Logic: Conditional (Either / or)" dropdown is floating between cards. It is visually unclear if this applies globally or just between Trigger 1 and 2. We need clearer visual grouping (e.g., indented blocks, vertical connecting lines, or visual brackets) to represent nested `AND`/`OR` logic.
*   **Unstructured Threshold Field:** The "Threshold Value" field currently shows "400 mm in 5 days" as a single text input. This defeats the purpose of structured data. It must be broken down into distinct fields: `Operator` (>, <, =), `Value` (400), and `Unit` (mm). The "in 5 days" part is already handled by the Lead Time/Timeframe fields, so combining them in the threshold field is redundant.

## The EAP Mental Model (Information Architecture)
To support complex, multi-stage EAPs, the UI must strictly adhere to the following three-level hierarchy:

### Level 1: The Phase
The highest level of organization. Users must be able to define rules for different stages of the disaster lifecycle.
*   **Pre-activation (Readiness):** Early warning conditions that start low-regret, preparatory actions (e.g., pre-positioning, coordination, beneficiary verification, alert messaging). This phase is usually forecast-led and earlier in lead time.
*   **Activation (Implementation):** Definitive trigger conditions that authorize funded early actions (cash, evacuation support, distribution, WASH/health actions, etc.).
*   **Stop / Deactivation / Scale-down:** Rules that halt or taper actions when updated evidence indicates lower risk, changed hazard trajectory, or unmet confirmation conditions.

### Phase Semantics for the UI (Critical)
The UI should treat these phases as separate logic containers, not just labels.

*   **Pre-activation is not full activation.** It is a readiness gate.
*   **Activation can be conditional or independent, depending on the EAP.** Some EAPs require Trigger 1 before Trigger 2; others allow Trigger 2 direct activation.
*   **Stop logic can apply at different moments:**
	 *   before early actions start (cancel planned activities)
	 *   during implementation (halt specific components)
	 *   as phased scale-down (health/outbreak contexts)
*   **Stop logic can be reversible in some EAPs.** If forecasts worsen again, actions may resume.

To support this operational reality, each Stop rule block in the form should capture:
*   **Scope:** all actions vs selected actions (e.g., cash only)
*   **Timing window:** pre-start only vs in-activation allowed
*   **Decision authority:** who formally signals stop (agency/role)
*   **Reversibility:** can the trigger be reactivated if conditions worsen again?

### Level 2: The Trigger Statement
The overarching rule within a Phase. A Phase can have multiple Trigger Statements. If *any* Trigger Statement is met, the Phase is activated.

### Level 3: The Conditions/Thresholds
The individual metrics (e.g., Rainfall > 400mm) that make up a Trigger Statement. These are combined using strict `AND` / `OR` logic.

### Data-Backed Examples from `extracted_triggers_openai.json`
From the current extracted dataset (65 records), pre-activation signals are explicitly present in 16 records, and non-null stop/deactivation descriptions appear in 34 records.

**A) What "Pre-activation" means in real EAPs**

1. **Mauritania - Flood sEAP (MDRMR020)**
	Uses a **pre-season first trigger** (seasonal probabilistic forecast) for readiness actions such as pre-positioning and training, then a short-term 5-day trigger for implementation.

2. **Pakistan - Riverine Floods EAP (MDRPK029)**
	Uses **section-specific pre-activation triggers** (mostly GLoFAS probability/return-period signals, 3-10 day lead times) before activation based on observed outflows and quantitative forecasts.

3. **Indonesia - Flood EAP (MDRID027)**
	Has a **pre-activation rainfall probability stage** (e.g., >50 mm/6h with >60% probability) before higher-threshold activation.

4. **Myanmar - Heatwave sEAP (MDRMM022)**
	Uses a **10-day pre-activation signal** to start preparatory activities, then confirmation trigger for implementation.

5. **Zambia - Drought EAP2024ZM02 (MDRZM025)**
	Includes **Trigger 0 (pre-trigger)** from ENSO outlook to initiate pre-actions before later activation triggers.

**B) What "Stop / Deactivation" means in real EAPs**

1. **Bangladesh - Cold Wave EAP (MDRBD038)**
	If a later forecast lowers event likelihood before action start, activation is stopped; in-kind items are stored and cash is halted.

2. **Kyrgyzstan - Cold Wave EAP (MDRKG020)**
	If 72-hour cold-wave probability drops below 75%, actions are stopped; if forecasts confirm again, actions can resume.

3. **Cameroon - Cholera EAP (MDRCM040)**
	Deactivation/phase-out is tied to epidemiological confirmation windows (no confirmed cases -> phase-out/discontinue).

4. **Somalia - Drought EAP (MDRSO019)**
	Stop mechanism is activated when updated SPI-12 and FEWSNET projections show improved food-security outlook.

5. **Malawi - Pluvial Floods sEAP (MDRMW023)**
	Daily forecast reduction in anticipated rainfall suspends early actions.

**C) Important design nuance: some EAPs have pre-activation but no explicit stop**

*   **Mauritania (MDRMR020)** and **Indonesia (MDRID027)** include pre-activation structure but do not define an explicit stop/deactivation rule.
*   The UI must therefore allow **Stop Mechanism = Not defined** rather than forcing a stop rule.

## Master Dropdown Taxonomies (The 80/20 Rule)
To prevent cognitive overload, dropdown menus must prioritize the most common options based on our global frequency analysis, rather than presenting a massive alphabetical list.

**1. Forecast Variables (Top 80%)**
Based on 258 historical records, just 12 variables account for over 80% of all triggers. These should be immediately visible:
1. Seasonal precipitation
2. River discharge
3. Rainfall total
4. Affected population
5. Wind speed
6. Inundation extent
7. Temperature
8. Lead time
9. Daily maximum temperature
10. Maximum temperature
11. Excess mortality
12. Confirmed Cases

**2. Sources**
The UI should prioritize the most frequently used regional and global forecasting centers from our dataset (e.g., ECMWF, GLoFAS, PAGASA, IDEAM, INAM). 

**3. Threshold Units**
Prioritize the most common units based on the variable type:
*   `mm`, `m`, `cm` (Precipitation/Water levels)
*   `km/h`, `knots` (Wind)
*   `°C`, `K` (Temperature)
*   `%`, `percentile` (Probabilities/Anomalies)
*   `people`, `cases` (Population/Health)
*   `alert level`, `signal level`, `phase` (Categorical warnings)

**4. Timeframes & Operators**
*   **Timeframes:** `hours`, `days`, `weeks`, `months`
*   **Operators:** `>`, `>=`, `<`, `<=`, `=`, `between`

**Crucial Instruction: Handling the "Long Tail"**
There are 42 "rare" forecast variables (e.g., *Fodder deficit*, *Ignition probability*) that appear only once globally. 
*   **Do not clutter the main dropdown.** 
*   Implement an **"Advanced / Other..."** option at the very bottom of the primary dropdown. 
*   When clicked, this should open a searchable combobox or a secondary modal allowing the user to search the rare variables or input a custom one.

## Dynamic UI/UX Rules & Progressive Disclosure

### Progressive Disclosure
Do not overwhelm the user with complex logic gates immediately.
1.  When a user adds a new Trigger Statement, show exactly **one** Condition card.
2.  Provide a clear **"+ Add Condition"** button below it.
3.  **Only when** a second condition is added should the UI reveal the `AND` / `OR` logic toggle between the two cards.

### Cascading Logic (Smart Filtering)
Dropdowns must be context-aware to prevent invalid data entry. The selection in the "Forecast Variable" dropdown must dynamically filter the options in the "Threshold Unit" dropdown.
*   *If* Variable = `Rainfall total` -> *Then* Unit dropdown only shows `mm`, `cm`, `inches`.
*   *If* Variable = `Wind speed` -> *Then* Unit dropdown only shows `km/h`, `knots`, `m/s`.
*   *If* Variable = `Confirmed cholera cases` -> *Then* Unit dropdown only shows `cases`, `people`.
*   *If* Variable = `Temperature` -> *Then* Unit dropdown only shows `°C`, `K`.

## The Auto-Generator (UX Feedback)
The auto-generated trigger statement at the bottom of the mockup is critical for UX. It translates the structured database fields back into human-readable text, serving as a confirmation step.

The UI should dynamically stitch the selections together using a standardized template:

> "When **[Source]** forecasts **[Forecast Variable]** **[Operator]** **[Threshold Value]** **[Threshold Unit]** within a lead time of **[Lead Time]** **[Timeframe]**, with a probability of **[Probability]**..."

**Example Dynamic Output:**
*User selects:* Source: PAGASA | Variable: Rainfall total | Operator: >= | Value: 400 | Unit: mm | Lead Time: 5 | Timeframe: days | Probability: 60%
*Auto-generated text:* "When **PAGASA** forecasts **Rainfall total** **>= 400 mm** within a lead time of **5 days**, with a probability of **60%**..."

If multiple conditions are linked, the generator must insert bolded **AND** / **OR** conjunctions to clearly reflect the nested logic.
