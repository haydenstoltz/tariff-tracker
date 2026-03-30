# Build 001 Spec — 2025 Steel Section 232 Raise to 50%

## Decision

Promote `build_001` from `queued` to `spec_ready`.

This is a valid upstream producer case candidate.

## Event

- `event_id`: `section232_steel_aluminum_raise50_2025`
- `event_title`: `2025 Steel and Aluminum Section 232 Raise to 50%`
- `authority`: `Section 232`
- `effective_date`: `2025-06-04`
- `event_month`: `2025-06`
- `legal_source_label`: `White House Proclamation — Adjusting Imports of Aluminum and Steel into the United States`
- `legal_source_url`: `https://www.whitehouse.gov/presidential-actions/2025/06/adjusting-imports-of-aluminum-and-steel-into-the-united-states/`

## Case recommendation

### Proposed case identity

- `case_id`: `steel_2025_raise50_case_main`
- `case_name`: `2025 Steel Section 232 Raise to 50%`
- `source_type`: `UPSTREAM`
- `case_stage`: `upstream`
- `estimate_kind`: `relative_pass_through`

### Treatment and control

#### Treatment
- `series_id`: `WPU1017`
- `series_label`: `Steel mill products`

#### Control
- `series_id`: `WPU102502`
- `series_label`: `Copper and brass mill shapes`

## Window design

- `event_date`: `2025-06-04`
- `base_date`: `2025-05-31`
- `window_start`: `2024-06-30`
- `window_end`: `2026-01-31`

## Why this design

This reuses the strongest part of the historical steel case architecture: a clean upstream producer treatment basket with a nearby metals control that is exposed to broad industrial conditions but not directly targeted by the steel tariff itself.

The case is not perfect. It sits on top of the preexisting Section 232 steel regime, so the empirical question is not “what did Section 232 do to steel from zero,” but rather “what happened when the steel tariff rate was raised from 25 percent to 50 percent in June 2025.”

That is still a legitimate event study, but it should be framed as an incremental tariff shock, not an initial-policy shock.

## Confidence recommendation

- `confidence_tier`: `medium`

## Short rationale

Reuse the existing upstream steel architecture for a live tariff-rate increase with a confirmed effective date and a defensible producer-price treatment basket.

## Core caveat

The 2025 event is a rate increase layered onto an already-existing Section 232 regime, and the United Kingdom remained at 25 percent for a time, so the shock is not as clean as the original 2018 steel implementation.

## Robustness note

Primary robustness check should compare:
1. the existing copper/brass control
2. one alternate nonferrous metals control
3. a shorter post window that isolates June 2025 through January 2026

## Method note

Treatment and control should be rebased to 100 at the base month. Relative effect is treatment minus control. Report 3m, 6m, and first-available post-window effect even if a full 12m window is not yet available.

## Required metadata rows to implement next

### site_cases.csv row
- `case_id`: `steel_2025_raise50_case_main`
- `case_name`: `2025 Steel Section 232 Raise to 50%`
- `source_type`: `UPSTREAM`
- `treatment_label`: `Steel mill products`
- `control_label`: `Copper and brass mill shapes`
- `confidence_tier`: `medium`
- `rationale_short`: `Reuse the historical steel upstream design for the June 2025 rate increase from 25 to 50 percent.`
- `caveat`: `Rate increase sits on top of the existing Section 232 steel regime and the UK temporarily remained at 25 percent.`
- `robustness_note`: `Test the existing copper/brass control against one alternate nonferrous control and a shorter post window.`
- `method_note`: `Treatment and control are rebased to 100 at the base month. Relative effect equals treatment minus control.`
- `site_status`: `live`

### event_case_map.csv row
- `event_id`: `section232_steel_aluminum_raise50_2025`
- `case_id`: `steel_2025_raise50_case_main`
- `display_order`: `1`
- `primary_case_flag`: `yes`

### case_stage_map.csv row
- `case_id`: `steel_2025_raise50_case_main`
- `case_stage`: `upstream`
- `stage_order`: `2`
- `estimate_kind`: `relative_pass_through`
- `notes`: `Upstream producer-price case for the June 2025 increase from 25 percent to 50 percent on steel imports.`

### product-case input row template
- `case_name`: `2025 Steel Section 232 Raise to 50%`
- `series_id`: `WPU1017`
- `series_label`: `Steel mill products`
- `source_type`: `UPSTREAM`
- `role`: `treatment`
- `event_date`: `2025-06-04`
- `base_date`: `2025-05-31`
- `window_start`: `2024-06-30`
- `window_end`: `2026-01-31`

- `case_name`: `2025 Steel Section 232 Raise to 50%`
- `series_id`: `WPU102502`
- `series_label`: `Copper and brass mill shapes`
- `source_type`: `UPSTREAM`
- `role`: `control`
- `event_date`: `2025-06-04`
- `base_date`: `2025-05-31`
- `window_start`: `2024-06-30`
- `window_end`: `2026-01-31`