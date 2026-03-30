# Build 002 Spec — 2025 Copper Section 232

## Decision

Promote `build_002` from `in_research` to `spec_ready`.

Do **not** implement as a live case yet.

## Event

- `event_id`: `section232_copper_2025`
- `event_title`: `2025 Copper Section 232`
- `authority`: `Section 232`
- `announcement_date`: `2025-07-30`
- `effective_date`: `2025-08-01`
- `event_month`: `2025-08`
- `legal_source_label`: `White House Proclamation — Adjusting Imports of Copper into the United States`
- `legal_source_url`: `https://www.whitehouse.gov/presidential-actions/2025/07/adjusting-imports-of-copper-into-the-united-states/`

## Decision on treatment basket

### Preferred treatment
- `series_label`: `Copper and brass mill shapes`
- `series_id`: `WPU102502`
- `stage`: `upstream`

### Rejected as primary treatment
- `series_label`: `Nonferrous wire and cable`
- `series_id`: `WPU1026`
- `reason_rejected`: too broad and too downstream for the first flagship copper case

## Preferred control

- `series_label`: `Aluminum mill shapes`
- `series_id`: `WPU102501`
- `stage`: `upstream`

## Window design

- `event_date`: `2025-08-01`
- `base_date`: `2025-07-31`
- `window_start`: `2024-08-31`
- `window_end`: `2026-01-31`

## Why this design

The legal action is not a generic tariff on all copper. It covers semi-finished copper products and copper-intensive derivative products. For a first-pass upstream case, `Copper and brass mill shapes` is closer to the semi-finished side of the legal scope than `Nonferrous wire and cable`, which is further downstream and bundles a broader electrical-wire product space.

`Aluminum mill shapes` is the best first control because it is a nearby nonferrous mill-shapes basket without being the directly targeted copper basket.

## Confidence recommendation

- `confidence_tier`: `medium`

## Short rationale

Use an upstream nonferrous mill-shapes design that matches the semi-finished side of the copper proclamation more closely than a broader wire-and-cable basket.

## Core caveat

The proclamation also covers copper-intensive derivative products, so an upstream mill-shapes case will not capture the full legal scope. This should be framed as a clean upstream copper case, not as a complete whole-chain estimate.

## Robustness note

Primary robustness check should compare:
1. `WPU102501` Aluminum mill shapes as the main control
2. one alternate nonferrous control if available
3. a downstream follow-on design later if a narrower derivative-product basket proves usable

## Method note

Treatment and control should be rebased to 100 at the base month. Relative effect is treatment minus control. Report 3m, 6m, and first-available post-window effect even if a full 12m window is not yet available.

## Required metadata rows to implement next

### site_cases.csv row
- `case_id`: `copper_2025_case_main`
- `case_name`: `2025 Copper Section 232`
- `source_type`: `UPSTREAM`
- `treatment_label`: `Copper and brass mill shapes`
- `control_label`: `Aluminum mill shapes`
- `confidence_tier`: `medium`
- `rationale_short`: `Use an upstream nonferrous mill-shapes design for the August 2025 copper Section 232 action.`
- `caveat`: `The legal action also covers derivative copper products, so this is an upstream case rather than a full-scope copper estimate.`
- `robustness_note`: `Test aluminum mill shapes against one alternate nonferrous control and consider a downstream follow-on design later.`
- `method_note`: `Treatment and control are rebased to 100 at the base month. Relative effect equals treatment minus control.`
- `site_status`: `live`

### event_case_map.csv row
- `event_id`: `section232_copper_2025`
- `case_id`: `copper_2025_case_main`
- `display_order`: `1`
- `primary_case_flag`: `yes`

### case_stage_map.csv row
- `case_id`: `copper_2025_case_main`
- `case_stage`: `upstream`
- `stage_order`: `2`
- `estimate_kind`: `relative_pass_through`
- `notes`: `Upstream producer-price case for the August 2025 copper Section 232 action using copper and brass mill shapes versus aluminum mill shapes.`

### product-case input row template
- `case_name`: `2025 Copper Section 232`
- `series_id`: `WPU102502`
- `series_label`: `Copper and brass mill shapes`
- `source_type`: `UPSTREAM`
- `role`: `treatment`
- `event_date`: `2025-08-01`
- `base_date`: `2025-07-31`
- `window_start`: `2024-08-31`
- `window_end`: `2026-01-31`

- `case_name`: `2025 Copper Section 232`
- `series_id`: `WPU102501`
- `series_label`: `Aluminum mill shapes`
- `source_type`: `UPSTREAM`
- `role`: `control`
- `event_date`: `2025-08-01`
- `base_date`: `2025-07-31`
- `window_start`: `2024-08-31`
- `window_end`: `2026-01-31`