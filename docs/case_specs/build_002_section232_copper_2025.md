# Build 002 Spec — 2025 Copper Section 232

## Decision

Promote `build_002` from `queued` to `in_research`.

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

## What the legal action actually covers

The proclamation does **not** simply tariff all copper at one clean stage.

It targets:
- semi-finished copper products
- copper-intensive derivative products

That means a case built on a very broad upstream copper commodity basket could be misleading.

## Current recommendation

Status: `in_research`

This should become a live case **only if** the treatment basket can be narrowed to a product grouping that matches the legal scope better than a generic copper commodity series.

## Candidate treatment baskets

### Candidate A
- `series_label`: `Copper and brass mill shapes`
- `series_id`: `WPU102502`
- `stage`: `upstream`
- `pros`: existing known series; already works in your pipeline; directly copper-related
- `cons`: may still be too broad relative to the legal scope and may include price movement from general copper commodity dynamics rather than tariff-specific pass-through

### Candidate B
- `series_label`: `Nonferrous wire and cable`
- `series_id`: `TO CONFIRM`
- `stage`: `downstream`
- `pros`: closer to some of the semi-finished / derivative copper scope
- `cons`: series ID still needs confirmation; may still be heterogeneous

## Candidate controls

### Control A
- `series_label`: `Aluminum mill shapes`
- `series_id`: `WPU102501`
- `reason`: nearby nonferrous mill-shapes market without being the directly tariffed copper basket

### Control B
- `series_label`: `Copper base scrap`
- `series_id`: `TO CONFIRM`
- `reason`: possible cross-check series, but likely too contaminated by commodity-market dynamics to be the main control

## Window design candidate

Use this only if a treatment-control pair is approved:

- `event_date`: `2025-08-01`
- `base_date`: `2025-07-31`
- `window_start`: `2024-08-31`
- `window_end`: `2026-01-31`

## Current research question

Which public BLS basket most closely matches the tariffed semi-finished / derivative copper scope **without** collapsing into a generic world-copper price story?

## Promotion rule to spec_ready

Do not move this to `spec_ready` until all of the following are confirmed:

1. final treatment series label
2. exact treatment series ID
3. final control series label
4. exact control series ID
5. short justification for why treatment better matches the legal scope than a broad copper commodity basket
6. short caveat explaining residual contamination risk

## Recommended next concrete research task

Check whether `Copper and brass mill shapes` or `Nonferrous wire and cable` is the better empirical treatment basket for the August 2025 copper 232 action, then identify one primary control and one backup control.