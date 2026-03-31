# Build 003 Spec — 2024 Solar Bifacial Exclusion Revocation

## Decision

Promote `build_003` from `queued` to `in_research`.

Do not implement live yet.

## Event

- `event_id`: `section201_solar_bifacial_revocation_2024`
- `event_title`: `2024 Solar Bifacial Exclusion Revocation`
- `authority`: `Section 201`
- `announcement_date`: `2024-06-21`
- `effective_date`: `2024-06-26`
- `event_month`: `2024-06`
- `legal_source_label`: `CBP CSMS #61152419 — Guidance: Section 201 Removal of Bifacial Exclusion`
- `legal_source_url`: `https://content.govdelivery.com/accounts/USDHSCBP/bulletins/3a51ca3`

## What changed

President Biden issued Proclamation 10779 on June 21, 2024 revoking the bifacial-panel exclusion from the solar safeguard, and CBP guidance states the change became effective June 26, 2024 at 12:01 a.m. EDT.

A temporary contract-based carveout remained available through September 23, 2024 for qualifying pre-existing contracts.

## Why this is a good candidate

This is a narrower and cleaner solar event than the full safeguard extension.

It is better suited to the project than a generic “solar tariffs” chart because:
- it is a specific legal change
- it has a clear effective date
- it plausibly maps to a narrower product scope than the full solar regime

## Current recommendation

Status: `in_research`

This should become a live case only if you can identify a public treatment basket that maps to bifacial or closely related CSPV module/product categories better than a generic broad solar or electrical-equipment basket.

## Candidate treatment directions

### Candidate A
- `stage`: `upstream`
- `concept`: solar module / photovoltaic equipment producer basket
- `status`: series still needs confirmation

### Candidate B
- `stage`: `import`
- `concept`: import-price basket if a sufficiently narrow solar-related public series exists
- `status`: series still needs confirmation

### Candidate C
- `stage`: `downstream`
- `concept`: electrical generation equipment basket with strong solar exposure
- `status`: likely too broad unless narrowed

## Candidate control directions

### Control A
- nearby non-solar electrical equipment basket
- reason: similar industrial conditions without direct bifacial-solar exposure

### Control B
- broader renewable or power-equipment basket excluding the treatment category
- reason: cross-check only, likely not main control

## Window design candidate

Use this only if a treatment-control pair is approved:

- `event_date`: `2024-06-26`
- `base_date`: `2024-05-31`
- `window_start`: `2023-06-30`
- `window_end`: `2026-01-31`

## Main research question

Is there a public BLS basket narrow enough to capture the bifacial-exclusion revocation without collapsing into a generic broad solar or electrical-equipment story?

## Promotion rule to spec_ready

Do not move this to `spec_ready` until all of the following are confirmed:

1. exact treatment series label
2. exact treatment series ID
3. exact control series label
4. exact control series ID
5. short justification for why the treatment basket matches the bifacial revocation better than the broader solar safeguard
6. short caveat explaining transition-period contamination risk from the contract carveout through September 23, 2024

## Recommended next concrete task

Search BLS producer and import-price series for the narrowest plausible photovoltaic / solar-equipment basket, then identify one primary control and one backup control.