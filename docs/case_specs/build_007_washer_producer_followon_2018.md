# Build 007 Spec — 2018 Washer Safeguard Producer Follow-on

## Decision

Promote `build_007` from `in_research` to `spec_ready`.

This is a **provisional** spec, not a high-confidence one.

## Why this case matters

This is the cleanest demonstration of the project’s intended architecture:

- one legal tariff event
- multiple empirical cases
- different supply-chain stages
- different effect sizes

The washer safeguard already has a strong live consumer case.

This follow-on case adds an upstream producer view for the same legal shock.

## Event

- `event_id`: `section201_washers_2018`
- `event_title`: `2018 Washer Safeguard Tariffs`
- `authority`: `Section 201`
- `effective_date`: `2018-02-07`
- `event_month`: `2018-02`

## Proposed case identity

- `case_id`: `washers_producer_case_main`
- `case_name`: `2018 Washer Safeguard Producer Case`
- `source_type`: `UPSTREAM`
- `case_stage`: `upstream`
- `estimate_kind`: `relative_pass_through`

## Treatment and control

### Treatment
- `series_id`: `PCU335220335220`
- `series_label`: `Major household appliance manufacturing`

### Provisional control
- `series_id`: `PCU335210335210`
- `series_label`: `Small electrical appliance manufacturing`

## Window design

- `event_date`: `2018-02-07`
- `base_date`: `2018-01-31`
- `window_start`: `2016-01-31`
- `window_end`: `2019-09-30`

## Why this design

The treatment series is already known and already used in the project as the washer producer-side proxy.

The control sweep did not recover the previously hoped-for nearby major-appliance subcategory controls from the BLS pull. The only usable returned candidate was `Small electrical appliance manufacturing`.

That control is not ideal, but it does reproduce the same basic qualitative shape as the earlier producer-side note:
- positive at 3 months
- positive at 6 months
- positive at 12 months
- much smaller than the live consumer washer case

## Comparison to the target producer pattern

Historical target pattern:
- 3m: `+0.57`
- 6m: `+1.16`
- 12m: `+1.86`

Sweep result with provisional control:
- 3m: `+1.259`
- 6m: `+2.339`
- 12m: `+2.215`

This is directionally consistent, but it overshoots the earlier target pattern enough that confidence should remain low.

## Confidence recommendation

- `confidence_tier`: `low`

## Short rationale

Add an upstream producer follow-on case for the same washer tariff event so the tracker shows that one legal event can produce very different incidence profiles by stage.

## Core caveat

The control is provisional and is not as close an industry match as originally hoped. This case should be presented as a lower-confidence upstream follow-on, not as a flagship estimate.

## Robustness note

Primary robustness checks should be:
1. rerun with any recovered nearby major-appliance subcategory control if a better BLS ID is found later
2. compare the provisional producer result against the historical target pattern
3. keep the confidence tier below the live consumer washer case

## Method note

Treatment and control should be rebased to 100 at the base month. Relative effect is treatment minus control. Report 3m, 6m, and 12m relative effects.

## Required metadata rows to implement next

### site_cases.csv row
- `case_id`: `washers_producer_case_main`
- `case_name`: `2018 Washer Safeguard Producer Case`
- `source_type`: `UPSTREAM`
- `treatment_label`: `Major household appliance manufacturing`
- `control_label`: `Small electrical appliance manufacturing`
- `confidence_tier`: `low`
- `rationale_short`: `Add an upstream producer follow-on case for the 2018 washer safeguard using appliance manufacturing treatment and a provisional nearby appliance control.`
- `caveat`: `Control is provisional and not as close an industry match as originally hoped.`
- `robustness_note`: `Replace the provisional control if a better major-appliance subcategory series can be recovered later.`
- `method_note`: `Treatment and control are rebased to 100 at the base month. Relative effect equals treatment minus control.`
- `site_status`: `live`

### event_case_map.csv row
- `event_id`: `section201_washers_2018`
- `case_id`: `washers_producer_case_main`
- `display_order`: `2`
- `primary_case_flag`: `no`

### case_stage_map.csv row
- `case_id`: `washers_producer_case_main`
- `case_stage`: `upstream`
- `stage_order`: `2`
- `estimate_kind`: `relative_pass_through`
- `notes`: `Provisional upstream producer follow-on case for the 2018 washer safeguard using appliance manufacturing treatment and a lower-confidence nearby appliance control.`

### product-case input row template
- `case_name`: `2018 Washer Safeguard Producer Case`
- `series_id`: `PCU335220335220`
- `series_label`: `Major household appliance manufacturing`
- `source_type`: `UPSTREAM`
- `role`: `treatment`
- `event_date`: `2018-02-07`
- `base_date`: `2018-01-31`
- `window_start`: `2016-01-31`
- `window_end`: `2019-09-30`

- `case_name`: `2018 Washer Safeguard Producer Case`
- `series_id`: `PCU335210335210`
- `series_label`: `Small electrical appliance manufacturing`
- `source_type`: `UPSTREAM`
- `role`: `control`
- `event_date`: `2018-02-07`
- `base_date`: `2018-01-31`
- `window_start`: `2016-01-31`
- `window_end`: `2019-09-30`