# Build 007 Spec — 2018 Washer Safeguard Producer Follow-on

## Decision

Add this as a case candidate and mark it `in_research`.

Do not implement it live yet.

## Why this is the right next case

This is the cleanest way to prove the project’s architecture is correct:

- one legal event
- multiple empirical cases
- different incidence stages
- different effect sizes

The washer safeguard already has a strong consumer case live.

The producer-side result is known to be positive but much smaller:

- `+0.57` percentage points at 3 months
- `+1.16` percentage points at 6 months
- `+1.86` percentage points at 12 months

That makes this a very useful follow-on case. It shows that the same legal shock can look very different depending on where in the chain you measure it.

## Event

- `event_id`: `section201_washers_2018`
- `event_title`: `2018 Washer Safeguard Tariffs`
- `authority`: `Section 201`
- `effective_date`: `2018-02-07`
- `event_month`: `2018-02`

## Candidate case identity

- `candidate_case_id`: `washers_producer_case_main`
- `candidate_case_name`: `2018 Washer Safeguard Producer Case`
- `planned_stage`: `upstream`
- `source_type`: `PPI`

## Treatment basket

### Preferred treatment
- `series_id`: `PCU335220335220`
- `series_label`: `Major household appliance manufacturing`

## What is still missing

The treatment series is known.

The missing piece is the exact control basket that generated the previously reported producer relative effects of:
- `+0.57`
- `+1.16`
- `+1.86`

This must be reconstructed and confirmed before promotion to `spec_ready`.

## Working hypothesis

This should become a medium-confidence upstream producer case if the original control can be recovered or if a defensible nearby untariffed manufacturing control reproduces a similar relative path.

## Core caveat

Producer-side appliance manufacturing is broader than tariffed washers alone, so even if the effect is real, it should be presented as a diluted upstream signal rather than a tightly product-level estimate.

## Promotion rule to spec_ready

Do not move this case to `spec_ready` until all of the following are confirmed:

1. exact control series label
2. exact control series ID
3. base month
4. window start
5. window end
6. short reason why the control is nearby but less exposed
7. confirmation that the producer-side result still lands near the previously reported `+0.57`, `+1.16`, and `+1.86` pattern

## Recommended next action

Reconstruct the original producer control from the earlier washer work and confirm the full treatment-control specification before implementation.