# Case Build Protocol

This file governs how a tariff event moves from legal-registry status into a live empirical incidence case.

## Principle

A tariff event is the legal unit.
An incidence case is the empirical unit.

One event can support multiple cases at different stages:
- import
- upstream
- downstream / retail
- consumer

Do not force one event into one chart.

## Required statuses

Use these queue statuses in `case_build_queue.csv`:

- `queued`
- `in_research`
- `spec_ready`
- `built`
- `rejected`

## Promotion rule

A queue row should not move to `spec_ready` until all of the following are written down:

1. Exact event date
   - confirmed from the cited legal source
   - effective date, not placeholder date

2. Proposed stage
   - import, upstream, downstream, or consumer

3. Treatment basket
   - exact public series label
   - exact series ID
   - short reason it matches tariff scope

4. Control basket
   - exact public series label
   - exact series ID
   - short reason it is nearby but untreated or less exposed

5. Window design
   - base month
   - event month
   - window start
   - window end

6. Risk memo
   - main contamination risk
   - main identification weakness
   - whether this should be high, medium, low, or exploratory confidence

## Rejection rule

Reject a candidate when any of these are true:

- event scope is too broad to map cleanly to a public basket
- no defensible control exists
- treatment basket is mostly driven by unrelated shocks
- event timing is too messy or too close to overlapping policy changes
- post-event window is too short to interpret
- result would amount to fake precision

## Build sequence

For each candidate:

1. confirm legal source and exact effective date
2. identify treatment series candidates
3. identify control series candidates
4. decide stage
5. write short rationale and caveat
6. only then create or edit:
   - `site_cases.csv`
   - `event_case_map.csv`
   - `case_stage_map.csv`

## Portfolio balance target

The case library should not all point in one direction.

Target mix:
- one additional strong positive case
- one additional upstream case
- one ambiguous or weak case
- one near-null or mixed case

That balance matters for credibility.