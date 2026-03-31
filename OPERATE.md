# Tariff Incidence Tracker — Operator Workflow

This is the only supported workflow.

The repo is now **spec-first**.  
Specs are the source of truth for case metadata.  
Live `site/data` is a build artifact, not an authoring layer.

## Working folder

Use this repo only:

`C:\Users\Hayden.Stolzenberg\tarriff-tracker-git`

## Product model

- **Event** = legal tariff action
- **Case** = empirical incidence mapping attached to an event
- One event can have multiple cases
- The project is a **curated tariff-incidence tracker**, not an exhaustive automatic tracker

## Files that are authoritative

### Authoritative inputs
- `docs/case_specs/*.json`
- `data/metadata/tariff_events_master.csv`
- `data/metadata/event_case_coverage.csv`
- `site/index.html`
- `site/app.js`
- `site/style.css`

### Build / preview artifacts
- `outputs/spec_preview/*`
- `outputs/spec_preview_build/*`
- `outputs/spec_site_preview/site/*`
- `data/processed/case_price_cache.csv`

### Live deploy artifact
- `site/data/*`

## Hard rules

- Do **not** hand-edit `site/data/*`
- Do **not** hand-edit `outputs/*`
- Do **not** hand-edit live metadata CSVs for cases if the change belongs in a spec
- Do **not** push before local verification
- Use the preview pipeline first, then promote, then verify live, then push

## Supported workflow

### 1. Add or edit a case spec

Create or edit:

`docs/case_specs/<case_id>.json`

If you are backfilling a case from current live metadata, you can bootstrap a draft spec:

```powershell
python src\bootstrap_spec_from_live_case.py <case_id>