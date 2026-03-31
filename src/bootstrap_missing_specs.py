from __future__ import annotations

import argparse
import json
from pathlib import Path

from bootstrap_spec_from_live_case import (
    SPEC_DIR,
    SITE_CASES,
    EVENT_CASE_MAP,
    CASE_STAGE_MAP,
    PRODUCT_CASE_STUDIES,
    build_spec,
    infer_product_rows,
    normalize_text,
    one_row,
    read_csv,
    validate_product_rows,
)


def existing_spec_case_ids(spec_dir: Path) -> set[str]:
    return {
        p.stem
        for p in spec_dir.glob("*.json")
        if p.is_file() and not p.name.startswith("_")
    }


def write_spec(path: Path, spec: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(spec, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing spec files if present.",
    )
    args = parser.parse_args()

    site_df = read_csv(SITE_CASES)
    event_map_df = read_csv(EVENT_CASE_MAP)
    case_stage_df = read_csv(CASE_STAGE_MAP)
    product_df = read_csv(PRODUCT_CASE_STUDIES)

    spec_dir = SPEC_DIR
    existing_specs = existing_spec_case_ids(spec_dir)

    site_case_ids = sorted(
        {
            normalize_text(x)
            for x in site_df["case_id"].tolist()
            if normalize_text(x)
        }
    )

    created: list[str] = []
    skipped: list[str] = []
    errors: list[tuple[str, str]] = []

    for case_id in site_case_ids:
        out_path = spec_dir / f"{case_id}.json"

        if out_path.exists() and not args.force:
            skipped.append(case_id)
            continue

        try:
            site_row = one_row(
                site_df[site_df["case_id"].astype(str).str.strip() == case_id],
                "site_cases",
                case_id,
            )

            event_map_row = one_row(
                event_map_df[event_map_df["case_id"].astype(str).str.strip() == case_id],
                "event_case_map",
                case_id,
            )

            case_stage_row = one_row(
                case_stage_df[case_stage_df["case_id"].astype(str).str.strip() == case_id],
                "case_stage_map",
                case_id,
            )

            product_rows = infer_product_rows(product_df, site_row)
            product_case_id = validate_product_rows(product_rows, site_row)

            spec = build_spec(
                site_row=site_row,
                event_map_row=event_map_row,
                case_stage_row=case_stage_row,
                product_rows=product_rows,
                product_case_id=product_case_id,
            )

            write_spec(out_path, spec)
            created.append(case_id)

        except Exception as exc:
            errors.append((case_id, str(exc)))

    print(f"Total site cases: {len(site_case_ids)}")
    print(f"Existing specs before run: {len(existing_specs)}")
    print(f"Created specs this run: {len(created)}")
    print(f"Skipped existing specs: {len(skipped)}")
    print(f"Errors: {len(errors)}")

    if created:
        print("\nCreated:")
        for case_id in created:
            print(f"- {case_id}")

    if skipped:
        print("\nSkipped:")
        for case_id in skipped:
            print(f"- {case_id}")

    if errors:
        print("\nErrors:")
        for case_id, message in errors:
            print(f"- {case_id}: {message}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()