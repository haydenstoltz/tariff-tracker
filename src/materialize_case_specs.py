from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = ROOT / "docs" / "case_specs"
META_DIR = ROOT / "data" / "metadata"

SITE_CASES = META_DIR / "site_cases.csv"
EVENT_CASE_MAP = META_DIR / "event_case_map.csv"
CASE_STAGE_MAP = META_DIR / "case_stage_map.csv"
PRODUCT_CASE_STUDIES = META_DIR / "product_case_studies.csv"

SITE_CASES_FIELDS = [
    "event_id",
    "event_title",
    "authority",
    "country",
    "announced_date",
    "effective_date",
    "status",
    "event_date_type",
    "event_source_label",
    "event_source_url",
    "event_status",
    "site_status",
    "case_id",
    "case_name",
    "source_type",
    "treatment_label",
    "control_label",
    "confidence_tier",
    "rationale_short",
    "caveat",
    "robustness_note",
    "method_note",
]

EVENT_CASE_MAP_FIELDS = [
    "event_id",
    "case_id",
    "display_order",
    "primary_case_flag",
]

CASE_STAGE_MAP_FIELDS = [
    "case_id",
    "case_stage",
    "stage_order",
    "estimate_kind",
    "notes",
]

PRODUCT_CASE_STUDIES_FIELDS = [
    "case_id",
    "case_name",
    "status",
    "series_id",
    "series_label",
    "source_type",
    "role",
    "event_date",
    "base_date",
    "window_start",
    "window_end",
    "policy_date_type",
    "tariff_authority",
    "notes",
]

REQUIRED_SPEC_FIELDS = [
    "event_id",
    "event_title",
    "authority",
    "country",
    "announced_date",
    "effective_date",
    "event_date_type",
    "event_source_label",
    "event_source_url",
    "event_status",
    "site_status",
    "case_id",
    "case_name",
    "source_type",
    "treatment_series_id",
    "treatment_label",
    "control_series_id",
    "control_label",
    "confidence_tier",
    "rationale_short",
    "caveat",
    "robustness_note",
    "method_note",
    "case_stage",
    "stage_order",
    "estimate_kind",
    "stage_notes",
    "display_order",
    "primary_case_flag",
    "base_date",
    "window_start",
    "window_end",
    "policy_date_type",
    "tariff_authority",
    "treatment_notes",
    "control_notes",
]


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def with_role_prefix(note: str, role: str) -> str:
    note = normalize_text(note)
    lower = note.lower()

    if role == "control":
        if "control" in lower:
            return note
        return f"control - {note}" if note else "control"

    if role == "treatment":
        if "treatment" in lower:
            return note
        return f"treatment - {note}" if note else "treatment"

    return note


def load_spec(path: Path) -> dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    spec = {str(k): normalize_text(v) for k, v in raw.items()}
    missing = [field for field in REQUIRED_SPEC_FIELDS if not spec.get(field)]
    if missing:
        raise ValueError(f"{path.name}: missing required spec fields: {missing}")

    return spec


def load_specs(spec_dir: Path) -> list[dict[str, str]]:
    if not spec_dir.exists():
        raise FileNotFoundError(f"Spec directory not found: {spec_dir}")

    spec_paths = sorted(
        p for p in spec_dir.glob("*.json")
        if p.is_file() and not p.name.startswith("_")
    )

    if not spec_paths:
        return []

    specs = [load_spec(path) for path in spec_paths]

    case_ids = [s["case_id"] for s in specs]
    duplicate_case_ids = sorted({x for x in case_ids if case_ids.count(x) > 1})
    if duplicate_case_ids:
        raise ValueError(f"Duplicate case_id values across specs: {duplicate_case_ids}")

    return specs


def sort_key_int(value: str) -> tuple[int, str]:
    try:
        return (0, int(value))
    except ValueError:
        return (1, value)


def build_rows(specs: Iterable[dict[str, str]]) -> tuple[list[dict[str, str]], ...]:
    site_cases_rows: list[dict[str, str]] = []
    event_case_map_rows: list[dict[str, str]] = []
    case_stage_map_rows: list[dict[str, str]] = []
    product_case_rows: list[dict[str, str]] = []

    for spec in specs:
        site_cases_rows.append(
            {
                "event_id": spec["event_id"],
                "event_title": spec["event_title"],
                "authority": spec["authority"],
                "country": spec["country"],
                "announced_date": spec["announced_date"],
                "effective_date": spec["effective_date"],
                "status": spec["event_status"],
                "event_date_type": spec["event_date_type"],
                "event_source_label": spec["event_source_label"],
                "event_source_url": spec["event_source_url"],
                "event_status": spec["event_status"],
                "site_status": spec["site_status"],
                "case_id": spec["case_id"],
                "case_name": spec["case_name"],
                "source_type": spec["source_type"],
                "treatment_label": spec["treatment_label"],
                "control_label": spec["control_label"],
                "confidence_tier": spec["confidence_tier"],
                "rationale_short": spec["rationale_short"],
                "caveat": spec["caveat"],
                "robustness_note": spec["robustness_note"],
                "method_note": spec["method_note"],
            }
        )

        event_case_map_rows.append(
            {
                "event_id": spec["event_id"],
                "case_id": spec["case_id"],
                "display_order": spec["display_order"],
                "primary_case_flag": spec["primary_case_flag"],
            }
        )

        case_stage_map_rows.append(
            {
                "case_id": spec["case_id"],
                "case_stage": spec["case_stage"],
                "stage_order": spec["stage_order"],
                "estimate_kind": spec["estimate_kind"],
                "notes": spec["stage_notes"],
            }
        )

        product_case_rows.append(
            {
                "case_id": spec["case_id"],
                "case_name": spec["case_name"],
                "status": "benchmark",
                "series_id": spec["treatment_series_id"],
                "series_label": spec["treatment_label"],
                "source_type": spec["source_type"],
                "role": "treatment",
                "event_date": spec["effective_date"],
                "base_date": spec["base_date"],
                "window_start": spec["window_start"],
                "window_end": spec["window_end"],
                "policy_date_type": spec["policy_date_type"],
                "tariff_authority": spec["tariff_authority"],
                "notes": with_role_prefix(spec["treatment_notes"], "treatment"),
            }
        )

        product_case_rows.append(
            {
                "case_id": spec["case_id"],
                "case_name": spec["case_name"],
                "status": "benchmark",
                "series_id": spec["control_series_id"],
                "series_label": spec["control_label"],
                "source_type": spec["source_type"],
                "role": "control",
                "event_date": spec["effective_date"],
                "base_date": spec["base_date"],
                "window_start": spec["window_start"],
                "window_end": spec["window_end"],
                "policy_date_type": spec["policy_date_type"],
                "tariff_authority": spec["tariff_authority"],
                "notes": with_role_prefix(spec["control_notes"], "control"),
            }
        )

    site_cases_rows.sort(key=lambda r: (r["event_id"], r["case_id"]))
    event_case_map_rows.sort(key=lambda r: (r["event_id"], sort_key_int(r["display_order"]), r["case_id"]))
    case_stage_map_rows.sort(key=lambda r: (sort_key_int(r["stage_order"]), r["case_id"]))
    product_case_rows.sort(key=lambda r: (r["case_id"], 0 if r["role"] == "treatment" else 1, r["series_label"]))

    return site_cases_rows, event_case_map_rows, case_stage_map_rows, product_case_rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def read_existing_case_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "case_id" not in (reader.fieldnames or []):
            return set()
        return {
            normalize_text(row.get("case_id", ""))
            for row in reader
            if normalize_text(row.get("case_id", ""))
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="Write rebuilt metadata CSVs.")
    parser.add_argument(
        "--spec-dir",
        default=str(SPEC_DIR),
        help="Directory containing case spec JSON files. Default: docs/case_specs",
    )
    args = parser.parse_args()

    spec_dir = Path(args.spec_dir)
    if not spec_dir.is_absolute():
        spec_dir = ROOT / spec_dir

    specs = load_specs(spec_dir)
    spec_case_ids = {spec["case_id"] for spec in specs}
    current_case_ids = read_existing_case_ids(SITE_CASES)

    print(f"Loaded {len(specs)} spec files from {spec_dir}")
    print(f"Spec case_ids: {sorted(spec_case_ids)}")

    if current_case_ids:
        missing_specs = sorted(current_case_ids - spec_case_ids)
        new_specs = sorted(spec_case_ids - current_case_ids)

        print(f"Current site_cases.csv case_ids: {sorted(current_case_ids)}")
        print(f"Missing specs for existing case_ids: {missing_specs if missing_specs else 'none'}")
        print(f"Spec-only case_ids not yet in site_cases.csv: {new_specs if new_specs else 'none'}")
    else:
        print("No existing site_cases.csv case_ids found for comparison.")

    if not specs:
        if args.write:
            raise ValueError(
                f"No case spec files found in {spec_dir}. Add at least one spec besides _case_template.json before using --write."
            )
        print("No live case specs found. Check complete. No files written.")
        return

    if not args.write:
        print("Check complete. No files written. Re-run with --write after all live cases have specs.")
        return

    site_cases_rows, event_case_map_rows, case_stage_map_rows, product_case_rows = build_rows(specs)

    write_csv(SITE_CASES, SITE_CASES_FIELDS, site_cases_rows)
    write_csv(EVENT_CASE_MAP, EVENT_CASE_MAP_FIELDS, event_case_map_rows)
    write_csv(CASE_STAGE_MAP, CASE_STAGE_MAP_FIELDS, case_stage_map_rows)
    write_csv(PRODUCT_CASE_STUDIES, PRODUCT_CASE_STUDIES_FIELDS, product_case_rows)

    print("Wrote:")
    print(f"- {SITE_CASES}")
    print(f"- {EVENT_CASE_MAP}")
    print(f"- {CASE_STAGE_MAP}")
    print(f"- {PRODUCT_CASE_STUDIES}")


if __name__ == "__main__":
    main()