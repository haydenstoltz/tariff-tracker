from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = ROOT / "docs" / "case_specs"
META_DIR = ROOT / "data" / "metadata"

SITE_CASES = META_DIR / "site_cases.csv"
EVENT_CASE_MAP = META_DIR / "event_case_map.csv"
CASE_STAGE_MAP = META_DIR / "case_stage_map.csv"
PRODUCT_CASE_STUDIES = META_DIR / "product_case_studies.csv"

SITE_CASES_FIELDS = [
    "announced_date",
    "authority",
    "case_id",
    "case_name",
    "caveat",
    "confidence_tier",
    "control_label",
    "country",
    "effective_date",
    "event_date_type",
    "event_id",
    "event_source_label",
    "event_source_url",
    "event_status",
    "event_title",
    "method_note",
    "rationale_short",
    "robustness_note",
    "site_status",
    "source_type",
    "treatment_label",
]

EVENT_CASE_MAP_FIELDS = [
    "case_id",
    "display_order",
    "event_id",
    "notes",
    "primary_case_flag",
]

CASE_STAGE_MAP_FIELDS = [
    "case_id",
    "case_stage",
    "estimate_kind",
    "notes",
    "stage_order",
]

PRODUCT_CASE_STUDIES_FIELDS = [
    "base_date",
    "case_id",
    "case_name",
    "event_date",
    "notes",
    "policy_date_type",
    "role",
    "series_id",
    "series_label",
    "source_type",
    "status",
    "tariff_authority",
    "window_end",
    "window_start",
]

REQUIRED_TOP_LEVEL_FIELDS = [
    "site_event_id",
    "event_map_id",
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
    "site_source_type",
    "treatment_label",
    "control_label",
    "confidence_tier",
    "rationale_short",
    "caveat",
    "robustness_note",
    "method_note",
    "display_order",
    "primary_case_flag",
    "event_case_map_notes",
    "case_stage",
    "stage_order",
    "estimate_kind",
    "case_stage_notes",
    "series",
]

REQUIRED_SERIES_FIELDS = [
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


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def sort_key_int(value: str) -> tuple[int, str]:
    try:
        return (0, int(value))
    except ValueError:
        return (1, value)


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


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def validate_series(case_id: str, site_source_type: str, treatment_label: str, control_label: str, series_rows: list[dict[str, str]]) -> None:
    if not series_rows:
        raise ValueError(f"{case_id}: spec must include at least one series row")

    site_rows = [row for row in series_rows if row["source_type"] == site_source_type]
    if not site_rows:
        raise ValueError(f"{case_id}: no series rows found for site_source_type='{site_source_type}'")

    matching_treatment = [
        row for row in site_rows
        if row["series_label"] == treatment_label and row["role"] == "treatment"
    ]
    if not matching_treatment:
        raise ValueError(
            f"{case_id}: no site-layer treatment row matching treatment_label='{treatment_label}' and role='treatment'"
        )

    matching_control = [
        row for row in site_rows
        if row["series_label"] == control_label and row["role"] == "control"
    ]
    if not matching_control:
        raise ValueError(
            f"{case_id}: no site-layer control row matching control_label='{control_label}' and role='control'"
        )


def load_spec(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"{path.name}: spec root must be a JSON object")

    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in raw]
    if missing:
        raise ValueError(f"{path.name}: missing required top-level fields: {missing}")

    spec: dict[str, object] = {}
    for key, value in raw.items():
        if key == "series":
            spec[key] = value
        else:
            spec[key] = normalize_text(value)

    if not isinstance(spec["series"], list):
        raise ValueError(f"{path.name}: 'series' must be a list")

    series_rows: list[dict[str, str]] = []
    for idx, row in enumerate(spec["series"], start=1):
        if not isinstance(row, dict):
            raise ValueError(f"{path.name}: series row {idx} must be a JSON object")

        missing_series = [field for field in REQUIRED_SERIES_FIELDS if field not in row]
        if missing_series:
            raise ValueError(f"{path.name}: series row {idx} missing required fields: {missing_series}")

        clean_row = {key: normalize_text(value) for key, value in row.items()}
        empty_required = [field for field in REQUIRED_SERIES_FIELDS if not clean_row.get(field)]
        if empty_required:
            raise ValueError(f"{path.name}: series row {idx} has blank required fields: {empty_required}")

        series_rows.append(clean_row)

    spec["series"] = series_rows

    empty_top = [
        field for field in REQUIRED_TOP_LEVEL_FIELDS
        if field != "series" and not normalize_text(spec.get(field, ""))
    ]
    if empty_top:
        raise ValueError(f"{path.name}: blank required top-level fields: {empty_top}")

    validate_series(
        case_id=normalize_text(spec["case_id"]),
        site_source_type=normalize_text(spec["site_source_type"]),
        treatment_label=normalize_text(spec["treatment_label"]),
        control_label=normalize_text(spec["control_label"]),
        series_rows=series_rows,
    )

    return spec


def load_specs(spec_dir: Path) -> list[dict]:
    if not spec_dir.exists():
        raise FileNotFoundError(f"Spec directory not found: {spec_dir}")

    spec_paths = sorted(
        p for p in spec_dir.glob("*.json")
        if p.is_file() and not p.name.startswith("_")
    )

    if not spec_paths:
        return []

    specs = [load_spec(path) for path in spec_paths]

    case_ids = [normalize_text(spec["case_id"]) for spec in specs]
    duplicate_case_ids = sorted({x for x in case_ids if case_ids.count(x) > 1})
    if duplicate_case_ids:
        raise ValueError(f"Duplicate case_id values across specs: {duplicate_case_ids}")

    return specs


def build_rows(specs: list[dict]) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    site_cases_rows: list[dict[str, str]] = []
    event_case_map_rows: list[dict[str, str]] = []
    case_stage_map_rows: list[dict[str, str]] = []
    product_case_rows: list[dict[str, str]] = []

    for spec in specs:
        case_id = normalize_text(spec["case_id"])

        site_cases_rows.append(
            {
                "announced_date": normalize_text(spec["announced_date"]),
                "authority": normalize_text(spec["authority"]),
                "case_id": case_id,
                "case_name": normalize_text(spec["case_name"]),
                "caveat": normalize_text(spec["caveat"]),
                "confidence_tier": normalize_text(spec["confidence_tier"]),
                "control_label": normalize_text(spec["control_label"]),
                "country": normalize_text(spec["country"]),
                "effective_date": normalize_text(spec["effective_date"]),
                "event_date_type": normalize_text(spec["event_date_type"]),
                "event_id": normalize_text(spec["site_event_id"]),
                "event_source_label": normalize_text(spec["event_source_label"]),
                "event_source_url": normalize_text(spec["event_source_url"]),
                "event_status": normalize_text(spec["event_status"]),
                "event_title": normalize_text(spec["event_title"]),
                "method_note": normalize_text(spec["method_note"]),
                "rationale_short": normalize_text(spec["rationale_short"]),
                "robustness_note": normalize_text(spec["robustness_note"]),
                "site_status": normalize_text(spec["site_status"]),
                "source_type": normalize_text(spec["site_source_type"]),
                "treatment_label": normalize_text(spec["treatment_label"]),
            }
        )

        event_case_map_rows.append(
            {
                "case_id": case_id,
                "display_order": normalize_text(spec["display_order"]),
                "event_id": normalize_text(spec["event_map_id"]),
                "notes": normalize_text(spec["event_case_map_notes"]),
                "primary_case_flag": normalize_text(spec["primary_case_flag"]),
            }
        )

        case_stage_map_rows.append(
            {
                "case_id": case_id,
                "case_stage": normalize_text(spec["case_stage"]),
                "estimate_kind": normalize_text(spec["estimate_kind"]),
                "notes": normalize_text(spec["case_stage_notes"]),
                "stage_order": normalize_text(spec["stage_order"]),
            }
        )

        for row in spec["series"]:
            product_case_rows.append(
                {
                    "base_date": row["base_date"],
                    "case_id": case_id,
                    "case_name": normalize_text(spec["case_name"]),
                    "event_date": row["event_date"],
                    "notes": row["notes"],
                    "policy_date_type": row["policy_date_type"],
                    "role": row["role"],
                    "series_id": row["series_id"],
                    "series_label": row["series_label"],
                    "source_type": row["source_type"],
                    "status": row["status"],
                    "tariff_authority": row["tariff_authority"],
                    "window_end": row["window_end"],
                    "window_start": row["window_start"],
                }
            )

    site_cases_rows.sort(key=lambda r: (r["event_id"], r["case_id"]))
    event_case_map_rows.sort(key=lambda r: (r["event_id"], sort_key_int(r["display_order"]), r["case_id"]))
    case_stage_map_rows.sort(key=lambda r: (sort_key_int(r["stage_order"]), r["case_id"]))
    product_case_rows.sort(
        key=lambda r: (
            r["case_id"],
            r["case_name"],
            r["source_type"],
            0 if r["role"] == "treatment" else 1 if r["role"] == "control" else 2,
            r["series_label"],
        )
    )

    return site_cases_rows, event_case_map_rows, case_stage_map_rows, product_case_rows


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
    spec_case_ids = sorted(normalize_text(spec["case_id"]) for spec in specs)
    current_case_ids = sorted(read_existing_case_ids(SITE_CASES))

    print(f"Loaded {len(specs)} spec files from {spec_dir}")
    print(f"Spec case_ids: {spec_case_ids}")

    if current_case_ids:
        missing_specs = sorted(set(current_case_ids) - set(spec_case_ids))
        new_specs = sorted(set(spec_case_ids) - set(current_case_ids))

        print(f"Current site_cases.csv case_ids: {current_case_ids}")
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