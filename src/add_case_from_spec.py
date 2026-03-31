from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
META = ROOT / "data" / "metadata"

SITE_CASES = META / "site_cases.csv"
EVENT_CASE_MAP = META / "event_case_map.csv"
CASE_STAGE_MAP = META / "case_stage_map.csv"
PRODUCT_CASE_STUDIES = META / "product_case_studies.csv"
EVENT_CASE_COVERAGE = META / "event_case_coverage.csv"
CASE_BUILD_QUEUE = META / "case_build_queue.csv"

SITE_CASES_FIELDS = [
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

EVENT_CASE_COVERAGE_FIELDS = [
    "event_id",
    "case_coverage_status",
    "incidence_priority",
    "candidate_stage",
    "candidate_notes",
]

CASE_BUILD_QUEUE_FIELDS = [
    "build_id",
    "event_id",
    "priority",
    "planned_stage",
    "target_case_type",
    "target_scope",
    "research_status",
    "next_action",
    "notes",
]


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            clean = {k: row.get(k, "") for k in fieldnames}
            writer.writerow(clean)


def upsert_rows(rows: list[dict], key_fields: list[str], new_row: dict) -> list[dict]:
    matched = False
    out = []

    for row in rows:
        same = all(str(row.get(k, "")).strip() == str(new_row.get(k, "")).strip() for k in key_fields)
        if same:
            out.append(new_row)
            matched = True
        else:
            out.append(row)

    if not matched:
        out.append(new_row)

    return out


def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def load_spec(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        spec = json.load(f)
    return {k: normalize_text(v) for k, v in spec.items()}


def require(spec: dict, keys: list[str]) -> None:
    missing = [k for k in keys if not normalize_text(spec.get(k, ""))]
    if missing:
        raise ValueError(f"Missing required spec fields: {missing}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("spec_path", help="Path to JSON spec file")
    args = parser.parse_args()

    spec_path = Path(args.spec_path)
    if not spec_path.is_absolute():
        spec_path = ROOT / spec_path

    spec = load_spec(spec_path)

    require(
        spec,
        [
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
            "coverage_status",
            "coverage_priority",
            "coverage_stage",
            "coverage_notes",
        ],
    )

    site_cases_row = {
        "event_id": spec["event_id"],
        "event_title": spec["event_title"],
        "authority": spec["authority"],
        "country": spec["country"],
        "announced_date": spec["announced_date"],
        "effective_date": spec["effective_date"],
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

    event_case_map_row = {
        "event_id": spec["event_id"],
        "case_id": spec["case_id"],
        "display_order": spec["display_order"],
        "primary_case_flag": spec["primary_case_flag"],
    }

    case_stage_row = {
        "case_id": spec["case_id"],
        "case_stage": spec["case_stage"],
        "stage_order": spec["stage_order"],
        "estimate_kind": spec["estimate_kind"],
        "notes": spec["stage_notes"],
    }

    treatment_row = {
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
        "notes": spec["treatment_notes"],
    }

    control_row = {
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
        "notes": spec["control_notes"],
    }

    coverage_row = {
        "event_id": spec["event_id"],
        "case_coverage_status": spec["coverage_status"],
        "incidence_priority": spec["coverage_priority"],
        "candidate_stage": spec["coverage_stage"],
        "candidate_notes": spec["coverage_notes"],
    }

    queue_row = None
    if normalize_text(spec.get("build_id", "")):
        queue_row = {
            "build_id": spec["build_id"],
            "event_id": spec["event_id"],
            "priority": normalize_text(spec.get("queue_priority", spec["coverage_priority"])),
            "planned_stage": normalize_text(spec.get("queue_planned_stage", spec["coverage_stage"])),
            "target_case_type": normalize_text(spec.get("queue_target_case_type", "")),
            "target_scope": normalize_text(spec.get("queue_target_scope", spec["treatment_label"])),
            "research_status": normalize_text(spec.get("queue_research_status", "built")),
            "next_action": normalize_text(spec.get("queue_next_action", f"Live case implemented as {spec['case_id']}")),
            "notes": normalize_text(spec.get("queue_notes", "")),
        }

    site_cases_rows = read_csv_rows(SITE_CASES)
    site_cases_rows = upsert_rows(site_cases_rows, ["case_id"], site_cases_row)
    write_csv_rows(SITE_CASES, SITE_CASES_FIELDS, site_cases_rows)

    event_case_map_rows = read_csv_rows(EVENT_CASE_MAP)
    event_case_map_rows = upsert_rows(event_case_map_rows, ["case_id"], event_case_map_row)
    write_csv_rows(EVENT_CASE_MAP, EVENT_CASE_MAP_FIELDS, event_case_map_rows)

    case_stage_rows = read_csv_rows(CASE_STAGE_MAP)
    case_stage_rows = upsert_rows(case_stage_rows, ["case_id"], case_stage_row)
    write_csv_rows(CASE_STAGE_MAP, CASE_STAGE_MAP_FIELDS, case_stage_rows)

    product_rows = read_csv_rows(PRODUCT_CASE_STUDIES)
    product_rows = upsert_rows(product_rows, ["case_id", "role"], treatment_row)
    product_rows = upsert_rows(product_rows, ["case_id", "role"], control_row)
    write_csv_rows(PRODUCT_CASE_STUDIES, PRODUCT_CASE_STUDIES_FIELDS, product_rows)

    coverage_rows = read_csv_rows(EVENT_CASE_COVERAGE)
    coverage_rows = upsert_rows(coverage_rows, ["event_id"], coverage_row)
    write_csv_rows(EVENT_CASE_COVERAGE, EVENT_CASE_COVERAGE_FIELDS, coverage_rows)

    if queue_row is not None:
        queue_rows = read_csv_rows(CASE_BUILD_QUEUE)
        queue_rows = upsert_rows(queue_rows, ["build_id"], queue_row)
        write_csv_rows(CASE_BUILD_QUEUE, CASE_BUILD_QUEUE_FIELDS, queue_rows)

    print("Updated:")
    print(f"- {SITE_CASES}")
    print(f"- {EVENT_CASE_MAP}")
    print(f"- {CASE_STAGE_MAP}")
    print(f"- {PRODUCT_CASE_STUDIES}")
    print(f"- {EVENT_CASE_COVERAGE}")
    if queue_row is not None:
        print(f"- {CASE_BUILD_QUEUE}")


if __name__ == "__main__":
    main()