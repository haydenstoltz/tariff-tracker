from __future__ import annotations

import csv
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE_DATA = ROOT / "site" / "data"

TARIFFS_JSON = SITE_DATA / "tariffs.json"
CASES_JSON = SITE_DATA / "cases.json"
SUMMARY_JSON = SITE_DATA / "summary.json"


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def fail(errors: list[str]) -> None:
    if not errors:
        return
    print("VALIDATION FAILED")
    for err in errors:
        print(f"- {err}")
    raise SystemExit(1)


def ensure_file(path: Path, errors: list[str]) -> None:
    if not path.exists():
        errors.append(f"Missing required file: {path}")


def ensure_unique(items: list[dict], key: str, label: str, errors: list[str]) -> None:
    seen = set()
    dupes = set()
    for item in items:
        value = str(item.get(key, "")).strip()
        if not value:
            errors.append(f"{label}: blank {key}")
            continue
        if value in seen:
            dupes.add(value)
        seen.add(value)
    if dupes:
        errors.append(f"{label}: duplicate {key} values: {sorted(dupes)}")


def to_float(value, field: str, label: str, errors: list[str]) -> float | None:
    if value is None or value == "":
        errors.append(f"{label}: missing numeric field '{field}'")
        return None
    try:
        return float(value)
    except Exception:
        errors.append(f"{label}: invalid numeric field '{field}' = {value!r}")
        return None


def validate_chart_payload(case_id: str, event_month: str, chart_path: Path, errors: list[str]) -> None:
    if not chart_path.exists():
        errors.append(f"{case_id}: missing chart payload {chart_path}")
        return

    payload = load_json(chart_path)

    for key in ["labels", "treatment", "control", "relative_effect"]:
        if key not in payload:
            errors.append(f"{case_id}: chart payload missing key '{key}'")
            return

    labels = payload["labels"]
    treatment = payload["treatment"]
    control = payload["control"]
    relative = payload["relative_effect"]

    n = len(labels)
    if not (len(treatment) == len(control) == len(relative) == n):
        errors.append(
            f"{case_id}: chart arrays have inconsistent lengths "
            f"(labels={len(labels)}, treatment={len(treatment)}, control={len(control)}, relative={len(relative)})"
        )
        return

    if n == 0:
        errors.append(f"{case_id}: chart payload has zero rows")
        return

    if event_month not in labels:
        errors.append(f"{case_id}: event month {event_month} not found in chart labels")

    for i, (t, c, r) in enumerate(zip(treatment, control, relative)):
        try:
            t = float(t)
            c = float(c)
            r = float(r)
        except Exception:
            errors.append(f"{case_id}: non-numeric chart value at row {i}")
            continue

        implied = round(t - c, 3)
        if not math.isclose(implied, float(r), abs_tol=0.02):
            errors.append(
                f"{case_id}: relative_effect mismatch at row {i} "
                f"(treatment-control={implied}, relative_effect={r})"
            )
            break


def validate_csv_payload(case_id: str, csv_path: Path, chart_path: Path, errors: list[str]) -> None:
    if not csv_path.exists():
        errors.append(f"{case_id}: missing csv payload {csv_path}")
        return

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    expected_cols = ["month", "treatment", "control", "relative_effect"]
    if reader.fieldnames != expected_cols:
        errors.append(f"{case_id}: csv columns {reader.fieldnames!r} do not match expected {expected_cols!r}")
        return

    if not rows:
        errors.append(f"{case_id}: csv payload has zero rows")
        return

    chart_payload = load_json(chart_path)
    if len(rows) != len(chart_payload["labels"]):
        errors.append(
            f"{case_id}: csv row count {len(rows)} does not match chart row count {len(chart_payload['labels'])}"
        )
        return

    for i, row in enumerate(rows):
        month = str(row["month"]).strip()
        if month != str(chart_payload["labels"][i]).strip():
            errors.append(
                f"{case_id}: csv/chart label mismatch at row {i} "
                f"(csv={month!r}, chart={chart_payload['labels'][i]!r})"
            )
            break

        try:
            t = float(row["treatment"])
            c = float(row["control"])
            r = float(row["relative_effect"])
        except Exception:
            errors.append(f"{case_id}: non-numeric csv value at row {i}")
            break

        implied = round(t - c, 3)
        if not math.isclose(implied, r, abs_tol=0.02):
            errors.append(
                f"{case_id}: csv relative_effect mismatch at row {i} "
                f"(treatment-control={implied}, relative_effect={r})"
            )
            break


def main() -> None:
    errors: list[str] = []

    for path in [TARIFFS_JSON, CASES_JSON, SUMMARY_JSON]:
        ensure_file(path, errors)
    fail(errors)

    tariffs = load_json(TARIFFS_JSON)
    cases = load_json(CASES_JSON)
    summary = load_json(SUMMARY_JSON)

    if not isinstance(tariffs, list):
        errors.append("tariffs.json is not a list")
        fail(errors)
    if not isinstance(cases, list):
        errors.append("cases.json is not a list")
        fail(errors)
    if not isinstance(summary, dict):
        errors.append("summary.json is not an object")
        fail(errors)

    ensure_unique(tariffs, "event_id", "tariffs.json", errors)
    ensure_unique(cases, "case_id", "cases.json", errors)

    tariff_map = {}
    for event in tariffs:
        event_id = str(event.get("event_id", "")).strip()
        if not event_id:
            continue
        tariff_map[event_id] = event

        for field in ["title", "authority", "effective_date", "status_bucket", "legal_source_label", "legal_source_url"]:
            if not str(event.get(field, "")).strip():
                errors.append(f"event {event_id}: missing required field '{field}'")

    mapped_counts = {}
    for case in cases:
        case_id = str(case.get("case_id", "")).strip()
        event_id = str(case.get("event_id", "")).strip()

        if not event_id:
            errors.append(f"case {case_id}: missing event_id")
            continue
        if event_id not in tariff_map:
            errors.append(f"case {case_id}: event_id '{event_id}' not found in tariffs.json")
            continue

        mapped_counts[event_id] = mapped_counts.get(event_id, 0) + 1

        for field in [
            "case_name",
            "source_type",
            "treatment_label",
            "control_label",
            "chart_file",
            "csv_file",
            "case_stage",
            "estimate_kind",
        ]:
            if not str(case.get(field, "")).strip():
                errors.append(f"case {case_id}: missing required field '{field}'")

        if case_id not in summary:
            errors.append(f"case {case_id}: missing summary entry in summary.json")
            continue

        s = summary[case_id]
        for metric in ["m3", "m6", "m12", "pre_event_gap_std_pp", "peak_post_gap_pp"]:
            if metric in s and s[metric] not in (None, ""):
                to_float(s[metric], metric, f"case {case_id} summary", errors)

        if str(s.get("sign", "")).strip().lower() not in {"positive", "negative"}:
            errors.append(f"case {case_id} summary: invalid sign '{s.get('sign')}'")

        event = tariff_map[event_id]
        event_month = str(event.get("effective_date", "")).strip()[:7]

        chart_rel = str(case.get("chart_file", "")).replace("./data/", "")
        csv_rel = str(case.get("csv_file", "")).replace("./data/", "")

        chart_path = SITE_DATA / chart_rel
        csv_path = SITE_DATA / csv_rel

        validate_chart_payload(case_id, event_month, chart_path, errors)
        validate_csv_payload(case_id, csv_path, chart_path, errors)

    for event in tariffs:
        event_id = str(event.get("event_id", "")).strip()
        has_live_cases = bool(event.get("has_live_cases"))
        live_case_count = int(event.get("live_case_count", 0) or 0)
        actual_count = mapped_counts.get(event_id, 0)

        if has_live_cases != (actual_count > 0):
            errors.append(
                f"event {event_id}: has_live_cases={has_live_cases} but actual mapped live cases={actual_count}"
            )

        if live_case_count != actual_count:
            errors.append(
                f"event {event_id}: live_case_count={live_case_count} but actual mapped live cases={actual_count}"
            )

    fail(errors)

    print("VALIDATION PASSED")
    print(f"events: {len(tariffs)}")
    print(f"cases: {len(cases)}")
    print(f"summary rows: {len(summary)}")


if __name__ == "__main__":
    main()