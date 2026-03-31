from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SITE_DATA_DIR = ROOT / "site" / "data"


def resolve_path(path_str: str, default_path: Path) -> Path:
    if not path_str.strip():
        return default_path
    path = Path(path_str)
    if not path.is_absolute():
        path = ROOT / path
    return path


def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing required JSON file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def require_keys(obj: dict, keys: list[str], label: str) -> None:
    missing = [k for k in keys if k not in obj]
    if missing:
        raise ValueError(f"Missing required keys in {label}: {missing}")


def normalize_rel_path(rel_path: str) -> str:
    rel_path = str(rel_path).strip()
    if rel_path.startswith("./"):
        rel_path = rel_path[2:]
    return rel_path.replace("/", str(Path("/"))).replace("\\", str(Path("/")))


def round_series(values) -> list[float]:
    return [round(float(x), 3) for x in values]


def series_close(a, b, tol: float = 0.002) -> bool:
    if len(a) != len(b):
        return False
    return all(abs(float(x) - float(y)) <= tol for x, y in zip(a, b))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--site-data-dir",
        default="",
        help="Directory containing exported site data files. Default: site/data",
    )
    args = parser.parse_args()

    site_data_dir = resolve_path(args.site_data_dir, DEFAULT_SITE_DATA_DIR)
    site_root = site_data_dir.parent

    tariffs_file = site_data_dir / "tariffs.json"
    cases_file = site_data_dir / "cases.json"
    summary_file = site_data_dir / "summary.json"
    charts_dir = site_data_dir / "charts"
    csv_dir = site_data_dir / "csv"

    if not charts_dir.exists():
        raise FileNotFoundError(f"Missing charts directory: {charts_dir}")
    if not csv_dir.exists():
        raise FileNotFoundError(f"Missing csv directory: {csv_dir}")

    tariffs = load_json(tariffs_file)
    cases = load_json(cases_file)
    summary = load_json(summary_file)

    if not isinstance(tariffs, list):
        raise ValueError("tariffs.json must contain a JSON list")
    if not isinstance(cases, list):
        raise ValueError("cases.json must contain a JSON list")
    if not isinstance(summary, dict):
        raise ValueError("summary.json must contain a JSON object")

    required_tariff_keys = [
        "event_id",
        "title",
        "authority",
        "status",
        "has_live_cases",
        "live_case_count",
    ]
    required_case_keys = [
        "case_id",
        "event_id",
        "case_name",
        "source_type",
        "treatment_label",
        "control_label",
        "chart_file",
        "csv_file",
        "display_order",
        "case_stage",
        "stage_order",
    ]
    required_summary_keys = [
        "m3",
        "m6",
        "m12",
        "sign",
        "pre_event_gap_std_pp",
        "peak_post_gap_pp",
        "peak_post_gap_month",
        "placebo_n_3m",
        "placebo_p_abs_3m",
        "placebo_n_6m",
        "placebo_p_abs_6m",
    ]

    event_ids = []
    for i, tariff in enumerate(tariffs):
        if not isinstance(tariff, dict):
            raise ValueError(f"tariffs.json item {i} must be an object")
        require_keys(tariff, required_tariff_keys, f"tariffs.json item {i}")
        event_id = str(tariff["event_id"]).strip()
        if not event_id:
            raise ValueError(f"Blank event_id in tariffs.json item {i}")
        event_ids.append(event_id)

    duplicate_event_ids = sorted({x for x in event_ids if event_ids.count(x) > 1})
    if duplicate_event_ids:
        raise ValueError(f"Duplicate event_id values in tariffs.json: {duplicate_event_ids}")

    tariff_by_event = {str(t["event_id"]).strip(): t for t in tariffs}

    case_ids = []
    live_case_counts_from_cases: dict[str, int] = {}

    for i, case in enumerate(cases):
        if not isinstance(case, dict):
            raise ValueError(f"cases.json item {i} must be an object")
        require_keys(case, required_case_keys, f"cases.json item {i}")

        case_id = str(case["case_id"]).strip()
        event_id = str(case["event_id"]).strip()

        if not case_id:
            raise ValueError(f"Blank case_id in cases.json item {i}")
        if not event_id:
            raise ValueError(f"Blank event_id in cases.json item {i}")
        if event_id not in tariff_by_event:
            raise ValueError(f"cases.json item {i} references unknown event_id '{event_id}'")

        case_ids.append(case_id)
        live_case_counts_from_cases[event_id] = live_case_counts_from_cases.get(event_id, 0) + 1

        if case_id not in summary:
            raise ValueError(f"Missing summary.json entry for case_id '{case_id}'")

        summary_entry = summary[case_id]
        if not isinstance(summary_entry, dict):
            raise ValueError(f"summary.json entry for case_id '{case_id}' must be an object")
        require_keys(summary_entry, required_summary_keys, f"summary.json['{case_id}']")

        sign = str(summary_entry["sign"]).strip()
        if sign not in {"positive", "negative"}:
            raise ValueError(f"Invalid sign for case_id '{case_id}': {sign}")

        chart_rel = normalize_rel_path(case["chart_file"])
        csv_rel = normalize_rel_path(case["csv_file"])

        chart_path = site_root / chart_rel
        csv_path = site_root / csv_rel

        if not chart_path.exists():
            raise FileNotFoundError(f"Missing chart JSON for case_id '{case_id}': {chart_path}")
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing chart CSV for case_id '{case_id}': {csv_path}")

        chart_payload = load_json(chart_path)
        if not isinstance(chart_payload, dict):
            raise ValueError(f"Chart payload for case_id '{case_id}' must be an object")

        require_keys(
            chart_payload,
            ["labels", "treatment", "control", "relative_effect"],
            f"chart payload for case_id '{case_id}'",
        )

        labels = chart_payload["labels"]
        treatment = chart_payload["treatment"]
        control = chart_payload["control"]
        relative = chart_payload["relative_effect"]

        if not all(isinstance(x, list) for x in [labels, treatment, control, relative]):
            raise ValueError(f"Chart payload arrays malformed for case_id '{case_id}'")

        n = len(labels)
        if n == 0:
            raise ValueError(f"Empty chart payload for case_id '{case_id}'")

        if len(treatment) != n or len(control) != n or len(relative) != n:
            raise ValueError(
                f"Chart array length mismatch for case_id '{case_id}': "
                f"labels={n}, treatment={len(treatment)}, control={len(control)}, relative={len(relative)}"
            )

        chart_relative_check = [round(float(t) - float(c), 3) for t, c in zip(treatment, control)]
        if not series_close(chart_relative_check, round_series(relative)):
            raise ValueError(
                f"relative_effect does not approximately equal treatment-control for case_id '{case_id}'"
            )

        csv_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        expected_csv_cols = ["month", "treatment", "control", "relative_effect"]
        if list(csv_df.columns) != expected_csv_cols:
            raise ValueError(
                f"CSV columns for case_id '{case_id}' are {list(csv_df.columns)}, expected {expected_csv_cols}"
            )

        if len(csv_df) != n:
            raise ValueError(
                f"CSV row count mismatch for case_id '{case_id}': csv={len(csv_df)} chart={n}"
            )

        csv_months = csv_df["month"].astype(str).str.strip().tolist()
        if csv_months != [str(x).strip() for x in labels]:
            raise ValueError(f"CSV months do not match chart labels for case_id '{case_id}'")

        csv_treatment = round_series(pd.to_numeric(csv_df["treatment"], errors="raise").tolist())
        csv_control = round_series(pd.to_numeric(csv_df["control"], errors="raise").tolist())
        csv_relative = round_series(pd.to_numeric(csv_df["relative_effect"], errors="raise").tolist())

        if not series_close(csv_treatment, round_series(treatment), tol=0.0005):
            raise ValueError(f"CSV treatment series does not match chart JSON for case_id '{case_id}'")
        if not series_close(csv_control, round_series(control), tol=0.0005):
            raise ValueError(f"CSV control series does not match chart JSON for case_id '{case_id}'")
        if not series_close(csv_relative, round_series(relative), tol=0.0005):
            raise ValueError(f"CSV relative_effect series does not match chart JSON for case_id '{case_id}'")

    duplicate_case_ids = sorted({x for x in case_ids if case_ids.count(x) > 1})
    if duplicate_case_ids:
        raise ValueError(f"Duplicate case_id values in cases.json: {duplicate_case_ids}")

    extra_summary_case_ids = sorted(set(summary.keys()) - set(case_ids))
    if extra_summary_case_ids:
        raise ValueError(f"summary.json contains case_ids not present in cases.json: {extra_summary_case_ids}")

    for event_id, tariff in tariff_by_event.items():
        expected = int(tariff["live_case_count"])
        actual = int(live_case_counts_from_cases.get(event_id, 0))
        if expected != actual:
            raise ValueError(
                f"live_case_count mismatch for event_id '{event_id}': tariffs.json={expected}, cases.json={actual}"
            )

        has_live_cases = bool(tariff["has_live_cases"])
        if has_live_cases != (actual > 0):
            raise ValueError(
                f"has_live_cases mismatch for event_id '{event_id}': tariffs.json={has_live_cases}, cases.json count={actual}"
            )

    print("Validated exported site data bundle successfully.")
    print(f"Site data dir: {site_data_dir}")
    print(f"Tariffs: {len(tariffs)}")
    print(f"Cases: {len(cases)}")
    print(f"Summary entries: {len(summary)}")
    print(f"Chart files: {len(list(charts_dir.glob('*.json')))}")
    print(f"CSV files: {len(list(csv_dir.glob('*.csv')))}")


if __name__ == "__main__":
    main()