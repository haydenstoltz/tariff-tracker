from __future__ import annotations

import argparse
import json
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_TARGETS_FILE = ROOT / "data" / "metadata" / "world" / "pair_pull_targets.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

INDICATORS: dict[str, dict[str, str]] = {
    "tradestats-trade": {
        "MPRT-TRD-VL": "Import Trade Value (US$ Thousand)",
    },
    "tradestats-tariff": {
        "AHS-WGHTD-AVRG": "Effectively Applied Weighted Average Tariff (%)",
        "AHS-SMPL-AVRG": "Effectively Applied Simple Average Tariff (%)",
        "TRF-NMBR-AGGRMNT": "Number of Tariff Agreements",
    },
}

# For this MVP, these are aggregate partner-level indicators.
# Use the Country-Year-Partner endpoint, not Country-Year-Partner-Product.
NO_PRODUCT_INDICATORS = {
    "MPRT-TRD-VL",
    "AHS-WGHTD-AVRG",
    "AHS-SMPL-AVRG",
    "TRF-NMBR-AGGRMNT",
}


def resolve_path(path_str: str, default_path: Path) -> Path:
    if not path_str.strip():
        return default_path
    path = Path(path_str)
    if not path.is_absolute():
        path = ROOT / path
    return path


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def normalize_key(key: str) -> str:
    return strip_ns(str(key)).strip().upper()


def build_url(dataset: str, reporter_id: str, partner_id: str, year: str, indicator_code: str) -> str:
    base = (
        f"https://wits.worldbank.org/API/V1/SDMX/V21/datasource/{dataset}"
        f"/reporter/{reporter_id.lower()}"
        f"/year/{year}"
        f"/partner/{partner_id.lower()}"
    )

    if indicator_code in NO_PRODUCT_INDICATORS:
        return f"{base}/indicator/{indicator_code}"

    return f"{base}/product/all/indicator/{indicator_code}"


def parse_sdmx_xml(xml_text: str) -> list[dict[str, str]]:
    root = ET.fromstring(xml_text)
    rows: list[dict[str, str]] = []

    for series in root.iter():
        if strip_ns(series.tag) != "Series":
            continue

        series_attrs = {normalize_key(k): normalize_text(v) for k, v in series.attrib.items()}
        obs_nodes = [node for node in series if strip_ns(node.tag) == "Obs"]

        if obs_nodes:
            for obs in obs_nodes:
                row = dict(series_attrs)
                row.update({normalize_key(k): normalize_text(v) for k, v in obs.attrib.items()})
                rows.append(row)
        elif series_attrs:
            rows.append(series_attrs)

    return rows


def fetch_one(
    session: requests.Session,
    dataset: str,
    reporter_id: str,
    partner_id: str,
    year: str,
    indicator_code: str,
    indicator_label: str,
    pause_seconds: float,
) -> list[dict[str, str]]:
    url = build_url(dataset, reporter_id, partner_id, year, indicator_code)

    try:
        response = session.get(url, timeout=60)
    except Exception as exc:
        return [
            {
                "year": year,
                "reporter_id": reporter_id,
                "partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": "",
                "obs_value": "",
                "series_count": "0",
                "pull_status": "request_error",
                "http_status": "",
                "error_message": str(exc),
                "source_url": url,
                "product_code": "",
                "partner_code_raw": "",
                "reporter_code_raw": "",
            }
        ]

    if pause_seconds > 0:
        time.sleep(pause_seconds)

    if response.status_code != 200:
        return [
            {
                "year": year,
                "reporter_id": reporter_id,
                "partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": "",
                "obs_value": "",
                "series_count": "0",
                "pull_status": "http_error",
                "http_status": str(response.status_code),
                "error_message": normalize_text(response.text)[:500],
                "source_url": url,
                "product_code": "",
                "partner_code_raw": "",
                "reporter_code_raw": "",
            }
        ]

    try:
        parsed_rows = parse_sdmx_xml(response.text)
    except Exception as exc:
        return [
            {
                "year": year,
                "reporter_id": reporter_id,
                "partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": "",
                "obs_value": "",
                "series_count": "0",
                "pull_status": "parse_error",
                "http_status": str(response.status_code),
                "error_message": str(exc),
                "source_url": url,
                "product_code": "",
                "partner_code_raw": "",
                "reporter_code_raw": "",
            }
        ]

    if not parsed_rows:
        return [
            {
                "year": year,
                "reporter_id": reporter_id,
                "partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": "",
                "obs_value": "",
                "series_count": "0",
                "pull_status": "empty_response",
                "http_status": str(response.status_code),
                "error_message": "",
                "source_url": url,
                "product_code": "",
                "partner_code_raw": "",
                "reporter_code_raw": "",
            }
        ]

    out: list[dict[str, str]] = []
    series_count = str(len(parsed_rows))

    for row in parsed_rows:
        out.append(
            {
                "year": normalize_text(row.get("TIME_PERIOD", year)),
                "reporter_id": reporter_id,
                "partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": normalize_text(row.get("TIME_PERIOD", year)),
                "obs_value": normalize_text(row.get("OBS_VALUE", "")),
                "series_count": series_count,
                "pull_status": "success",
                "http_status": str(response.status_code),
                "error_message": "",
                "source_url": url,
                "product_code": normalize_text(row.get("PRODUCT", row.get("PRODUCTCODE", ""))),
                "partner_code_raw": normalize_text(row.get("PARTNER", "")),
                "reporter_code_raw": normalize_text(row.get("REPORTER", "")),
            }
        )

    return out


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets-file", default="", help="Path to pair_pull_targets.csv")
    parser.add_argument("--out-dir", default="", help="Output directory")
    parser.add_argument("--pause-seconds", type=float, default=0.2, help="Pause between requests")
    args = parser.parse_args()

    targets_file = resolve_path(args.targets_file, DEFAULT_TARGETS_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    targets = pd.read_csv(targets_file, dtype=str, keep_default_na=False)
    for col in targets.columns:
        targets[col] = targets[col].map(normalize_text)

    required = {"year", "reporter_id", "partner_id", "enabled_flag"}
    missing = sorted(required - set(targets.columns))
    if missing:
        raise ValueError(f"Missing required columns in {targets_file.name}: {missing}")

    targets = targets[targets["enabled_flag"].str.lower() == "yes"].copy()
    if targets.empty:
        raise ValueError("No enabled pair pull targets found")

    rows: list[dict[str, str]] = []
    session = requests.Session()
    session.headers.update({"User-Agent": "tariff-tracker-worldwide-pull/1.0"})

    for _, target in targets.iterrows():
        year = normalize_text(target["year"])
        reporter_id = normalize_text(target["reporter_id"]).upper()
        partner_id = normalize_text(target["partner_id"]).upper()

        print(f"Pulling {reporter_id}->{partner_id} for {year}")

        for dataset, indicator_map in INDICATORS.items():
            for indicator_code, indicator_label in indicator_map.items():
                rows.extend(
                    fetch_one(
                        session=session,
                        dataset=dataset,
                        reporter_id=reporter_id,
                        partner_id=partner_id,
                        year=year,
                        indicator_code=indicator_code,
                        indicator_label=indicator_label,
                        pause_seconds=args.pause_seconds,
                    )
                )

    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(rows)
    csv_path = out_dir / "wits_pair_indicators_raw.csv"
    json_path = out_dir / "wits_pair_indicators_raw.json"

    df.to_csv(csv_path, index=False)
    write_json(json_path, df.to_dict(orient="records"))

    ok = int((df["pull_status"] == "success").sum())
    bad = int((df["pull_status"] != "success").sum())

    print(f"Targets pulled: {len(targets)}")
    print(f"Successful rows: {ok}")
    print(f"Non-success rows: {bad}")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {json_path}")


if __name__ == "__main__":
    main()