from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

INDICATORS = [
    ("tradestats-trade", "MPRT-TRD-VL"),
    ("tradestats-tariff", "AHS-WGHTD-AVRG"),
    ("tradestats-tariff", "AHS-SMPL-AVRG"),
    ("tradestats-tariff", "TRF-NMBR-AGGRMNT"),
]


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


def find_first_text(root: ET.Element, wanted_names: list[str]) -> str:
    wanted = {name.upper() for name in wanted_names}
    for node in root.iter():
        tag = strip_ns(node.tag).upper()
        if tag in wanted:
            text = normalize_text(node.text)
            if text:
                return text
    return ""


def fetch_metadata(dataset: str, indicator_code: str) -> dict[str, str]:
    url = f"https://wits.worldbank.org/API/V1/wits/datasource/{dataset}/indicator/{indicator_code}"
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    root = ET.fromstring(response.text)

    return {
        "dataset": dataset,
        "indicator_code": indicator_code,
        "indicator_name": find_first_text(root, ["IndicatorName", "Name", "INDICATORNAME"]),
        "is_partner_required": find_first_text(root, ["IsPartnerRequired", "PARTNERREQUIRED", "ISPARTNERREQUIRED"]),
        "sdmx_partner_value": find_first_text(root, ["SDMXPartnerValue", "PARTNERSDMXVALUE", "SDMXPARTNERVALUE"]),
        "is_product_required": find_first_text(root, ["IsProductRequired", "PRODUCTREQUIRED", "ISPRODUCTREQUIRED"]),
        "sdmx_product_value": find_first_text(root, ["SDMXProductValue", "PRODUCTSDMXVALUE", "SDMXPRODUCTVALUE"]),
        "definition": find_first_text(root, ["IndicatorDefinition", "Definition", "INDICATORDEFINITION"]),
        "topic": find_first_text(root, ["Topic"]),
        "source": find_first_text(root, ["Source"]),
        "notes": find_first_text(root, ["Notes", "Note"]),
        "metadata_url": url,
    }


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="", help="Output directory")
    args = parser.parse_args()

    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for dataset, indicator_code in INDICATORS:
        print(f"Pulling metadata for {dataset} / {indicator_code}")
        rows.append(fetch_metadata(dataset, indicator_code))

    df = pd.DataFrame(rows)
    csv_path = out_dir / "wits_indicator_metadata.csv"
    json_path = out_dir / "wits_indicator_metadata.json"

    df.to_csv(csv_path, index=False)
    write_json(json_path, df.to_dict(orient="records"))

    print(f"Wrote: {csv_path}")
    print(f"Wrote: {json_path}")


if __name__ == "__main__":
    main()