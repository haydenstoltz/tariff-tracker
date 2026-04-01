from __future__ import annotations

import argparse
import csv
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_URLS_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_timeseries_urls.txt"
DEFAULT_CODE_MAP_FILE = ROOT / "data" / "metadata" / "world" / "wto_actor_code_map.csv"
DEFAULT_OUT_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_source_pull_registry.csv"

REQUIRED_CODE_MAP_COLS = [
    "actor_id",
    "wto_partner_code",
    "canonical_name",
]

SUPPORTED_INDICATORS = {
    "TP_A_0010": {
        "logical_dataset": "mfn_simple_average_all_products",
        "output_filename": "mfn_simple_average_latest.csv",
        "indicator_label": "Simple average MFN applied tariff - all products",
    },
    "TP_A_0030": {
        "logical_dataset": "mfn_trade_weighted_all_products",
        "output_filename": "mfn_trade_weighted_latest.csv",
        "indicator_label": "Trade-weighted MFN applied tariff average - all products",
    },
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


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def read_urls_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Missing URL input file: {path}")

    suffix = path.suffix.lower()

    if suffix in {".csv", ".tsv"}:
        sep = "\t" if suffix == ".tsv" else ","
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=sep)
            if not reader.fieldnames:
                raise ValueError(f"URL file has no header row: {path}")

            candidate_cols = ["request_url", "url", "urls"]
            url_col = next((c for c in candidate_cols if c in reader.fieldnames), None)
            if not url_col:
                raise ValueError(
                    f"URL file must contain one of these columns: {candidate_cols}. "
                    f"Found: {reader.fieldnames}"
                )

            urls = [normalize_text(row.get(url_col, "")) for row in reader]
            return [u for u in urls if u]

    with open(path, "r", encoding="utf-8-sig") as f:
        lines = [normalize_text(line) for line in f.readlines()]

    urls = [
        line for line in lines
        if line and not line.startswith("#")
    ]
    return urls


def normalize_wto_code(code: str) -> str:
    text = normalize_text(code)
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return text
    return str(int(digits))


def build_code_lookup(code_map: pd.DataFrame) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    for _, row in code_map.iterrows():
        key = normalize_wto_code(row["wto_partner_code"])
        if not key:
            continue
        lookup[key] = {
            "actor_id": normalize_text(row["actor_id"]),
            "canonical_name": normalize_text(row["canonical_name"]),
            "wto_partner_code": normalize_text(row["wto_partner_code"]),
        }
    return lookup


def strip_subscription_key(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    for key in ["subscription-key", "subscription_key"]:
        if key in params:
            params.pop(key, None)

    clean_query = urlencode(params, doseq=True)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            clean_query,
            parsed.fragment,
        )
    )


def extract_single_param(params: dict[str, list[str]], name: str, url: str) -> str:
    values = params.get(name, [])
    values = [normalize_text(v) for v in values if normalize_text(v)]
    if not values:
        raise ValueError(f"Missing required query parameter '{name}' in URL: {url}")
    if len(values) != 1:
        raise ValueError(f"Expected one '{name}' value in URL, found {values}: {url}")
    return values[0]


def parse_wto_url(url: str, code_lookup: dict[str, dict[str, str]]) -> dict[str, str] | None:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    indicator = extract_single_param(params, "i", url)
    reporter_code = extract_single_param(params, "r", url)
    year = extract_single_param(params, "ps", url)

    if indicator not in SUPPORTED_INDICATORS:
        return None

    reporter_key = normalize_wto_code(reporter_code)
    reporter_info = code_lookup.get(reporter_key)
    if not reporter_info:
        raise ValueError(
            f"Reporter code '{reporter_code}' from URL is not mapped in wto_actor_code_map.csv: {url}"
        )

    meta = SUPPORTED_INDICATORS[indicator]
    clean_url = strip_subscription_key(url)

    actor_id = reporter_info["actor_id"]
    canonical_name = reporter_info["canonical_name"]

    return {
        "logical_dataset": meta["logical_dataset"],
        "batch_id": f"{actor_id}_{year}",
        "provider": "WTO",
        "enabled_flag": "yes",
        "request_url": clean_url,
        "output_filename": meta["output_filename"],
        "auth_location": "query_param",
        "auth_name": "subscription-key",
        "subscription_env_var": "WTO_API_KEY",
        "timeout_seconds": "120",
        "notes": f"{meta['indicator_label']} | reporter={canonical_name} | year={year}",
        "_indicator": indicator,
        "_reporter_actor_id": actor_id,
        "_year": year,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build worldwide_source_pull_registry.csv automatically from a file containing WTO Timeseries URLs. "
            "Supports TP_A_0010 and TP_A_0030 only."
        )
    )
    parser.add_argument(
        "--urls-file",
        default="",
        help="Path to a text/csv/tsv file containing URLs. Default: data/metadata/world/worldwide_timeseries_urls.txt",
    )
    parser.add_argument(
        "--code-map-file",
        default="",
        help="Path to wto_actor_code_map.csv",
    )
    parser.add_argument(
        "--out-file",
        default="",
        help="Path to worldwide_source_pull_registry.csv",
    )
    args = parser.parse_args()

    urls_file = resolve_path(args.urls_file, DEFAULT_URLS_FILE)
    code_map_file = resolve_path(args.code_map_file, DEFAULT_CODE_MAP_FILE)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)

    code_map = pd.read_csv(code_map_file, dtype=str, keep_default_na=False)
    for col in code_map.columns:
        code_map[col] = code_map[col].map(normalize_text)
    require_columns(code_map, REQUIRED_CODE_MAP_COLS, code_map_file.name)

    code_lookup = build_code_lookup(code_map)
    urls = read_urls_file(urls_file)

    if not urls:
        raise ValueError(f"No URLs found in input file: {urls_file}")

    rows: list[dict[str, str]] = []
    skipped: list[str] = []

    for url in urls:
        parsed = parse_wto_url(url, code_lookup)
        if parsed is None:
            skipped.append(url)
            continue
        rows.append(parsed)

    if not rows:
        raise ValueError(
            "No supported WTO URLs found. Supported indicators: "
            + ", ".join(sorted(SUPPORTED_INDICATORS))
        )

    out = pd.DataFrame(rows)

    duplicate_key = out.duplicated(subset=["logical_dataset", "_reporter_actor_id", "_year"], keep=False)
    if duplicate_key.any():
        raise ValueError(
            "Duplicate logical_dataset / reporter / year combinations found:\n"
            + out.loc[duplicate_key, ["logical_dataset", "_reporter_actor_id", "_year", "request_url"]].to_string(index=False)
        )

    out = out.sort_values(
        ["logical_dataset", "_reporter_actor_id", "_year"],
        kind="stable",
    ).reset_index(drop=True)

    final_cols = [
        "logical_dataset",
        "batch_id",
        "provider",
        "enabled_flag",
        "request_url",
        "output_filename",
        "auth_location",
        "auth_name",
        "subscription_env_var",
        "timeout_seconds",
        "notes",
    ]
    out = out[final_cols]

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_file, index=False)

    print(f"Input URLs read: {len(urls)}")
    print(f"Supported rows written: {len(out)}")
    print(f"Registry written: {out_file}")

    if skipped:
        print(f"Skipped unsupported URLs: {len(skipped)}")
        for item in skipped:
            print(f"- {item}")


if __name__ == "__main__":
    main()