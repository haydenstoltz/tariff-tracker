from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_REGISTRY_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_source_pull_registry.csv"
DEFAULT_OUT_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox"
DEFAULT_MANIFEST_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "source_pull_manifest.json"

REQUIRED_REGISTRY_COLS = [
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

SUPPORTED_LOGICAL_DATASETS = {
    "mfn_simple_average_all_products": {
        "value_field": "simple_average",
        "debug_filename": "mfn_simple_average_latest.csv",
    },
    "mfn_trade_weighted_all_products": {
        "value_field": "trade_weighted",
        "debug_filename": "mfn_trade_weighted_latest.csv",
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


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def normalize_code(value: object) -> str:
    text = normalize_text(value)
    digits = "".join(ch for ch in text if ch.isdigit())
    return str(int(digits)) if digits else text


def normalize_year(value: object) -> str:
    text = normalize_text(value)
    match = re.search(r"(19|20)\d{2}", text)
    return match.group(0) if match else text


def normalize_colname(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())


def inject_auth(url: str, row: pd.Series) -> str:
    auth_location = normalize_text(row["auth_location"]).lower()
    auth_name = normalize_text(row["auth_name"])
    env_var = normalize_text(row["subscription_env_var"]) or "WTO_API_KEY"

    if auth_location in {"", "none"}:
        return url

    if auth_location != "query_param":
        raise ValueError(
            f"{row['batch_id']}: unsupported auth_location '{row['auth_location']}'. "
            "Use query_param for WTO Timeseries URLs."
        )

    if not auth_name:
        raise ValueError(f"{row['batch_id']}: auth_name is blank")

    key = os.getenv(env_var, "").strip()
    if not key:
        raise ValueError(f"{row['batch_id']}: missing required environment variable {env_var}")

    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params[auth_name] = [key]
    query = urlencode(params, doseq=True)

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            query,
            parsed.fragment,
        )
    )


def extract_records(obj: object) -> list[dict] | None:
    if isinstance(obj, list):
        if obj and all(isinstance(item, dict) for item in obj):
            return obj
        for item in obj:
            found = extract_records(item)
            if found:
                return found
        return None

    if isinstance(obj, dict):
        for key in ["Dataset", "dataset", "data", "Data", "value", "Value", "results", "Results", "items", "Items"]:
            if key in obj:
                found = extract_records(obj[key])
                if found:
                    return found
        for value in obj.values():
            found = extract_records(value)
            if found:
                return found
        return None

    return None


def load_response_frame(response: requests.Response) -> pd.DataFrame:
    text = response.text.lstrip("\ufeff").strip()
    if not text:
        raise ValueError("Empty response body")

    if text.startswith("{") or text.startswith("["):
        obj = response.json()
        records = extract_records(obj)
        if not records:
            raise ValueError("JSON response did not contain a usable record list")
        frame = pd.DataFrame(records)
    else:
        frame = pd.read_csv(StringIO(response.text), dtype=str, keep_default_na=False)

    if frame.empty:
        raise ValueError("Parsed response contains no rows")

    for col in frame.columns:
        frame[col] = frame[col].map(normalize_text)

    return frame


def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {normalize_colname(col): col for col in df.columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return None


def standardize_timeseries_row(frame: pd.DataFrame, row: pd.Series) -> dict[str, object]:
    expected_url = normalize_text(row["request_url"])
    params = parse_qs(urlparse(expected_url).query, keep_blank_values=True)

    expected_reporter_code = normalize_text(params.get("r", [""])[0])
    expected_year = normalize_text(params.get("ps", [""])[0])

    if not expected_reporter_code or not expected_year:
        raise ValueError(
            f"{row['batch_id']}: request_url must include single reporter code 'r' and single year 'ps'"
        )

    reporter_code_col = find_column(
        frame,
        [
            "reportingeconomycode",
            "reportingeconomy_code",
            "reportercode",
            "reporter_code",
            "economycode",
        ],
    )
    reporter_name_col = find_column(
        frame,
        [
            "reportingeconomylabel",
            "reportingeconomy",
            "reportername",
            "reporter_name",
            "economylabel",
            "economyname",
        ],
    )
    year_col = find_column(
        frame,
        [
            "year",
            "period",
            "timeperiod",
            "time_period",
            "calendaryear",
        ],
    )
    value_col = find_column(
        frame,
        [
            "value",
            "obsvalue",
            "observationvalue",
            "seriesvalue",
        ],
    )

    missing = [
        name for name, col in [
            ("reporter_code", reporter_code_col),
            ("year", year_col),
            ("value", value_col),
        ] if col is None
    ]
    if missing:
        raise ValueError(
            f"{row['batch_id']}: response missing required columns {missing}. "
            f"Actual columns: {list(frame.columns)}"
        )

    work = pd.DataFrame(
        {
            "reporter_code_raw": frame[reporter_code_col].map(normalize_text),
            "reporter_name": frame[reporter_name_col].map(normalize_text) if reporter_name_col else "",
            "year_raw": frame[year_col].map(normalize_text),
            "value_raw": frame[value_col].map(normalize_text),
        }
    )

    work["reporter_code_norm"] = work["reporter_code_raw"].map(normalize_code)
    work["year_norm"] = work["year_raw"].map(normalize_year)
    work["value_num"] = pd.to_numeric(work["value_raw"], errors="coerce")

    expected_code_norm = normalize_code(expected_reporter_code)
    expected_year_norm = normalize_year(expected_year)

    work = work[
        (work["reporter_code_norm"] == expected_code_norm)
        & (work["year_norm"] == expected_year_norm)
        & (work["value_num"].notna())
    ].copy()

    if work.empty:
        raise ValueError(
            f"{row['batch_id']}: no usable rows matched reporter={expected_reporter_code} year={expected_year}"
        )

    work = work.drop_duplicates(subset=["reporter_code_norm", "year_norm", "value_num"]).copy()
    if len(work) != 1:
        raise ValueError(
            f"{row['batch_id']}: expected exactly one reporter-year value, found {len(work)} rows"
        )

    single = work.iloc[0]
    reporter_name = normalize_text(single["reporter_name"]) or expected_reporter_code

    value_field = SUPPORTED_LOGICAL_DATASETS[normalize_text(row["logical_dataset"])]["value_field"]

    return {
        "reporter_code": expected_reporter_code,
        "reporter_name": reporter_name,
        "year": expected_year_norm,
        value_field: round(float(single["value_num"]), 6),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Pull WTO Timeseries MFN source extracts from the URL registry and build a canonical "
            "mfn_applied_total_latest.csv file in the raw inbox."
        )
    )
    parser.add_argument("--registry-file", default="", help="Path to worldwide_source_pull_registry.csv")
    parser.add_argument("--out-dir", default="", help="Raw inbox output directory")
    parser.add_argument("--manifest-file", default="", help="Path to source pull manifest JSON")
    args = parser.parse_args()

    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)
    manifest_file = resolve_path(args.manifest_file, DEFAULT_MANIFEST_FILE)

    registry = pd.read_csv(registry_file, dtype=str, keep_default_na=False)
    require_columns(registry, REQUIRED_REGISTRY_COLS, registry_file.name)

    for col in registry.columns:
        registry[col] = registry[col].map(normalize_text)

    enabled = registry[registry["enabled_flag"].str.lower() == "yes"].copy()
    if enabled.empty:
        raise ValueError("No enabled source pull rows found in worldwide_source_pull_registry.csv")

    unsupported = enabled[~enabled["logical_dataset"].isin(SUPPORTED_LOGICAL_DATASETS.keys())]
    if not unsupported.empty:
        raise ValueError(
            "Unsupported logical_dataset values found in source pull registry:\n"
            + unsupported[["logical_dataset", "batch_id"]].to_string(index=False)
        )

    out_dir.mkdir(parents=True, exist_ok=True)

    rows_by_dataset: dict[str, list[dict[str, object]]] = {k: [] for k in SUPPORTED_LOGICAL_DATASETS}
    batch_manifest: list[dict[str, object]] = []

    for _, row in enabled.iterrows():
        final_url = inject_auth(normalize_text(row["request_url"]), row)
        timeout = int(float(normalize_text(row["timeout_seconds"]) or "120"))

        response = requests.get(final_url, timeout=timeout)
        response.raise_for_status()

        frame = load_response_frame(response)
        standardized = standardize_timeseries_row(frame, row)

        logical_dataset = normalize_text(row["logical_dataset"])
        rows_by_dataset[logical_dataset].append(standardized)

        batch_manifest.append(
            {
                "logical_dataset": logical_dataset,
                "batch_id": normalize_text(row["batch_id"]),
                "resolved_url": response.url,
                "status_code": response.status_code,
                "response_row_count": int(len(frame)),
                "standardized_row": standardized,
            }
        )

    output_names: dict[str, str] = {}
    for logical_dataset in SUPPORTED_LOGICAL_DATASETS:
        candidates = enabled.loc[
            enabled["logical_dataset"] == logical_dataset,
            "output_filename",
        ].drop_duplicates().tolist()
        if len(candidates) != 1:
            raise ValueError(
                f"{logical_dataset}: expected exactly one output_filename in registry, found {candidates}"
            )
        output_names[logical_dataset] = candidates[0]

    simple_df = pd.DataFrame(rows_by_dataset["mfn_simple_average_all_products"])
    weighted_df = pd.DataFrame(rows_by_dataset["mfn_trade_weighted_all_products"])

    if simple_df.empty or weighted_df.empty:
        raise ValueError("Both simple-average and trade-weighted MFN datasets are required")

    if simple_df.duplicated(subset=["reporter_code", "year"]).any():
        raise ValueError("Duplicate reporter/year rows found in simple-average MFN pulls")
    if weighted_df.duplicated(subset=["reporter_code", "year"]).any():
        raise ValueError("Duplicate reporter/year rows found in trade-weighted MFN pulls")

    simple_out = out_dir / output_names["mfn_simple_average_all_products"]
    weighted_out = out_dir / output_names["mfn_trade_weighted_all_products"]

    simple_df = simple_df.sort_values(["year", "reporter_code"], kind="stable").reset_index(drop=True)
    weighted_df = weighted_df.sort_values(["year", "reporter_code"], kind="stable").reset_index(drop=True)

    simple_df.to_csv(simple_out, index=False)
    weighted_df.to_csv(weighted_out, index=False)

    merged = simple_df.merge(
        weighted_df,
        on=["reporter_code", "year"],
        how="outer",
        suffixes=("_simple", "_weighted"),
        validate="one_to_one",
    )

    merged["reporter_name"] = merged["reporter_name_simple"].where(
        merged["reporter_name_simple"].astype(str).str.strip() != "",
        merged["reporter_name_weighted"],
    )

    missing_simple = merged[merged["simple_average"].isna()]
    missing_weighted = merged[merged["trade_weighted"].isna()]
    if not missing_simple.empty or not missing_weighted.empty:
        raise ValueError(
            "MFN merge produced missing simple/trade-weighted values. "
            "Check that both URL sets cover the same reporters and year."
        )

    canonical = pd.DataFrame(
        {
            "reporter_name": merged["reporter_name"].map(normalize_text),
            "reporter_code": merged["reporter_code"].map(normalize_text),
            "year": merged["year"].map(normalize_text),
            "classification": "",
            "classification_version": "",
            "duty_scheme_code": "MFN",
            "duty_scheme_name": "MFN applied",
            "product_code": "TOTAL",
            "mtn_categories": "All products",
            "simple_average": merged["simple_average"],
            "trade_weighted": merged["trade_weighted"],
            "duty_free_share": "",
        }
    )

    canonical = canonical.sort_values(["year", "reporter_code"], kind="stable").reset_index(drop=True)

    canonical_out = out_dir / "mfn_applied_total_latest.csv"
    canonical.to_csv(canonical_out, index=False)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "registry_file": str(registry_file),
        "output_dir": str(out_dir),
        "batch_count": int(len(batch_manifest)),
        "batches": batch_manifest,
        "simple_average_rows": int(len(simple_df)),
        "trade_weighted_rows": int(len(weighted_df)),
        "canonical_mfn_rows": int(len(canonical)),
        "canonical_mfn_file": str(canonical_out),
    }
    write_json(manifest_file, manifest)

    print(f"Wrote: {simple_out}")
    print(f"Wrote: {weighted_out}")
    print(f"Wrote: {canonical_out}")
    print(f"Wrote: {manifest_file}")


if __name__ == "__main__":
    main()