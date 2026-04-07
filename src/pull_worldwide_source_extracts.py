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


def extract_year_from_batch_id(value: object) -> str:
    text = normalize_text(value)
    match = re.search(r"(19|20)\d{2}$", text)
    return match.group(0) if match else ""


def extract_reporter_from_batch_id(value: object) -> str:
    text = normalize_text(value)
    match = re.match(r"^([A-Z0-9]+)_(19|20)\d{2}$", text)
    return match.group(1) if match else ""


def replace_request_year(url: str, new_year: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["ps"] = [normalize_text(new_year)]
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


def year_fallback_candidates(requested_year: str, fallback_years_back: int, min_year: int) -> list[str]:
    req = int(requested_year)
    lo = max(int(min_year), req - max(int(fallback_years_back), 0))
    return [str(y) for y in range(req, lo - 1, -1)]


def fetch_standardized_with_fallback(
    session: requests.Session,
    row: pd.Series,
    requested_year: str,
    timeout: int,
    fallback_years_back: int,
    min_year: int,
) -> tuple[dict[str, object], dict[str, object]]:
    logical_dataset = normalize_text(row["logical_dataset"])
    batch_id = normalize_text(row["batch_id"])
    base_request_url = normalize_text(row["request_url"])

    attempts: list[dict[str, object]] = []

    for candidate_year in year_fallback_candidates(
        requested_year=requested_year,
        fallback_years_back=fallback_years_back,
        min_year=min_year,
    ):
        candidate_request_url = replace_request_year(base_request_url, candidate_year)

        row_for_year = row.copy()
        row_for_year["request_url"] = candidate_request_url

        final_url = inject_auth(candidate_request_url, row_for_year)
        response = session.get(final_url, timeout=timeout)

        raw_text = response.text.lstrip("\ufeff")
        text = raw_text.strip()
        content_type = normalize_text(response.headers.get("content-type", ""))

        attempt_record = {
            "candidate_year": candidate_year,
            "resolved_url": response.url,
            "status_code": int(response.status_code),
            "content_type": content_type,
        }
        attempts.append(attempt_record)

        if response.status_code == 204 or not text:
            continue

        response.raise_for_status()

        frame = load_response_frame(
            response=response,
            batch_id=batch_id,
            logical_dataset=logical_dataset,
            requested_url=candidate_request_url,
        )
        standardized = standardize_timeseries_row(frame, row_for_year)

        source_year = normalize_text(standardized["year"])
        standardized["source_year"] = source_year
        standardized["year"] = requested_year

        manifest_entry = {
            "logical_dataset": logical_dataset,
            "batch_id": batch_id,
            "requested_year": requested_year,
            "source_year": source_year,
            "resolved_url": response.url,
            "status_code": int(response.status_code),
            "response_row_count": int(len(frame)),
            "standardized_row": standardized,
            "attempts": attempts,
        }
        return standardized, manifest_entry

    raise ValueError(
        f"{batch_id} [{logical_dataset}]: no usable MFN response found for requested_year={requested_year} "
        f"after fallback attempts {', '.join([a['candidate_year'] for a in attempts])}"
    )


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


def load_response_frame(
    response: requests.Response,
    batch_id: str,
    logical_dataset: str,
    requested_url: str,
) -> pd.DataFrame:
    raw_text = response.text.lstrip("\ufeff")
    text = raw_text.strip()
    content_type = normalize_text(response.headers.get("content-type", ""))

    if not text:
        raise ValueError(
            f"{batch_id} [{logical_dataset}]: empty response body "
            f"(status={response.status_code}, content_type={content_type}, "
            f"resolved_url={response.url}, requested_url={requested_url})"
        )

    try:
        if text.startswith("{") or text.startswith("["):
            obj = response.json()
            records = extract_records(obj)
            if not records:
                raise ValueError(
                    f"{batch_id} [{logical_dataset}]: JSON response contained no usable record list "
                    f"(status={response.status_code}, content_type={content_type}, resolved_url={response.url})"
                )
            frame = pd.DataFrame(records)
        else:
            frame = pd.read_csv(StringIO(raw_text), dtype=str, keep_default_na=False)
    except Exception as exc:
        preview = text[:400].replace("\n", " ")
        raise ValueError(
            f"{batch_id} [{logical_dataset}]: failed to parse response "
            f"(status={response.status_code}, content_type={content_type}, "
            f"resolved_url={response.url}, preview={preview})"
        ) from exc

    if frame.empty:
        raise ValueError(
            f"{batch_id} [{logical_dataset}]: parsed response contains no rows "
            f"(status={response.status_code}, content_type={content_type}, resolved_url={response.url})"
        )

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
    parser.add_argument("--year", default="", help="Optional explicit year filter for a multi-year source registry")
    parser.add_argument(
        "--disable-reporters",
        default="",
        help="Optional comma-separated reporter actor_ids to exclude from source pulls",
    )
    parser.add_argument(
        "--fallback-years-back",
        type=int,
        default=5,
        help="How many earlier years to try when the requested MFN year is unavailable",
    )
    parser.add_argument(
        "--min-fallback-year",
        type=int,
        default=1996,
        help="Lower bound for MFN fallback search",
    )
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

    requested_year = normalize_text(args.year)
    enabled["registry_year"] = enabled["batch_id"].map(extract_year_from_batch_id)
    enabled["registry_reporter_id"] = enabled["batch_id"].map(extract_reporter_from_batch_id)

    if requested_year:
        if not requested_year.isdigit() or len(requested_year) != 4:
            raise ValueError(f"--year must be a four-digit year, got: {requested_year}")
        enabled = enabled[enabled["registry_year"] == requested_year].copy()
        if enabled.empty:
            raise ValueError(f"No enabled source pull rows found for year {requested_year}")
    else:
        known_years = sorted({x for x in enabled["registry_year"].tolist() if x})
        if len(known_years) > 1:
            raise ValueError(
                "worldwide_source_pull_registry.csv contains multiple years. "
                "Pass --year to pull one selected year."
            )

    disabled_reporters = {
        normalize_text(x).upper()
        for x in normalize_text(args.disable_reporters).split(",")
        if normalize_text(x)
    }
    if disabled_reporters:
        enabled = enabled[~enabled["registry_reporter_id"].str.upper().isin(disabled_reporters)].copy()
        if enabled.empty:
            raise ValueError(
                f"All enabled source pull rows were removed by --disable-reporters={sorted(disabled_reporters)}"
            )

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
        session = requests.Session()
        timeout = int(float(normalize_text(row["timeout_seconds"]) or "120"))

        logical_dataset = normalize_text(row["logical_dataset"])
        batch_id = normalize_text(row["batch_id"])
        requested_row_year = normalize_text(row.get("registry_year", "")) or extract_year_from_batch_id(row["batch_id"])

        print(f"Pulling {batch_id} [{logical_dataset}]")

        standardized, manifest_entry = fetch_standardized_with_fallback(
            session=session,
            row=row,
            requested_year=requested_row_year,
            timeout=timeout,
            fallback_years_back=args.fallback_years_back,
            min_year=args.min_fallback_year,
        )

        rows_by_dataset[logical_dataset].append(standardized)
        batch_manifest.append(manifest_entry)

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

    for col in ["source_year_simple", "source_year_weighted"]:
        if col not in merged.columns:
            merged[col] = ""

    merged["simple_average_source_year"] = merged["source_year_simple"].map(normalize_text)
    merged["trade_weighted_source_year"] = merged["source_year_weighted"].map(normalize_text)

    merged["source_year_mismatch_flag"] = merged.apply(
        lambda row: (
            "yes"
            if normalize_text(row["simple_average_source_year"])
            and normalize_text(row["trade_weighted_source_year"])
            and normalize_text(row["simple_average_source_year"]) != normalize_text(row["trade_weighted_source_year"])
            else "no"
        ),
        axis=1,
    )

    merged["source_year"] = merged.apply(
        lambda row: (
            normalize_text(row["simple_average_source_year"])
            if normalize_text(row["simple_average_source_year"]) == normalize_text(row["trade_weighted_source_year"])
            else (
                normalize_text(row["trade_weighted_source_year"])
                or normalize_text(row["simple_average_source_year"])
            )
        ),
        axis=1,
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
            "source_year": merged["source_year"].map(normalize_text),
            "simple_average_source_year": merged["simple_average_source_year"].map(normalize_text),
            "trade_weighted_source_year": merged["trade_weighted_source_year"].map(normalize_text),
            "source_year_mismatch_flag": merged["source_year_mismatch_flag"].map(normalize_text),
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