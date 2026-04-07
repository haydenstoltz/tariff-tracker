from __future__ import annotations

import argparse
import io
import json
import math
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_BASE_URL = "https://api.wto.org/timeseries/v1"
DEFAULT_REGISTRY_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"
DEFAULT_BATCH_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "imports_batches"
DEFAULT_MANIFEST_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "imports_api_pull_manifest.json"
DEFAULT_MERGED_OUT_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "imports_by_partner_api_latest.csv"

REQUIRED_REGISTRY_COLS = [
    "year",
    "batch_id",
    "reporter_id",
    "reporter_name",
    "wto_reporter_code",
    "expected_batch_filename",
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


def normalize_code3(value: object) -> str:
    text = normalize_text(value)
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits:
        return digits.zfill(3)
    return text.upper()


def normalize_colname(value: str) -> str:
    return "".join(ch for ch in value.lower().strip() if ch.isalnum())


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def build_session(api_key_env: str) -> requests.Session:
    api_key = os.getenv(api_key_env, "").strip()
    if not api_key:
        raise ValueError(f"Missing required environment variable: {api_key_env}")

    session = requests.Session()
    session.headers.update(
        {
            "Ocp-Apim-Subscription-Key": api_key,
            "User-Agent": "tariff-tracker-wto-import-pull/1.0",
        }
    )
    return session


def load_registry(registry_file: Path, year: str, reporters_filter: set[str]) -> pd.DataFrame:
    registry = pd.read_csv(registry_file, dtype=str, keep_default_na=False)
    for col in registry.columns:
        registry[col] = registry[col].map(normalize_text)

    require_columns(registry, REQUIRED_REGISTRY_COLS, registry_file.name)

    registry = registry[registry["year"] == year].copy()
    if reporters_filter:
        registry = registry[registry["reporter_id"].str.upper().isin(reporters_filter)].copy()

    if registry.empty:
        raise ValueError(f"No registry rows found for year={year} and reporters filter={sorted(reporters_filter)}")

    registry["wto_reporter_code_3"] = registry["wto_reporter_code"].map(normalize_code3)
    registry = registry.sort_values(["reporter_id"], kind="stable").reset_index(drop=True)
    return registry


def api_data_count(
    session: requests.Session,
    base_url: str,
    indicator_code: str,
    reporter_code: str,
    year: str,
    pc: str,
    timeout: int,
) -> int:
    url = f"{base_url.rstrip('/')}/data_count"
    params = {
        "i": indicator_code,
        "r": reporter_code,
        "p": "all",
        "ps": year,
        "pc": pc,
        "spc": "false",
    }
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, int):
        return payload
    if isinstance(payload, float):
        return int(payload)
    if isinstance(payload, str) and payload.strip().isdigit():
        return int(payload.strip())

    raise ValueError(f"Unexpected /data_count payload for reporter {reporter_code}: {payload}")


def parse_zip_csv_response(content: bytes, label: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        csv_names = [name for name in zf.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"{label}: zip response contained no CSV file")
        if len(csv_names) > 1:
            csv_names = sorted(csv_names)
        with zf.open(csv_names[0]) as fh:
            raw = fh.read()

    frame = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False, encoding="utf-8-sig")
    for col in frame.columns:
        frame[col] = frame[col].map(normalize_text)
    return frame


def fetch_data_page(
    session: requests.Session,
    base_url: str,
    indicator_code: str,
    reporter_code: str,
    year: str,
    pc: str,
    offset: int,
    max_rows: int,
    timeout: int,
) -> pd.DataFrame:
    url = f"{base_url.rstrip('/')}/data"
    payload = {
        "i": indicator_code,
        "r": reporter_code,
        "p": "all",
        "ps": year,
        "pc": pc,
        "spc": False,
        "fmt": "csv",
        "mode": "full",
        "head": "M",
        "lang": 1,
        "meta": False,
        "off": offset,
        "max": max_rows,
    }

    response = session.post(url, json=payload, timeout=timeout)
    response.raise_for_status()

    content_type = normalize_text(response.headers.get("content-type", "")).lower()
    if "application/json" in content_type:
        preview = response.text[:500].replace("\n", " ")
        raise ValueError(f"{reporter_code}: expected zip/csv response, got JSON: {preview}")

    return parse_zip_csv_response(response.content, label=f"reporter={reporter_code} offset={offset}")


def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {normalize_colname(col): col for col in df.columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return None


def format_numeric_string(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    try:
        number = float(text)
    except ValueError:
        return text

    if math.isfinite(number) and number.is_integer():
        return str(int(number))

    out = f"{number:.12f}".rstrip("0").rstrip(".")
    return out if out else "0"


def canonicalize_product_code(product_code: str, product_name: str) -> str:
    code = normalize_text(product_code).upper()
    name = normalize_text(product_name).lower()

    if code in {"", "0", "000", "ALL", "TOT", "TOTAL"}:
        return "TOTAL"
    if "all products" in name or "total merchandise" in name or "all merchandise" in name:
        return "TOTAL"
    return code or "TOTAL"


def canonicalize_product_name(product_name: str) -> str:
    name = normalize_text(product_name)
    if not name:
        return "All products"
    return name


def standardize_frame(frame: pd.DataFrame, expected_reporter_code: str, expected_year: str) -> pd.DataFrame:
    reporter_code_col = find_column(
        frame,
        ["reportingeconomycode", "reportercode", "reportingeconomy_code", "economycode"],
    )
    reporter_name_col = find_column(
        frame,
        ["reportingeconomy", "reportingeconomylabel", "reportername", "economylabel", "economyname"],
    )
    partner_code_col = find_column(
        frame,
        ["partnereconomycode", "partnercode", "partnereconomy_code"],
    )
    partner_name_col = find_column(
        frame,
        ["partnereconomy", "partnereconomylabel", "partnername"],
    )
    year_col = find_column(
        frame,
        ["year", "period", "timeperiod", "time_period", "calendaryear"],
    )
    classification_code_col = find_column(
        frame,
        [
            "productorsectorclassificationcode",
            "productsectorclassificationcode",
            "productclassificationcode",
        ],
    )
    classification_label_col = find_column(
        frame,
        [
            "productorsectorclassification",
            "productsectorclassification",
            "productclassification",
        ],
    )
    product_code_col = find_column(
        frame,
        ["productorsectorcode", "productcode", "sectorcode"],
    )
    product_name_col = find_column(
        frame,
        ["productorsector", "productsector", "product", "sector"],
    )
    value_col = find_column(
        frame,
        ["value", "obsvalue", "observationvalue", "seriesvalue", "textvalue"],
    )

    missing = [
        name for name, col in [
            ("reporter_code", reporter_code_col),
            ("partner_code", partner_code_col),
            ("year", year_col),
            ("value", value_col),
        ]
        if col is None
    ]
    if missing:
        raise ValueError(f"Missing required WTO columns in API page: {missing}. Actual columns: {list(frame.columns)}")

    work = pd.DataFrame(
        {
            "reporter_name": frame[reporter_name_col].map(normalize_text) if reporter_name_col else "",
            "reporter_code": frame[reporter_code_col].map(normalize_code3),
            "year": frame[year_col].map(normalize_text),
            "classification": (
                frame[classification_code_col].map(normalize_text)
                if classification_code_col
                else (frame[classification_label_col].map(normalize_text) if classification_label_col else "")
            ),
            "classification_version": "",
            "product_code_raw": frame[product_code_col].map(normalize_text) if product_code_col else "",
            "product_name_raw": frame[product_name_col].map(normalize_text) if product_name_col else "",
            "partner_code": frame[partner_code_col].map(normalize_code3),
            "partner_name": frame[partner_name_col].map(normalize_text) if partner_name_col else "",
            "value": frame[value_col].map(format_numeric_string),
        }
    )

    work = work[
        (work["reporter_code"] == normalize_code3(expected_reporter_code))
        & (work["year"] == normalize_text(expected_year))
    ].copy()

    if work.empty:
        raise ValueError(
            f"No usable rows remained after filtering to reporter={expected_reporter_code} year={expected_year}"
        )

    work["product_code"] = work.apply(
        lambda row: canonicalize_product_code(row["product_code_raw"], row["product_name_raw"]),
        axis=1,
    )
    work["mtn_categories"] = work["product_name_raw"].map(canonicalize_product_name)

    work = work[work["partner_code"] != ""].copy()
    work = work[work["value"] != ""].copy()

    work = work[
        [
            "reporter_name",
            "reporter_code",
            "year",
            "classification",
            "classification_version",
            "product_code",
            "mtn_categories",
            "partner_code",
            "partner_name",
            "value",
        ]
    ].drop_duplicates().reset_index(drop=True)

    if work.empty:
        raise ValueError(
            f"No partner/value rows remained after standardization for reporter={expected_reporter_code} year={expected_year}"
        )

    return work


def maybe_filter_total_rows(df: pd.DataFrame, total_only: bool) -> pd.DataFrame:
    if not total_only or df.empty:
        return df

    total_mask = (
        df["product_code"].str.upper().eq("TOTAL")
        | df["mtn_categories"].str.lower().eq("all products")
    )

    if total_mask.any():
        return df[total_mask].copy().reset_index(drop=True)

    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Pull partner-aware bilateral imports directly from the WTO Timeseries API and write "
            "canonical per-reporter import batch CSVs compatible with the existing worldwide pipeline."
        )
    )
    parser.add_argument("--indicator-code", required=True, help="WTO indicator code for bilateral imports by partner")
    parser.add_argument("--year", default="2023", help="Target year")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="WTO Timeseries API base URL")
    parser.add_argument("--api-key-env", default="WTO_API_KEY", help="Environment variable holding the WTO API key")
    parser.add_argument("--registry-file", default="", help="Path to worldwide_import_batch_registry.csv")
    parser.add_argument("--batch-dir", default="", help="Output imports_batches directory")
    parser.add_argument("--manifest-file", default="", help="Output manifest JSON path")
    parser.add_argument("--merged-out-file", default="", help="Optional merged CSV output path")
    parser.add_argument("--pc", default="default", help="Product filter passed to WTO API")
    parser.add_argument("--max-per-request", type=int, default=200000, help="WTO API max rows per request")
    parser.add_argument("--timeout-seconds", type=int, default=240, help="HTTP timeout seconds")
    parser.add_argument(
        "--reporters",
        default="",
        help="Optional comma-separated reporter_ids subset, e.g. BRA,IND,SAU",
    )
    parser.add_argument("--skip-existing", action="store_true", help="Skip reporters whose canonical batch file already exists")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue processing other reporters if one reporter fails")
    parser.add_argument("--total-only", action="store_true", help="Keep only TOTAL / All products rows when present")
    args = parser.parse_args()

    if not args.year.isdigit() or len(args.year) != 4:
        raise ValueError(f"--year must be a four-digit year, got: {args.year}")

    if args.max_per_request < 1 or args.max_per_request > 1_000_000:
        raise ValueError("--max-per-request must be between 1 and 1,000,000")

    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    batch_dir = resolve_path(args.batch_dir, DEFAULT_BATCH_DIR)
    manifest_file = resolve_path(args.manifest_file, DEFAULT_MANIFEST_FILE)
    merged_out_file = resolve_path(args.merged_out_file, DEFAULT_MERGED_OUT_FILE)

    reporters_filter = {
        normalize_text(x).upper()
        for x in args.reporters.split(",")
        if normalize_text(x)
    }

    registry = load_registry(registry_file, year=args.year, reporters_filter=reporters_filter)
    batch_dir.mkdir(parents=True, exist_ok=True)
    merged_out_file.parent.mkdir(parents=True, exist_ok=True)

    session = build_session(args.api_key_env)

    reporter_results: list[dict[str, object]] = []
    merged_parts: list[pd.DataFrame] = []

    for _, row in registry.iterrows():
        reporter_id = normalize_text(row["reporter_id"])
        reporter_name = normalize_text(row["reporter_name"])
        reporter_code_3 = normalize_text(row["wto_reporter_code_3"])
        out_path = batch_dir / normalize_text(row["expected_batch_filename"])

        result: dict[str, object] = {
            "reporter_id": reporter_id,
            "reporter_name": reporter_name,
            "reporter_code": reporter_code_3,
            "output_file": str(out_path),
            "status": "started",
        }

        try:
            if args.skip_existing and out_path.exists():
                result["status"] = "skipped_existing"
                reporter_results.append(result)
                continue

            total_count = api_data_count(
                session=session,
                base_url=args.base_url,
                indicator_code=args.indicator_code,
                reporter_code=reporter_code_3,
                year=args.year,
                pc=args.pc,
                timeout=args.timeout_seconds,
            )
            result["data_count"] = int(total_count)

            if total_count <= 0:
                raise ValueError(
                    f"WTO API returned data_count=0 for reporter={reporter_id} code={reporter_code_3}"
                )

            page_offsets = list(range(0, total_count, args.max_per_request))
            page_frames: list[pd.DataFrame] = []

            for offset in page_offsets:
                page = fetch_data_page(
                    session=session,
                    base_url=args.base_url,
                    indicator_code=args.indicator_code,
                    reporter_code=reporter_code_3,
                    year=args.year,
                    pc=args.pc,
                    offset=offset,
                    max_rows=args.max_per_request,
                    timeout=args.timeout_seconds,
                )
                standardized = standardize_frame(
                    page,
                    expected_reporter_code=reporter_code_3,
                    expected_year=args.year,
                )
                page_frames.append(standardized)

            reporter_df = pd.concat(page_frames, ignore_index=True)
            reporter_df = maybe_filter_total_rows(reporter_df, total_only=args.total_only)
            reporter_df = reporter_df.drop_duplicates().sort_values(
                ["year", "partner_code", "product_code"],
                kind="stable",
            ).reset_index(drop=True)

            if reporter_df.empty:
                raise ValueError(f"No rows remained after pull/standardization for reporter={reporter_id}")

            reporter_df.to_csv(out_path, index=False)
            merged_parts.append(reporter_df)

            result["status"] = "ok"
            result["page_count"] = len(page_offsets)
            result["row_count_written"] = int(len(reporter_df))
            reporter_results.append(result)
            print(f"Pulled {reporter_id} ({reporter_code_3}) -> {len(reporter_df)} rows -> {out_path}")

        except Exception as exc:
            result["status"] = "error"
            result["error"] = str(exc)
            reporter_results.append(result)
            print(f"FAILED {reporter_id} ({reporter_code_3}): {exc}")
            if not args.continue_on_error:
                manifest = {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "indicator_code": args.indicator_code,
                    "year": args.year,
                    "pc": args.pc,
                    "reporter_count_requested": int(len(registry)),
                    "reporters": reporter_results,
                }
                write_json(manifest_file, manifest)
                raise

    merged_row_count = 0
    if merged_parts:
        merged_df = pd.concat(merged_parts, ignore_index=True).drop_duplicates().reset_index(drop=True)
        merged_df.to_csv(merged_out_file, index=False)
        merged_row_count = int(len(merged_df))
        print(f"Wrote merged API imports file: {merged_out_file}")

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "indicator_code": args.indicator_code,
        "year": args.year,
        "pc": args.pc,
        "reporter_count_requested": int(len(registry)),
        "reporter_count_ok": int(sum(1 for row in reporter_results if row.get("status") == "ok")),
        "reporter_count_error": int(sum(1 for row in reporter_results if row.get("status") == "error")),
        "reporter_count_skipped_existing": int(sum(1 for row in reporter_results if row.get("status") == "skipped_existing")),
        "merged_output_file": str(merged_out_file),
        "merged_row_count": merged_row_count,
        "reporters": reporter_results,
    }
    write_json(manifest_file, manifest)

    print(f"Wrote: {manifest_file}")


if __name__ == "__main__":
    main()