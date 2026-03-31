from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
import requests
from pandas.tseries.offsets import MonthEnd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_META_FILE = ROOT / "outputs" / "spec_preview" / "product_case_studies.csv"
DEFAULT_SEED_PRICES_FILE = ROOT / "data" / "processed" / "prices_clean.csv"
DEFAULT_CACHE_FILE = ROOT / "data" / "processed" / "case_price_cache.csv"
BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


def resolve_path(path_str: str, default_path: Path) -> Path:
    if not path_str.strip():
        return default_path
    path = Path(path_str)
    if not path.is_absolute():
        path = ROOT / path
    return path


def normalize_prices(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "series_id" not in df.columns or "date" not in df.columns:
        raise ValueError("Price data must contain 'series_id' and 'date' columns")

    if "level" not in df.columns:
        if "value" in df.columns:
            df = df.rename(columns={"value": "level"})
        else:
            raise ValueError("Price data must contain either 'level' or 'value'")

    df["series_id"] = df["series_id"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["level"] = pd.to_numeric(df["level"], errors="coerce")

    df = df.dropna(subset=["series_id", "date", "level"]).copy()
    df = df[["series_id", "date", "level"]]
    df = df.drop_duplicates(subset=["series_id", "date"], keep="last")
    df = df.sort_values(["series_id", "date"]).reset_index(drop=True)
    return df


def load_optional_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["series_id", "date", "level"])
    return normalize_prices(pd.read_csv(path))


def fetch_bls_chunk(series_ids: list[str], start_year: int, end_year: int, timeout: int) -> pd.DataFrame:
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }

    r = requests.post(BLS_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    if data.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS request failed: {data}")

    rows = []
    returned_ids = []

    for series in data["Results"]["series"]:
        sid = series["seriesID"]
        returned_ids.append(sid)

        for item in series["data"]:
            period = item["period"]
            if not period.startswith("M") or period == "M13":
                continue

            try:
                value = float(item["value"])
            except (TypeError, ValueError):
                continue

            date = pd.to_datetime(
                {
                    "year": [int(item["year"])],
                    "month": [int(period[1:])],
                    "day": [1],
                }
            )[0] + MonthEnd(0)

            rows.append(
                {
                    "series_id": sid,
                    "date": date,
                    "level": value,
                }
            )

    missing = sorted(set(series_ids) - set(returned_ids))
    if missing:
        raise RuntimeError(f"BLS did not return requested series: {missing}")

    if not rows:
        raise RuntimeError(f"BLS returned no usable monthly observations for series: {series_ids}")

    return normalize_prices(pd.DataFrame(rows))


def chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def fetch_with_retries(
    series_ids: list[str],
    start_year: int,
    end_year: int,
    chunk_size: int,
    timeout: int,
    max_retries: int,
) -> pd.DataFrame:
    all_parts = []

    for chunk in chunked(series_ids, chunk_size):
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                print(f"Fetching chunk {chunk} for {start_year}-{end_year} (attempt {attempt}/{max_retries})")
                part = fetch_bls_chunk(chunk, start_year, end_year, timeout=timeout)
                all_parts.append(part)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt < max_retries:
                    sleep_seconds = 2 * attempt
                    print(f"Chunk failed: {exc}")
                    print(f"Retrying in {sleep_seconds} seconds...")
                    time.sleep(sleep_seconds)

        if last_error is not None:
            raise RuntimeError(
                f"Failed to fetch BLS chunk {chunk} for {start_year}-{end_year}: {last_error}"
            ) from last_error

    if not all_parts:
        return pd.DataFrame(columns=["series_id", "date", "level"])

    out = pd.concat(all_parts, ignore_index=True)
    out = out.drop_duplicates(subset=["series_id", "date"], keep="last")
    out = out.sort_values(["series_id", "date"]).reset_index(drop=True)
    return out


def build_requirements(meta: pd.DataFrame) -> pd.DataFrame:
    req = (
        meta.groupby("series_id", as_index=False)
        .agg(required_start=("window_start", "min"), required_end=("window_end", "max"))
        .sort_values("series_id")
        .reset_index(drop=True)
    )
    req["start_year"] = req["required_start"].dt.year.astype(int)
    req["end_year"] = req["required_end"].dt.year.astype(int)
    return req


def series_coverage_ok(
    df: pd.DataFrame,
    series_id: str,
    required_start: pd.Timestamp,
    required_end: pd.Timestamp,
) -> bool:
    g = df[df["series_id"] == series_id].copy()
    if g.empty:
        return False

    g["date"] = pd.to_datetime(g["date"], errors="coerce")
    g = g.dropna(subset=["date"])
    if g.empty:
        return False

    min_month = g["date"].min().to_period("M")
    max_month = g["date"].max().to_period("M")
    need_start = pd.Timestamp(required_start).to_period("M")
    need_end = pd.Timestamp(required_end).to_period("M")

    return min_month <= need_start and max_month >= need_end


def fetch_missing_by_requirement_group(
    missing_requirements: pd.DataFrame,
    chunk_size: int,
    timeout: int,
    max_retries: int,
) -> pd.DataFrame:
    all_parts = []

    grouped = missing_requirements.groupby(["start_year", "end_year"], dropna=False)

    for (start_year, end_year), group in grouped:
        series_ids = sorted(group["series_id"].tolist())
        print(
            f"Fetching requirement group {start_year}-{end_year} for {len(series_ids)} series: {series_ids}"
        )
        part = fetch_with_retries(
            series_ids=series_ids,
            start_year=int(start_year),
            end_year=int(end_year),
            chunk_size=chunk_size,
            timeout=timeout,
            max_retries=max_retries,
        )
        all_parts.append(part)

    if not all_parts:
        return pd.DataFrame(columns=["series_id", "date", "level"])

    out = pd.concat(all_parts, ignore_index=True)
    out = out.drop_duplicates(subset=["series_id", "date"], keep="last")
    out = out.sort_values(["series_id", "date"]).reset_index(drop=True)
    return out


def coverage_message(
    df: pd.DataFrame,
    series_id: str,
    required_start: pd.Timestamp,
    required_end: pd.Timestamp,
) -> str:
    g = df[df["series_id"] == series_id].copy()
    if g.empty:
        return f"{series_id} [{required_start.strftime('%Y-%m')} to {required_end.strftime('%Y-%m')}] no rows"

    g["date"] = pd.to_datetime(g["date"], errors="coerce")
    g = g.dropna(subset=["date"])
    if g.empty:
        return f"{series_id} [{required_start.strftime('%Y-%m')} to {required_end.strftime('%Y-%m')}] invalid dates"

    actual_start = g["date"].min().strftime("%Y-%m")
    actual_end = g["date"].max().strftime("%Y-%m")
    need_start = pd.Timestamp(required_start).strftime("%Y-%m")
    need_end = pd.Timestamp(required_end).strftime("%Y-%m")

    return f"{series_id} need {need_start} to {need_end}, have {actual_start} to {actual_end}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--meta-file",
        default="",
        help="Case metadata CSV path. Default: outputs/spec_preview/product_case_studies.csv",
    )
    parser.add_argument(
        "--seed-prices-file",
        default="",
        help="Optional local prices seed file. Default: data/processed/prices_clean.csv",
    )
    parser.add_argument(
        "--cache-file",
        default="",
        help="Case price cache output path. Default: data/processed/case_price_cache.csv",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=4,
        help="BLS series count per request chunk. Default: 4",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=90,
        help="Per-request timeout seconds. Default: 90",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retries per chunk. Default: 3",
    )
    args = parser.parse_args()

    meta_file = resolve_path(args.meta_file, DEFAULT_META_FILE)
    seed_prices_file = resolve_path(args.seed_prices_file, DEFAULT_SEED_PRICES_FILE)
    cache_file = resolve_path(args.cache_file, DEFAULT_CACHE_FILE)

    meta = pd.read_csv(
        meta_file,
        parse_dates=["event_date", "base_date", "window_start", "window_end"],
    )

    required_cols = {"series_id", "window_start", "window_end"}
    missing_cols = required_cols - set(meta.columns)
    if missing_cols:
        raise ValueError(f"Missing required metadata columns: {sorted(missing_cols)}")

    meta["series_id"] = meta["series_id"].astype(str).str.strip()
    meta = meta[meta["series_id"] != ""].copy()

    requirements = build_requirements(meta)
    seed_df = load_optional_prices(seed_prices_file)
    cache_df = load_optional_prices(cache_file)

    combined_existing = pd.concat([seed_df, cache_df], ignore_index=True)
    if not combined_existing.empty:
        combined_existing = normalize_prices(combined_existing)

    print(f"Requested series: {len(requirements)}")
    print(f"Seed prices file: {seed_prices_file}")
    print(f"Existing cache file: {cache_file}")

    ready = []
    missing_rows = []

    for _, row in requirements.iterrows():
        sid = row["series_id"]
        required_start = row["required_start"]
        required_end = row["required_end"]

        if series_coverage_ok(combined_existing, sid, required_start, required_end):
            ready.append(sid)
        else:
            missing_rows.append(row)

    print(f"Series already covered locally: {len(ready)}")
    print(f"Series needing fetch: {len(missing_rows)}")

    fetched = pd.DataFrame(columns=["series_id", "date", "level"])
    fetch_errors: list[str] = []

    if missing_rows:
        missing_requirements = pd.DataFrame(missing_rows)
        try:
            fetched = fetch_missing_by_requirement_group(
                missing_requirements=missing_requirements,
                chunk_size=args.chunk_size,
                timeout=args.timeout,
                max_retries=args.max_retries,
            )
        except Exception as exc:
            fetch_errors.append(str(exc))
            print("Fetch warning:")
            print(f"- {exc}")
            print("Proceeding with existing local cache/seed data.")

    final_df = pd.concat([combined_existing, fetched], ignore_index=True)
    if not final_df.empty:
        final_df = normalize_prices(final_df)

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(cache_file, index=False)

    print(f"Wrote cache: {cache_file}")
    print(f"Cached rows: {len(final_df)}")
    print(f"Cached series: {final_df['series_id'].nunique() if not final_df.empty else 0}")

    present_series = set(final_df["series_id"].unique()) if not final_df.empty else set()
    required_series = set(requirements["series_id"].tolist())
    missing_entirely = sorted(required_series - present_series)
    if missing_entirely:
        raise RuntimeError(
            f"Required series missing entirely from cache after fetch attempt: {missing_entirely}"
        )

    uncovered = []
    for _, row in requirements.iterrows():
        sid = row["series_id"]
        required_start = row["required_start"]
        required_end = row["required_end"]

        if not series_coverage_ok(final_df, sid, required_start, required_end):
            uncovered.append(coverage_message(final_df, sid, required_start, required_end))

    if fetch_errors:
        print("Fetch errors encountered:")
        for msg in fetch_errors:
            print(f"- {msg}")

    if uncovered:
        print("Coverage warnings:")
        for msg in uncovered:
            print(f"- {msg}")


if __name__ == "__main__":
    main()