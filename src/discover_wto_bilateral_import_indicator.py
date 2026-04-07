from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"
DEFAULT_REPORTER_REGISTRY = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"
DEFAULT_BASE_URL = "https://api.wto.org/timeseries/v1"


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


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def build_session(api_key_env: str) -> requests.Session:
    api_key = os.getenv(api_key_env, "").strip()
    if not api_key:
        raise ValueError(f"Missing required environment variable: {api_key_env}")

    session = requests.Session()
    session.headers.update(
        {
            "Ocp-Apim-Subscription-Key": api_key,
            "Accept": "application/json",
            "User-Agent": "tariff-tracker-wto-discovery/1.0",
        }
    )
    return session


def ensure_text_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[col].map(normalize_text)


def load_sample_reporter_codes(registry_file: Path, sample_size: int) -> list[str]:
    registry = pd.read_csv(registry_file, dtype=str, keep_default_na=False)
    for col in registry.columns:
        registry[col] = registry[col].map(normalize_text)

    require_columns(
        registry,
        ["year", "reporter_id", "wto_reporter_code"],
        registry_file.name,
    )

    codes = []
    for code in registry["wto_reporter_code"].tolist():
        code3 = normalize_code3(code)
        if code3 and code3 not in codes:
            codes.append(code3)

    if not codes:
        raise ValueError(f"No usable wto_reporter_code values found in {registry_file}")

    return codes[:sample_size]


def candidate_score(name: str, description: str, include_keywords: list[str], exclude_keywords: list[str]) -> int:
    haystack = f"{name} {description}".lower()
    score = 0

    for keyword in include_keywords:
        kw = keyword.lower().strip()
        if kw and kw in haystack:
            score += 2

    for keyword in exclude_keywords:
        kw = keyword.lower().strip()
        if kw and kw in haystack:
            score -= 3

    if "import" in haystack:
        score += 3
    if "partner" in haystack or "origin" in haystack or "bilateral" in haystack:
        score += 2
    if "goods" in haystack or "merchandise" in haystack:
        score += 1
    if "service" in haystack or "services" in haystack:
        score -= 4
    if "export" in haystack or "exports" in haystack:
        score -= 4

    return score


def fetch_indicators(
    session: requests.Session,
    base_url: str,
    language: int,
) -> pd.DataFrame:
    url = f"{base_url.rstrip('/')}/indicators"
    params = {
        "tp": "yes",
        "frq": "A",
        "lang": language,
    }

    response = session.get(url, params=params, timeout=120)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise ValueError(f"Unexpected /indicators response shape: {type(payload)}")

    frame = pd.DataFrame(payload)
    if frame.empty:
        raise ValueError("WTO /indicators returned no rows")

    for col in frame.columns:
        frame[col] = frame[col].map(normalize_text)

    return frame


def probe_data_count(
    session: requests.Session,
    base_url: str,
    indicator_code: str,
    reporter_code: str,
    year: str,
    pc: str,
) -> tuple[int | None, str]:
    url = f"{base_url.rstrip('/')}/data_count"
    params = {
        "i": indicator_code,
        "r": reporter_code,
        "p": "all",
        "ps": year,
        "pc": pc,
        "spc": "false",
    }

    try:
        response = session.get(url, params=params, timeout=120)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, int):
            return payload, ""
        if isinstance(payload, float):
            return int(payload), ""
        if isinstance(payload, str) and payload.strip().isdigit():
            return int(payload.strip()), ""
        return None, f"unexpected data_count payload type={type(payload).__name__}"
    except Exception as exc:
        return None, str(exc)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Discover likely WTO Timeseries indicators for partner-aware bilateral imports, "
            "and optionally probe them with /data_count for a sample reporter/year."
        )
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="WTO Timeseries API base URL")
    parser.add_argument("--api-key-env", default="WTO_API_KEY", help="Environment variable holding the WTO API key")
    parser.add_argument("--reporter-registry", default="", help="Path to worldwide_import_batch_registry.csv")
    parser.add_argument("--sample-size", type=int, default=3, help="Number of sample reporters to use for probing")
    parser.add_argument("--sample-year", default="2023", help="Sample year for data_count probes")
    parser.add_argument("--pc", default="default", help="Product filter passed to data_count probes")
    parser.add_argument(
        "--include-keywords",
        default="import,imports,origin,partner,bilateral,goods,merchandise",
        help="Comma-separated keywords used to raise candidate scores",
    )
    parser.add_argument(
        "--exclude-keywords",
        default="export,exports,service,services",
        help="Comma-separated keywords used to lower candidate scores",
    )
    parser.add_argument(
        "--probe-limit",
        type=int,
        default=40,
        help="Probe /data_count for at most this many top-ranked candidates",
    )
    parser.add_argument("--lang", type=int, default=1, help="WTO API language id")
    parser.add_argument("--out-dir", default="", help="Output directory")
    args = parser.parse_args()

    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)
    reporter_registry = resolve_path(args.reporter_registry, DEFAULT_REPORTER_REGISTRY)

    include_keywords = [x.strip() for x in args.include_keywords.split(",") if x.strip()]
    exclude_keywords = [x.strip() for x in args.exclude_keywords.split(",") if x.strip()]

    if not args.sample_year.isdigit() or len(args.sample_year) != 4:
        raise ValueError(f"--sample-year must be a four-digit year, got: {args.sample_year}")

    sample_reporters = load_sample_reporter_codes(reporter_registry, max(args.sample_size, 1))
    session = build_session(args.api_key_env)

    indicators = fetch_indicators(session=session, base_url=args.base_url, language=args.lang)

    indicators["code"] = ensure_text_col(indicators, "code")
    indicators["name"] = ensure_text_col(indicators, "name")
    indicators["description"] = ensure_text_col(indicators, "description")
    indicators["numberPartners_num"] = pd.to_numeric(ensure_text_col(indicators, "numberPartners"), errors="coerce")
    indicators["numberReporters_num"] = pd.to_numeric(ensure_text_col(indicators, "numberReporters"), errors="coerce")
    indicators["keyword_score"] = indicators.apply(
        lambda row: candidate_score(
            name=normalize_text(row.get("name", "")),
            description=normalize_text(row.get("description", "")),
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
        ),
        axis=1,
    )

    indicators = indicators.sort_values(
        by=["keyword_score", "numberPartners_num", "numberReporters_num", "code"],
        ascending=[False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)

    indicators["sample_reporter_code"] = ""
    indicators["sample_year"] = ""
    indicators["sample_data_count"] = ""
    indicators["probe_status"] = ""

    probe_rows = indicators.head(max(args.probe_limit, 0)).copy()

    for idx in probe_rows.index:
        indicator_code = normalize_text(indicators.at[idx, "code"])
        best_count = None
        best_reporter = ""
        status_parts: list[str] = []

        for reporter_code in sample_reporters:
            count, status = probe_data_count(
                session=session,
                base_url=args.base_url,
                indicator_code=indicator_code,
                reporter_code=reporter_code,
                year=args.sample_year,
                pc=args.pc,
            )
            if count is not None:
                if best_count is None or count > best_count:
                    best_count = count
                    best_reporter = reporter_code
            else:
                status_parts.append(f"{reporter_code}: {status}")

        indicators.at[idx, "sample_year"] = args.sample_year
        indicators.at[idx, "sample_reporter_code"] = best_reporter
        indicators.at[idx, "sample_data_count"] = "" if best_count is None else str(int(best_count))
        indicators.at[idx, "probe_status"] = " | ".join(status_parts)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "wto_partner_indicator_candidates.csv"

    preferred_cols = [
        "code",
        "name",
        "keyword_score",
        "numberPartners",
        "numberReporters",
        "frequencyCode",
        "productSectorClassificationCode",
        "productSectorClassificationLabel",
        "unitCode",
        "unitLabel",
        "startYear",
        "endYear",
        "sample_reporter_code",
        "sample_year",
        "sample_data_count",
        "categoryCode",
        "categoryLabel",
        "subcategoryCode",
        "subcategoryLabel",
        "description",
        "probe_status",
    ]
    output_cols = [col for col in preferred_cols if col in indicators.columns] + [
        col for col in indicators.columns if col not in preferred_cols
    ]

    indicators[output_cols].to_csv(out_file, index=False)

    print(f"Indicators scanned: {len(indicators)}")
    print(f"Sample reporters used for probing: {', '.join(sample_reporters)}")
    print(f"Wrote: {out_file}")


if __name__ == "__main__":
    main()