from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_TERRITORIES_FILE = ROOT / "data" / "metadata" / "world" / "customs_territories.csv"
DEFAULT_CODE_MAP_FILE = ROOT / "data" / "metadata" / "world" / "wto_actor_code_map.csv"
DEFAULT_OUT_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_source_pull_registry.csv"

TERRITORY_REQUIRED_COLS = [
    "actor_id",
    "display_name",
    "active_flag",
]

CODE_MAP_REQUIRED_COLS = [
    "actor_id",
    "wto_partner_code",
]

DATASETS = [
    {
        "logical_dataset": "mfn_simple_average_all_products",
        "indicator_code": "TP_A_0010",
        "output_filename": "mfn_simple_average_latest.csv",
        "note_stub": "Simple average MFN applied tariff - all products",
    },
    {
        "logical_dataset": "mfn_trade_weighted_all_products",
        "indicator_code": "TP_A_0030",
        "output_filename": "mfn_trade_weighted_latest.csv",
        "note_stub": "Trade-weighted MFN applied tariff average - all products",
    },
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


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a multi-year WTO Timeseries MFN source-pull registry covering all active "
            "reporter-years."
        )
    )
    parser.add_argument("--territories-file", default="", help="Path to customs_territories.csv")
    parser.add_argument("--code-map-file", default="", help="Path to wto_actor_code_map.csv")
    parser.add_argument("--out-file", default="", help="Output path")
    parser.add_argument("--start-year", type=int, default=1996, help="First year inclusive")
    parser.add_argument("--end-year", type=int, default=2026, help="Last year inclusive")
    parser.add_argument("--timeout-seconds", type=int, default=120, help="HTTP timeout")
    args = parser.parse_args()

    if args.start_year > args.end_year:
        raise ValueError("--start-year must be <= --end-year")

    territories_file = resolve_path(args.territories_file, DEFAULT_TERRITORIES_FILE)
    code_map_file = resolve_path(args.code_map_file, DEFAULT_CODE_MAP_FILE)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)

    territories = pd.read_csv(territories_file, dtype=str, keep_default_na=False)
    code_map = pd.read_csv(code_map_file, dtype=str, keep_default_na=False)

    for df in [territories, code_map]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    require_columns(territories, TERRITORY_REQUIRED_COLS, territories_file.name)
    require_columns(code_map, CODE_MAP_REQUIRED_COLS, code_map_file.name)

    territories = territories[territories["active_flag"].str.lower() == "yes"].copy()
    code_map["wto_partner_code"] = code_map["wto_partner_code"].map(lambda x: normalize_text(x).zfill(3))

    merged = territories.merge(code_map, on="actor_id", how="left", validate="one_to_one")
    missing_codes = merged[merged["wto_partner_code"].map(normalize_text) == ""]
    if not missing_codes.empty:
        raise ValueError(
            "Missing WTO code-map rows for actor_ids:\n"
            + "\n".join(sorted(missing_codes["actor_id"].tolist()))
        )

    rows: list[dict[str, str]] = []
    for year in range(args.start_year, args.end_year + 1):
        for _, row in merged.sort_values(["actor_id"], kind="stable").iterrows():
            actor_id = normalize_text(row["actor_id"]).upper()
            display_name = normalize_text(row["display_name"])
            reporter_code = normalize_text(row["wto_partner_code"])

            for dataset in DATASETS:
                request_url = (
                    "https://api.wto.org/timeseries/v1/data"
                    f"?i={dataset['indicator_code']}"
                    f"&r={reporter_code}"
                    f"&ps={year}"
                )

                rows.append(
                    {
                        "logical_dataset": dataset["logical_dataset"],
                        "batch_id": f"{actor_id}_{year}",
                        "provider": "WTO",
                        "enabled_flag": "yes",
                        "request_url": request_url,
                        "output_filename": dataset["output_filename"],
                        "auth_location": "query_param",
                        "auth_name": "subscription-key",
                        "subscription_env_var": "WTO_API_KEY",
                        "timeout_seconds": str(args.timeout_seconds),
                        "notes": f"{dataset['note_stub']} | reporter={display_name} | year={year}",
                    }
                )

    out = pd.DataFrame(rows).sort_values(
        by=["logical_dataset", "batch_id"],
        ascending=[True, False],
        kind="stable",
    ).reset_index(drop=True)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_file, index=False)

    print(f"Active reporters: {merged['actor_id'].nunique()}")
    print(f"Years covered: {args.start_year}-{args.end_year}")
    print(f"Rows written: {len(out)}")
    print(f"Wrote: {out_file}")


if __name__ == "__main__":
    main()