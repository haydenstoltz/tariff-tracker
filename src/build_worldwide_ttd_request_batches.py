from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_REGISTRY_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

REGISTRY_REQUIRED_COLS = [
    "year",
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


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def build_markdown(df: pd.DataFrame, indicator: str, product_group: str) -> str:
    lines: list[str] = []
    lines.append("# WTO TTD imports-by-partner request batches")
    lines.append("")
    lines.append(f"- Indicator: {indicator}")
    lines.append(f"- Product group: {product_group}")
    lines.append("- Priority order: reporter groups only, each request covering the full year range")
    lines.append("- Constraint: at most five reporters per request")
    lines.append("")

    for _, row in df.iterrows():
        lines.append(f"## {row['request_id']}")
        lines.append("")
        lines.append(f"- Request priority: {row['request_priority']}")
        lines.append(f"- Year range: {row['start_year']}–{row['end_year']}")
        lines.append(f"- Reporters ({row['reporter_count']}): {row['reporter_ids']}")
        lines.append(f"- Reporter names: {row['reporter_names']}")
        lines.append(f"- WTO reporter codes: {row['wto_reporter_codes']}")
        lines.append(f"- Expected canonical file count after normalize: {row['expected_canonical_file_count']}")
        lines.append(f"- Expected canonical patterns: {row['expected_canonical_patterns']}")
        lines.append(f"- TTD product group: {product_group}")
        lines.append(f"- TTD indicator: {indicator}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build WTO TTD request batches for imports-by-partner acquisition across a full year range, "
            "grouped only by reporters."
        )
    )
    parser.add_argument("--registry-file", default="", help="Path to worldwide_import_batch_registry.csv")
    parser.add_argument("--out-dir", default="", help="Output directory")
    parser.add_argument("--start-year", type=int, default=1996, help="First year inclusive")
    parser.add_argument("--end-year", type=int, default=2026, help="Last year inclusive")
    parser.add_argument(
        "--reporters",
        default="",
        help="Optional comma-separated reporter_ids subset, e.g. USA,CHN,JPN",
    )
    parser.add_argument("--max-reporters-per-request", type=int, default=5, help="Maximum reporters per request")
    parser.add_argument("--indicator", default="Imports by partner ($US)", help="TTD indicator label")
    parser.add_argument("--product-group", default="Total - all products", help="TTD product group label")
    args = parser.parse_args()

    if args.start_year > args.end_year:
        raise ValueError("--start-year must be <= --end-year")
    if args.max_reporters_per_request < 1:
        raise ValueError("--max-reporters-per-request must be >= 1")

    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    reporters_filter = {
        normalize_text(x).upper()
        for x in args.reporters.split(",")
        if normalize_text(x)
    }

    registry = pd.read_csv(registry_file, dtype=str, keep_default_na=False)
    for col in registry.columns:
        registry[col] = registry[col].map(normalize_text)
    require_columns(registry, REGISTRY_REQUIRED_COLS, registry_file.name)

    registry["year_num"] = pd.to_numeric(registry["year"], errors="coerce")
    registry = registry[
        registry["year_num"].between(args.start_year, args.end_year, inclusive="both")
    ].copy()

    if reporters_filter:
        registry = registry[registry["reporter_id"].str.upper().isin(reporters_filter)].copy()

    if registry.empty:
        raise ValueError("No registry rows remain after applying year/reporter filters")

    reporter_rows = (
        registry[
            ["reporter_id", "reporter_name", "wto_reporter_code"]
        ]
        .drop_duplicates(subset=["reporter_id"])
        .sort_values(["reporter_id"], kind="stable")
        .reset_index(drop=True)
    )

    year_count = args.end_year - args.start_year + 1

    rows: list[dict[str, str | int]] = []
    priority = 1
    for start in range(0, len(reporter_rows), args.max_reporters_per_request):
        chunk = reporter_rows.iloc[start : start + args.max_reporters_per_request].copy()
        batch_num = (start // args.max_reporters_per_request) + 1

        reporter_ids = chunk["reporter_id"].tolist()

        rows.append(
            {
                "request_priority": priority,
                "request_id": f"TTD_IMPORTS_{args.start_year}_{args.end_year}_{batch_num:02d}",
                "start_year": args.start_year,
                "end_year": args.end_year,
                "year_range": f"{args.start_year}-{args.end_year}",
                "indicator": args.indicator,
                "product_group": args.product_group,
                "reporter_count": int(len(chunk)),
                "reporter_ids": ",".join(reporter_ids),
                "reporter_names": " | ".join(chunk["reporter_name"].tolist()),
                "wto_reporter_codes": ",".join(chunk["wto_reporter_code"].tolist()),
                "expected_canonical_file_count": int(len(chunk) * year_count),
                "expected_canonical_patterns": " | ".join([f"imports_{rid}_<YEAR>.csv" for rid in reporter_ids]),
            }
        )
        priority += 1

    out = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = f"{args.start_year}_{args.end_year}"
    csv_file = out_dir / f"ttd_import_request_batches_{suffix}.csv"
    md_file = out_dir / f"ttd_import_request_batches_{suffix}.md"

    out.to_csv(csv_file, index=False)
    md_file.write_text(
        build_markdown(out, indicator=args.indicator, product_group=args.product_group),
        encoding="utf-8",
    )

    print(f"Reporter groups written: {len(out)}")
    print(f"Wrote: {csv_file}")
    print(f"Wrote: {md_file}")


if __name__ == "__main__":
    main()