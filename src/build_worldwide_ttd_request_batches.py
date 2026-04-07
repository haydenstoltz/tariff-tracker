from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_REGISTRY_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"
DEFAULT_BATCH_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "imports_batches"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

REGISTRY_REQUIRED_COLS = [
    "year",
    "reporter_id",
    "reporter_name",
    "wto_reporter_code",
    "expected_batch_filename",
]

OUTPUT_COLUMNS = [
    "request_url",
    "request_priority",
    "request_id",
    "start_year",
    "end_year",
    "year_range",
    "requested_year_count",
    "requested_years_csv",
    "indicator",
    "product_group",
    "reporter_count",
    "reporter_ids",
    "reporter_names",
    "wto_reporter_codes",
    "missing_canonical_file_count",
    "expected_canonical_patterns",
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


def canonical_product_group_slug(label: str) -> str:
    mapping = {
        "total - all products": "total",
        "agricultural products": "agricultural",
        "nonagricultural products": "nonagricultural",
        "mtn product group": "mtn",
    }
    key = normalize_text(label).lower()
    return mapping.get(key, "total")


def canonical_indicator_slug(label: str) -> str:
    mapping = {
        "imports by partner ($us)": "imports",
        "exports by destination ($us)": "exports",
        "mfn applied tariffs": "mfn_applied",
        "mfn final bound tariffs": "mfn_final_bound",
        "preferential tariffs": "preferential",
    }
    key = normalize_text(label).lower()
    return mapping.get(key, "imports")


def build_request_url(
    product_group_slug: str,
    indicator_slug: str,
    years_desc: list[str],
    reporter_codes: list[str],
) -> str:
    base = "https://ttd.wto.org/en/download/indicators"
    parts = [
        f"product_group={product_group_slug}",
        f"indicator={indicator_slug}",
    ]

    for idx, year in enumerate(years_desc):
        parts.append(f"years[{idx}]={year}")

    for idx, code in enumerate(reporter_codes):
        parts.append(f"reporters[{idx}]=C{normalize_text(code).zfill(3)}")

    return f"{base}?{'&'.join(parts)}"


def format_year_range(years_desc: list[str]) -> str:
    if not years_desc:
        return ""
    years_int = sorted({int(y) for y in years_desc})
    if years_int == list(range(years_int[0], years_int[-1] + 1)):
        return f"{years_int[0]}-{years_int[-1]}"
    return ",".join(str(y) for y in sorted(years_int, reverse=True))


def build_markdown(df: pd.DataFrame, indicator: str, product_group: str) -> str:
    lines: list[str] = []
    lines.append("# WTO TTD imports-by-partner request batches")
    lines.append("")
    lines.append(f"- Indicator: {indicator}")
    lines.append(f"- Product group: {product_group}")
    lines.append("- Rows shown below are still missing from the canonical imports batch inbox.")
    lines.append("- Fully completed request groups are omitted automatically.")
    lines.append("")

    if df.empty:
        lines.append("All request groups are currently complete.")
        return "\n".join(lines)

    for _, row in df.iterrows():
        lines.append(f"## {row['request_id']}")
        lines.append("")
        lines.append(f"- Request priority: {row['request_priority']}")
        lines.append(f"- Request URL: {row['request_url']}")
        lines.append(f"- Requested years: {row['requested_years_csv']}")
        lines.append(f"- Reporters ({row['reporter_count']}): {row['reporter_ids']}")
        lines.append(f"- Reporter names: {row['reporter_names']}")
        lines.append(f"- WTO reporter codes: {row['wto_reporter_codes']}")
        lines.append(f"- Missing canonical files represented: {row['missing_canonical_file_count']}")
        lines.append(f"- Expected canonical patterns: {row['expected_canonical_patterns']}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build WTO TTD request batches for missing imports-by-partner data. "
            "Completed reporter-year files already present in imports_batches are removed automatically."
        )
    )
    parser.add_argument("--registry-file", default="", help="Path to worldwide_import_batch_registry.csv")
    parser.add_argument("--batch-dir", default="", help="Path to canonical imports_batches directory")
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
    batch_dir = resolve_path(args.batch_dir, DEFAULT_BATCH_DIR)
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

    present_files = {
        path.name for path in batch_dir.glob("imports_*.csv") if path.is_file()
    }

    reporter_rows = (
        registry[["reporter_id", "reporter_name", "wto_reporter_code"]]
        .drop_duplicates(subset=["reporter_id"])
        .sort_values(["reporter_id"], kind="stable")
        .reset_index(drop=True)
    )

    product_group_slug = canonical_product_group_slug(args.product_group)
    indicator_slug = canonical_indicator_slug(args.indicator)

    rows: list[dict[str, object]] = []
    batch_counter = 1

    for start in range(0, len(reporter_rows), args.max_reporters_per_request):
        chunk = reporter_rows.iloc[start : start + args.max_reporters_per_request].copy()
        original_request_id = f"TTD_IMPORTS_{args.start_year}_{args.end_year}_{batch_counter:02d}"
        batch_counter += 1

        chunk_ids = chunk["reporter_id"].tolist()
        chunk_registry = registry[registry["reporter_id"].isin(chunk_ids)].copy()
        chunk_registry["file_present"] = chunk_registry["expected_batch_filename"].isin(present_files)

        missing_registry = chunk_registry[~chunk_registry["file_present"]].copy()
        if missing_registry.empty:
            continue

        reporter_missing_years: dict[str, tuple[str, ...]] = {}
        for reporter_id in chunk_ids:
            reporter_years = (
                missing_registry.loc[
                    missing_registry["reporter_id"] == reporter_id,
                    "year",
                ]
                .drop_duplicates()
                .tolist()
            )
            reporter_years = [normalize_text(y) for y in reporter_years if normalize_text(y)]
            if reporter_years:
                reporter_missing_years[reporter_id] = tuple(
                    str(y) for y in sorted({int(y) for y in reporter_years}, reverse=True)
                )

        grouped_patterns: dict[tuple[str, ...], list[str]] = {}
        for reporter_id, year_pattern in reporter_missing_years.items():
            grouped_patterns.setdefault(year_pattern, []).append(reporter_id)

        subgroup_index = 0
        for year_pattern in sorted(
            grouped_patterns.keys(),
            key=lambda years: (
                -max(int(y) for y in years),
                -len(years),
                ",".join(grouped_patterns[years]),
            ),
        ):
            subgroup_reporters = sorted(grouped_patterns[year_pattern])
            subgroup_meta = chunk[chunk["reporter_id"].isin(subgroup_reporters)].copy()
            subgroup_meta = subgroup_meta.sort_values(["reporter_id"], kind="stable").reset_index(drop=True)

            subgroup_missing = missing_registry[
                missing_registry["reporter_id"].isin(subgroup_reporters)
                & missing_registry["year"].isin(list(year_pattern))
            ].copy()

            if subgroup_missing.empty:
                continue

            request_id = (
                original_request_id
                if len(grouped_patterns) == 1
                else f"{original_request_id}_{chr(65 + subgroup_index)}"
            )
            subgroup_index += 1

            years_desc = list(year_pattern)
            reporter_codes = subgroup_meta["wto_reporter_code"].map(lambda x: normalize_text(x).zfill(3)).tolist()

            rows.append(
                {
                    "request_url": build_request_url(
                        product_group_slug=product_group_slug,
                        indicator_slug=indicator_slug,
                        years_desc=years_desc,
                        reporter_codes=reporter_codes,
                    ),
                    "request_priority": 0,
                    "request_id": request_id,
                    "start_year": min(int(y) for y in years_desc),
                    "end_year": max(int(y) for y in years_desc),
                    "year_range": format_year_range(years_desc),
                    "requested_year_count": len(years_desc),
                    "requested_years_csv": ",".join(years_desc),
                    "indicator": args.indicator,
                    "product_group": args.product_group,
                    "reporter_count": int(len(subgroup_meta)),
                    "reporter_ids": ",".join(subgroup_meta["reporter_id"].tolist()),
                    "reporter_names": " | ".join(subgroup_meta["reporter_name"].tolist()),
                    "wto_reporter_codes": ",".join(reporter_codes),
                    "missing_canonical_file_count": int(len(subgroup_missing)),
                    "expected_canonical_patterns": " | ".join(
                        [f"imports_{rid}_<YEAR>.csv" for rid in subgroup_meta["reporter_id"].tolist()]
                    ),
                }
            )

    out = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    if not out.empty:
        out = out.sort_values(
            by=["end_year", "requested_year_count", "reporter_ids"],
            ascending=[False, False, True],
            kind="stable",
        ).reset_index(drop=True)
        out["request_priority"] = range(1, len(out) + 1)

    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = f"{args.start_year}_{args.end_year}"
    csv_file = out_dir / f"ttd_import_request_batches_{suffix}.csv"
    md_file = out_dir / f"ttd_import_request_batches_{suffix}.md"

    out.to_csv(csv_file, index=False)
    md_file.write_text(
        build_markdown(out, indicator=args.indicator, product_group=args.product_group),
        encoding="utf-8",
    )

    print(f"Outstanding request rows written: {len(out)}")
    print(f"Wrote: {csv_file}")
    print(f"Wrote: {md_file}")


if __name__ == "__main__":
    main()