from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_QUEUE_FILE = ROOT / "outputs" / "worldwide" / "worldwide_import_acquisition_queue.csv"
DEFAULT_REGISTRY_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

QUEUE_REQUIRED_COLS = [
    "priority_rank",
    "year",
    "reporter_id",
    "reporter_name",
    "wto_reporter_code",
    "expected_batch_filename",
]

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


def load_request_rows(queue_file: Path, registry_file: Path, year: str, reporters_filter: set[str]) -> pd.DataFrame:
    if queue_file.exists():
        df = pd.read_csv(queue_file, dtype=str, keep_default_na=False)
        for col in df.columns:
            df[col] = df[col].map(normalize_text)
        require_columns(df, QUEUE_REQUIRED_COLS, queue_file.name)

        df = df[df["year"] == year].copy()
        if reporters_filter:
            df = df[df["reporter_id"].str.upper().isin(reporters_filter)].copy()

        df["priority_rank_num"] = pd.to_numeric(df["priority_rank"], errors="coerce")
        df = df.sort_values(
            by=["priority_rank_num", "reporter_id"],
            ascending=[True, True],
            kind="stable",
            na_position="last",
        ).reset_index(drop=True)
        return df

    df = pd.read_csv(registry_file, dtype=str, keep_default_na=False)
    for col in df.columns:
        df[col] = df[col].map(normalize_text)
    require_columns(df, REGISTRY_REQUIRED_COLS, registry_file.name)

    df = df[df["year"] == year].copy()
    if reporters_filter:
        df = df[df["reporter_id"].str.upper().isin(reporters_filter)].copy()

    df["priority_rank"] = [str(i) for i in range(1, len(df) + 1)]
    df["priority_rank_num"] = pd.to_numeric(df["priority_rank"], errors="coerce")
    df = df.sort_values(["priority_rank_num", "reporter_id"], kind="stable").reset_index(drop=True)
    return df


def build_markdown(batch_df: pd.DataFrame, year: str, indicator: str, product_group: str) -> str:
    lines: list[str] = []
    lines.append("# WTO TTD imports-by-partner request batches")
    lines.append("")
    lines.append(f"- Year: {year}")
    lines.append(f"- Indicator: {indicator}")
    lines.append(f"- Product group: {product_group}")
    lines.append("- Constraint: request one year at a time")
    lines.append("")

    for _, row in batch_df.iterrows():
        lines.append(f"## {row['request_id']}")
        lines.append("")
        lines.append(f"- Reporters ({row['reporter_count']}): {row['reporter_ids']}")
        lines.append(f"- Reporter names: {row['reporter_names']}")
        lines.append(f"- WTO reporter codes: {row['wto_reporter_codes']}")
        lines.append(f"- Expected canonical files: {row['expected_batch_filenames']}")
        lines.append("- TTD selection:")
        lines.append(f"  - Product group: {product_group}")
        lines.append(f"  - Indicator: {indicator}")
        lines.append(f"  - Year: {year}")
        lines.append("- Delivery: email CSV")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build WTO TTD request batches for imports-by-partner acquisition, "
            "grouped into at most N reporters per email-export request."
        )
    )
    parser.add_argument("--queue-file", default="", help="Path to worldwide_import_acquisition_queue.csv")
    parser.add_argument("--registry-file", default="", help="Path to worldwide_import_batch_registry.csv")
    parser.add_argument("--year", default="2023", help="Single target year")
    parser.add_argument(
        "--reporters",
        default="",
        help="Optional comma-separated reporter_ids subset, e.g. BRA,SAU,RUS",
    )
    parser.add_argument(
        "--max-reporters-per-request",
        type=int,
        default=5,
        help="Maximum reporters per TTD request batch",
    )
    parser.add_argument(
        "--indicator",
        default="Imports by partner ($US)",
        help="TTD indicator label",
    )
    parser.add_argument(
        "--product-group",
        default="Total - all products",
        help="TTD product group label",
    )
    parser.add_argument("--out-dir", default="", help="Output directory")
    args = parser.parse_args()

    if not args.year.isdigit() or len(args.year) != 4:
        raise ValueError(f"--year must be a single four-digit year, got: {args.year}")

    if args.max_reporters_per_request < 1:
        raise ValueError("--max-reporters-per-request must be >= 1")

    reporters_filter = {
        normalize_text(x).upper()
        for x in args.reporters.split(",")
        if normalize_text(x)
    }

    queue_file = resolve_path(args.queue_file, DEFAULT_QUEUE_FILE)
    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    rows = load_request_rows(
        queue_file=queue_file,
        registry_file=registry_file,
        year=args.year,
        reporters_filter=reporters_filter,
    )
    if rows.empty:
        raise ValueError("No reporter rows available for request-batch generation")

    batches: list[dict[str, str | int]] = []
    for start in range(0, len(rows), args.max_reporters_per_request):
        chunk = rows.iloc[start : start + args.max_reporters_per_request].copy()
        request_num = len(batches) + 1

        batches.append(
            {
                "request_id": f"TTD_IMPORTS_{args.year}_{request_num:02d}",
                "year": args.year,
                "indicator": args.indicator,
                "product_group": args.product_group,
                "reporter_count": int(len(chunk)),
                "reporter_ids": ",".join(chunk["reporter_id"].tolist()),
                "reporter_names": " | ".join(chunk["reporter_name"].tolist()),
                "wto_reporter_codes": ",".join(chunk["wto_reporter_code"].tolist()),
                "expected_batch_filenames": ",".join(chunk["expected_batch_filename"].tolist()),
                "priority_ranks": ",".join(chunk["priority_rank"].tolist()),
            }
        )

    batch_df = pd.DataFrame(batches)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_file = out_dir / f"ttd_import_request_batches_{args.year}.csv"
    md_file = out_dir / f"ttd_import_request_batches_{args.year}.md"

    batch_df.to_csv(csv_file, index=False)
    md_file.write_text(
        build_markdown(
            batch_df=batch_df,
            year=args.year,
            indicator=args.indicator,
            product_group=args.product_group,
        ),
        encoding="utf-8",
    )

    print(f"Reporter rows batched: {len(rows)}")
    print(f"Request batches created: {len(batch_df)}")
    print(f"Wrote: {csv_file}")
    print(f"Wrote: {md_file}")


if __name__ == "__main__":
    main()