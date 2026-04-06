from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_COVERAGE_FILE = ROOT / "outputs" / "worldwide" / "worldwide_import_batch_coverage.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

REQUIRED_COVERAGE_COLS = [
    "year",
    "batch_id",
    "reporter_id",
    "reporter_iso3",
    "reporter_name",
    "canonical_name",
    "wto_reporter_code",
    "expected_batch_filename",
    "expected_batch_path",
    "file_present",
    "source_family",
    "notes",
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
            "Build a canonical acquisition queue for missing WTO bilateral-import reporter batches "
            "from the live import-batch coverage file."
        )
    )
    parser.add_argument("--coverage-file", default="", help="Path to worldwide_import_batch_coverage.csv")
    parser.add_argument("--out-dir", default="", help="Output directory")
    args = parser.parse_args()

    coverage_file = resolve_path(args.coverage_file, DEFAULT_COVERAGE_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    coverage = pd.read_csv(coverage_file, dtype=str, keep_default_na=False)
    for col in coverage.columns:
        coverage[col] = coverage[col].map(normalize_text)

    require_columns(coverage, REQUIRED_COVERAGE_COLS, coverage_file.name)

    missing = coverage[coverage["file_present"].str.lower() != "yes"].copy()
    missing = missing.sort_values(["year", "reporter_id"], kind="stable").reset_index(drop=True)
    missing["priority_rank"] = range(1, len(missing) + 1)

    queue = missing[
        [
            "priority_rank",
            "year",
            "batch_id",
            "reporter_id",
            "reporter_iso3",
            "reporter_name",
            "canonical_name",
            "wto_reporter_code",
            "expected_batch_filename",
            "expected_batch_path",
            "source_family",
            "notes",
        ]
    ].copy()

    queue["source_portal"] = "WTO Data portal"
    queue["source_section"] = "INDICATORS"
    queue["flow"] = "Imports by origin"
    queue["product_scope"] = "TOTAL / All products"
    queue["format"] = "CSV"
    queue["download_status"] = "pending"

    queue = queue[
        [
            "priority_rank",
            "year",
            "batch_id",
            "reporter_id",
            "reporter_iso3",
            "reporter_name",
            "canonical_name",
            "wto_reporter_code",
            "source_portal",
            "source_section",
            "flow",
            "product_scope",
            "format",
            "expected_batch_filename",
            "expected_batch_path",
            "download_status",
            "source_family",
            "notes",
        ]
    ]

    out_dir.mkdir(parents=True, exist_ok=True)
    queue_file = out_dir / "worldwide_import_acquisition_queue.csv"
    queue.to_csv(queue_file, index=False)

    print(f"Missing reporter batches queued: {len(queue)}")
    print(f"Wrote: {queue_file}")


if __name__ == "__main__":
    main()