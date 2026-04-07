from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_REGISTRY_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"
DEFAULT_BATCH_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "imports_batches"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

REQUIRED_REGISTRY_COLS = [
    "year",
    "batch_id",
    "reporter_id",
    "reporter_iso3",
    "reporter_name",
    "canonical_name",
    "wto_reporter_code",
    "expected_batch_filename",
    "source_family",
    "acquisition_status",
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
            "Build a coverage report showing which expected bilateral-import reporter batch files "
            "are present or missing in the imports batch inbox."
        )
    )
    parser.add_argument("--registry-file", default="", help="Path to worldwide_import_batch_registry.csv")
    parser.add_argument("--batch-dir", default="", help="Path to imports_batches directory")
    parser.add_argument("--out-dir", default="", help="Output directory for coverage reports")
    parser.add_argument("--year", default="", help="Optional explicit year filter")
    args = parser.parse_args()

    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    batch_dir = resolve_path(args.batch_dir, DEFAULT_BATCH_DIR)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    registry = pd.read_csv(registry_file, dtype=str, keep_default_na=False)
    for col in registry.columns:
        registry[col] = registry[col].map(normalize_text)

    require_columns(registry, REQUIRED_REGISTRY_COLS, registry_file.name)
    
    requested_year = normalize_text(args.year)
    if requested_year:
        if not requested_year.isdigit() or len(requested_year) != 4:
            raise ValueError(f"--year must be a four-digit year, got: {requested_year}")
        registry = registry[registry["year"] == requested_year].copy()
        if registry.empty:
            raise ValueError(f"No registry rows found for year {requested_year}")

    batch_dir.mkdir(parents=True, exist_ok=True)
    present_files = {path.name: path for path in batch_dir.glob("*.csv") if path.is_file()}

    coverage = registry.copy()
    coverage["expected_batch_path"] = coverage["expected_batch_filename"].map(
        lambda x: str(batch_dir / x)
    )
    coverage["file_present"] = coverage["expected_batch_filename"].map(
        lambda x: "yes" if x in present_files else "no"
    )
    coverage["present_file_name"] = coverage["expected_batch_filename"].map(
        lambda x: x if x in present_files else ""
    )
    coverage["present_file_size_bytes"] = coverage["expected_batch_filename"].map(
        lambda x: present_files[x].stat().st_size if x in present_files else ""
    )

    coverage = coverage[
        [
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
            "present_file_name",
            "present_file_size_bytes",
            "source_family",
            "acquisition_status",
            "notes",
        ]
    ].sort_values(["year", "reporter_id"], kind="stable").reset_index(drop=True)

    missing = coverage[coverage["file_present"] != "yes"].copy()

    out_dir.mkdir(parents=True, exist_ok=True)
    coverage_file = out_dir / "worldwide_import_batch_coverage.csv"
    missing_file = out_dir / "worldwide_import_batch_missing.csv"

    coverage.to_csv(coverage_file, index=False)
    missing.to_csv(missing_file, index=False)

    print(f"Expected reporter batches: {len(coverage)}")
    print(f"Present reporter batches: {(coverage['file_present'] == 'yes').sum()}")
    print(f"Missing reporter batches: {(coverage['file_present'] != 'yes').sum()}")
    print(f"Wrote: {coverage_file}")
    print(f"Wrote: {missing_file}")


if __name__ == "__main__":
    main()