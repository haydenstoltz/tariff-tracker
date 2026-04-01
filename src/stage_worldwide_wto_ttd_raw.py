from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_INBOX_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox"
DEFAULT_STAGED_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd"
DEFAULT_MANIFEST_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "raw_refresh_manifest.json"

DEFAULT_IMPORTS_INBOX_NAME = "imports_by_partner_latest.csv"
DEFAULT_MFN_INBOX_NAME = "mfn_applied_total_latest.csv"

REQUIRED_IMPORTS_COLS = [
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

REQUIRED_MFN_COLS = [
    "reporter_name",
    "reporter_code",
    "year",
    "classification",
    "classification_version",
    "duty_scheme_code",
    "duty_scheme_name",
    "product_code",
    "mtn_categories",
    "simple_average",
    "trade_weighted",
    "duty_free_share",
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


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required raw file: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in df.columns:
        df[col] = df[col].map(normalize_text)
    return df


def infer_single_year(df: pd.DataFrame, label: str) -> str:
    years = sorted({normalize_text(x) for x in df["year"].tolist() if normalize_text(x)})
    if not years:
        raise ValueError(f"No nonblank year values found in {label}")
    if len(years) != 1:
        raise ValueError(f"Expected exactly one year in {label}, found: {years}")
    year = years[0]
    if not year.isdigit() or len(year) != 4:
        raise ValueError(f"Invalid year value in {label}: {year}")
    return year


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def safe_copy(source: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve() == dest.resolve():
        return
    shutil.copy2(source, dest)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Validate manually dropped WTO TTD CSV exports and stage them into canonical "
            "year-specific raw files used by the worldwide refresh pipeline."
        )
    )
    parser.add_argument("--inbox-dir", default="", help="Inbox directory for raw WTO exports")
    parser.add_argument("--staged-dir", default="", help="Canonical staged raw directory")
    parser.add_argument("--manifest-file", default="", help="Path to raw staging manifest JSON")
    parser.add_argument("--imports-file", default="", help="Optional explicit path to imports raw CSV")
    parser.add_argument("--mfn-file", default="", help="Optional explicit path to MFN raw CSV")
    args = parser.parse_args()

    inbox_dir = resolve_path(args.inbox_dir, DEFAULT_INBOX_DIR)
    staged_dir = resolve_path(args.staged_dir, DEFAULT_STAGED_DIR)
    manifest_file = resolve_path(args.manifest_file, DEFAULT_MANIFEST_FILE)

    inbox_dir.mkdir(parents=True, exist_ok=True)
    staged_dir.mkdir(parents=True, exist_ok=True)

    imports_source = (
        resolve_path(args.imports_file, inbox_dir / DEFAULT_IMPORTS_INBOX_NAME)
        if args.imports_file.strip()
        else inbox_dir / DEFAULT_IMPORTS_INBOX_NAME
    )
    mfn_source = (
        resolve_path(args.mfn_file, inbox_dir / DEFAULT_MFN_INBOX_NAME)
        if args.mfn_file.strip()
        else inbox_dir / DEFAULT_MFN_INBOX_NAME
    )

    imports_df = read_csv(imports_source)
    mfn_df = read_csv(mfn_source)

    require_columns(imports_df, REQUIRED_IMPORTS_COLS, imports_source.name)
    require_columns(mfn_df, REQUIRED_MFN_COLS, mfn_source.name)

    if imports_df.empty:
        raise ValueError(f"Imports raw file is empty: {imports_source}")
    if mfn_df.empty:
        raise ValueError(f"MFN raw file is empty: {mfn_source}")

    imports_year = infer_single_year(imports_df, imports_source.name)
    mfn_year = infer_single_year(mfn_df, mfn_source.name)

    if imports_year != mfn_year:
        raise ValueError(
            f"Imports year ({imports_year}) and MFN year ({mfn_year}) do not match."
        )

    year = imports_year

    staged_imports = staged_dir / f"imports_by_partner_{year}.csv"
    staged_mfn = staged_dir / f"mfn_applied_total_{year}.csv"

    safe_copy(imports_source, staged_imports)
    safe_copy(mfn_source, staged_mfn)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "year": year,
        "imports_source_file": str(imports_source),
        "mfn_source_file": str(mfn_source),
        "staged_imports_file": str(staged_imports),
        "staged_mfn_file": str(staged_mfn),
        "imports_row_count": int(len(imports_df)),
        "mfn_row_count": int(len(mfn_df)),
        "imports_columns": list(imports_df.columns),
        "mfn_columns": list(mfn_df.columns),
    }
    write_json(manifest_file, manifest)

    print(f"Staged raw year: {year}")
    print(f"Wrote: {staged_imports}")
    print(f"Wrote: {staged_mfn}")
    print(f"Wrote: {manifest_file}")


if __name__ == "__main__":
    main()