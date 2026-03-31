from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LIVE_DIR = ROOT / "data" / "metadata"
DEFAULT_PREVIEW_DIR = ROOT / "outputs" / "spec_preview"

FILES = [
    "site_cases.csv",
    "event_case_map.csv",
    "case_stage_map.csv",
    "product_case_studies.csv",
]


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df = df.reindex(sorted(df.columns), axis=1)
    if len(df.columns) > 0:
        df = df.sort_values(list(df.columns), kind="stable").reset_index(drop=True)
    return df


def row_key_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=str)
    return df.astype(str).agg(" | ".join, axis=1)


def compare_one_file(live_path: Path, preview_path: Path, max_examples: int) -> bool:
    if not live_path.exists():
        raise FileNotFoundError(f"Missing live file: {live_path}")
    if not preview_path.exists():
        raise FileNotFoundError(f"Missing preview file: {preview_path}")

    live_df = normalize_df(pd.read_csv(live_path, dtype=str, keep_default_na=False))
    preview_df = normalize_df(pd.read_csv(preview_path, dtype=str, keep_default_na=False))

    live_cols = list(live_df.columns)
    preview_cols = list(preview_df.columns)

    print(f"\n=== {live_path.name} ===")
    print(f"Live rows: {len(live_df)}")
    print(f"Preview rows: {len(preview_df)}")

    if live_cols != preview_cols:
        print("Column mismatch detected.")
        print(f"Live columns   : {live_cols}")
        print(f"Preview columns: {preview_cols}")
        return False

    live_keys = row_key_series(live_df)
    preview_keys = row_key_series(preview_df)

    live_only = live_df.loc[~live_keys.isin(set(preview_keys.tolist()))].copy()
    preview_only = preview_df.loc[~preview_keys.isin(set(live_keys.tolist()))].copy()

    if live_only.empty and preview_only.empty:
        print("Match: identical after normalization and row-order sorting.")
        return True

    print("Mismatch detected.")

    if not live_only.empty:
        print(f"Rows only in live: {len(live_only)}")
        print(live_only.head(max_examples).to_string(index=False))

    if not preview_only.empty:
        print(f"Rows only in preview: {len(preview_only)}")
        print(preview_only.head(max_examples).to_string(index=False))

    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preview-dir",
        default=str(DEFAULT_PREVIEW_DIR),
        help="Directory containing preview CSVs built from case specs.",
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=10,
        help="Maximum number of differing rows to print per side per file.",
    )
    args = parser.parse_args()

    preview_dir = Path(args.preview_dir)
    if not preview_dir.is_absolute():
        preview_dir = ROOT / preview_dir

    all_ok = True
    for filename in FILES:
        live_path = LIVE_DIR / filename
        preview_path = preview_dir / filename
        ok = compare_one_file(live_path, preview_path, max_examples=args.max_examples)
        all_ok = all_ok and ok

    print("\n=== Summary ===")
    if all_ok:
        print("All compared metadata files match.")
    else:
        print("Differences found.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()