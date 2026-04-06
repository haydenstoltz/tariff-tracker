from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_BATCH_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "imports_batches"
DEFAULT_OUT_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "imports_by_partner_latest.csv"
DEFAULT_MANIFEST_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "imports_merge_manifest.json"
DEFAULT_TERRITORIES_FILE = ROOT / "data" / "metadata" / "world" / "customs_territories.csv"
DEFAULT_CODE_MAP_FILE = ROOT / "data" / "metadata" / "world" / "wto_actor_code_map.csv"

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

REQUIRED_TERRITORIES_COLS = [
    "actor_id",
    "display_name",
    "active_flag",
]

REQUIRED_CODE_MAP_COLS = [
    "actor_id",
    "wto_partner_code",
    "canonical_name",
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


def normalize_wto_code(value: object) -> str:
    text = normalize_text(value).upper()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return text
    return str(int(digits))


def require_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


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


def active_reporter_expectations(territories_file: Path, code_map_file: Path) -> tuple[dict[str, dict[str, str]], list[str]]:
    territories = pd.read_csv(territories_file, dtype=str, keep_default_na=False)
    code_map = pd.read_csv(code_map_file, dtype=str, keep_default_na=False)

    for df in [territories, code_map]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    require_columns(territories, REQUIRED_TERRITORIES_COLS, territories_file.name)
    require_columns(code_map, REQUIRED_CODE_MAP_COLS, code_map_file.name)

    territories = territories[territories["active_flag"].str.lower() == "yes"].copy()

    merged = territories.merge(
        code_map[["actor_id", "wto_partner_code", "canonical_name"]],
        on="actor_id",
        how="left",
        validate="one_to_one",
    )

    missing_codes = merged[merged["wto_partner_code"] == ""]
    if not missing_codes.empty:
        raise ValueError(
            "Active territories missing WTO code mappings:\n"
            + missing_codes[["actor_id", "display_name"]].to_string(index=False)
        )

    expectations: dict[str, dict[str, str]] = {}
    actor_ids: list[str] = []

    for _, row in merged.iterrows():
        code_norm = normalize_wto_code(row["wto_partner_code"])
        actor_id = normalize_text(row["actor_id"])
        actor_ids.append(actor_id)
        expectations[code_norm] = {
            "actor_id": actor_id,
            "display_name": normalize_text(row["display_name"]) or normalize_text(row["canonical_name"]),
            "canonical_name": normalize_text(row["canonical_name"]),
            "wto_partner_code": normalize_text(row["wto_partner_code"]),
        }

    return expectations, sorted(actor_ids)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Merge multiple WTO bilateral-imports raw CSV batches into one canonical imports_by_partner_latest.csv, "
            "validate year consistency, and validate active reporter coverage."
        )
    )
    parser.add_argument("--batch-dir", default="", help="Directory containing bilateral-imports batch CSVs")
    parser.add_argument("--out-file", default="", help="Canonical merged bilateral-imports file")
    parser.add_argument("--manifest-file", default="", help="JSON manifest output path")
    parser.add_argument("--territories-file", default="", help="Path to customs_territories.csv")
    parser.add_argument("--code-map-file", default="", help="Path to wto_actor_code_map.csv")
    parser.add_argument("--allow-partial", action="store_true", help="Allow missing active reporters without failing")
    args = parser.parse_args()

    batch_dir = resolve_path(args.batch_dir, DEFAULT_BATCH_DIR)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)
    manifest_file = resolve_path(args.manifest_file, DEFAULT_MANIFEST_FILE)
    territories_file = resolve_path(args.territories_file, DEFAULT_TERRITORIES_FILE)
    code_map_file = resolve_path(args.code_map_file, DEFAULT_CODE_MAP_FILE)

    batch_dir.mkdir(parents=True, exist_ok=True)

    batch_files = sorted(
        [
            path for path in batch_dir.glob("*.csv")
            if path.is_file()
        ]
    )

    if not batch_files:
        raise FileNotFoundError(
            f"No bilateral-import batch CSVs found in {batch_dir}. "
            "Drop reporter-batch WTO TTD bilateral import files into this folder."
        )

    expected_reporters, expected_actor_ids = active_reporter_expectations(territories_file, code_map_file)

    frames: list[pd.DataFrame] = []
    file_manifest: list[dict[str, object]] = []
    years_seen: set[str] = set()
    found_reporter_codes: set[str] = set()

    for path in batch_files:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

        require_columns(df, REQUIRED_IMPORTS_COLS, path.name)

        if df.empty:
            raise ValueError(f"Batch file is empty: {path}")

        year = infer_single_year(df, path.name)
        years_seen.add(year)

        reporter_codes = sorted({normalize_wto_code(x) for x in df["reporter_code"].tolist() if normalize_wto_code(x)})
        found_reporter_codes.update(reporter_codes)

        frames.append(df)

        reporter_actor_ids = [
            expected_reporters[code]["actor_id"] for code in reporter_codes if code in expected_reporters
        ]

        file_manifest.append(
            {
                "file": str(path),
                "row_count": int(len(df)),
                "year": year,
                "reporter_codes_found": reporter_codes,
                "reporter_actor_ids_found": reporter_actor_ids,
            }
        )

    if len(years_seen) != 1:
        raise ValueError(f"Expected one common year across bilateral-import batch files, found: {sorted(years_seen)}")

    merged = pd.concat(frames, ignore_index=True)

    dedupe_cols = [
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
    merged = merged.drop_duplicates(subset=dedupe_cols, keep="first").reset_index(drop=True)

    merged["reporter_code_norm"] = merged["reporter_code"].map(normalize_wto_code)
    present_actor_ids = sorted(
        {
            expected_reporters[code]["actor_id"]
            for code in set(merged["reporter_code_norm"].tolist())
            if code in expected_reporters
        }
    )
    missing_actor_ids = sorted(set(expected_actor_ids) - set(present_actor_ids))

    out_file.parent.mkdir(parents=True, exist_ok=True)
    merged.drop(columns=["reporter_code_norm"]).to_csv(out_file, index=False)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_dir": str(batch_dir),
        "output_file": str(out_file),
        "common_year": sorted(years_seen)[0],
        "source_file_count": len(batch_files),
        "source_files": file_manifest,
        "merged_row_count": int(len(merged)),
        "expected_active_reporters": expected_actor_ids,
        "present_active_reporters": present_actor_ids,
        "missing_active_reporters": missing_actor_ids,
    }
    write_json(manifest_file, manifest)

    print(f"Common year: {sorted(years_seen)[0]}")
    print(f"Source batch files: {len(batch_files)}")
    print(f"Merged rows: {len(merged)}")
    print(f"Wrote: {out_file}")
    print(f"Wrote: {manifest_file}")

    if missing_actor_ids and not args.allow_partial:
        raise ValueError(
            "Missing bilateral-import reporter coverage for active actors: "
            + ", ".join(missing_actor_ids)
        )


if __name__ == "__main__":
    main()