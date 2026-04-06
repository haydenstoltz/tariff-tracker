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
DEFAULT_REGISTRY_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"

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

REQUIRED_BATCH_REGISTRY_COLS = [
    "year",
    "batch_id",
    "reporter_id",
    "reporter_name",
    "wto_reporter_code",
    "expected_batch_filename",
    "acquisition_status",
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


def enabled_reporter_expectations(registry_file: Path) -> tuple[dict[str, dict[str, str]], list[str], list[str]]:
    registry = pd.read_csv(registry_file, dtype=str, keep_default_na=False)

    for col in registry.columns:
        registry[col] = registry[col].map(normalize_text)

    require_columns(registry, REQUIRED_BATCH_REGISTRY_COLS, registry_file.name)

    expected_files: list[str] = []
    expectations: dict[str, dict[str, str]] = {}
    actor_ids: list[str] = []

    registry = registry.sort_values(["year", "reporter_id"], kind="stable").reset_index(drop=True)

    if registry.empty:
        raise ValueError("No rows found in worldwide_import_batch_registry.csv")

    duplicate_reporters = registry.duplicated(subset=["year", "reporter_id"], keep=False)
    if duplicate_reporters.any():
        raise ValueError(
            "Duplicate reporter/year rows found in worldwide_import_batch_registry.csv:\n"
            + registry.loc[duplicate_reporters, ["year", "reporter_id", "expected_batch_filename"]].to_string(index=False)
        )

    duplicate_files = registry.duplicated(subset=["expected_batch_filename"], keep=False)
    if duplicate_files.any():
        raise ValueError(
            "Duplicate expected_batch_filename rows found in worldwide_import_batch_registry.csv:\n"
            + registry.loc[duplicate_files, ["year", "reporter_id", "expected_batch_filename"]].to_string(index=False)
        )

    for _, row in registry.iterrows():
        code_norm = normalize_wto_code(row["wto_reporter_code"])
        actor_id = normalize_text(row["reporter_id"])
        actor_ids.append(actor_id)
        expected_files.append(normalize_text(row["expected_batch_filename"]))
        expectations[code_norm] = {
            "actor_id": actor_id,
            "display_name": normalize_text(row["reporter_name"]),
            "wto_reporter_code": normalize_text(row["wto_reporter_code"]),
            "expected_batch_filename": normalize_text(row["expected_batch_filename"]),
            "batch_id": normalize_text(row["batch_id"]),
        }

    return expectations, sorted(actor_ids), sorted(expected_files)


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
    parser.add_argument("--registry-file", default="", help="Path to worldwide_import_batch_registry.csv")
    parser.add_argument("--allow-partial", action="store_true", help="Allow missing active reporters without failing")
    args = parser.parse_args()

    batch_dir = resolve_path(args.batch_dir, DEFAULT_BATCH_DIR)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)
    manifest_file = resolve_path(args.manifest_file, DEFAULT_MANIFEST_FILE)
    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)

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
    
        found_filenames = sorted([path.name for path in batch_files])

    unexpected_filenames = sorted(set(found_filenames) - set(expected_filenames))
    if unexpected_filenames:
        raise ValueError(
            "Unexpected bilateral-import batch files found:\n"
            + "\n".join(unexpected_filenames)
        )

    missing_filenames = sorted(set(expected_filenames) - set(found_filenames))

    expected_reporters, expected_actor_ids, expected_filenames = enabled_reporter_expectations(registry_file)

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
        "registry_file": str(registry_file),
        "expected_enabled_reporters": expected_actor_ids,
        "present_enabled_reporters": present_actor_ids,
        "missing_enabled_reporters": missing_actor_ids,
        "expected_batch_filenames": expected_filenames,
        "found_batch_filenames": found_filenames,
        "missing_batch_filenames": missing_filenames,
    }
    write_json(manifest_file, manifest)

    print(f"Common year: {sorted(years_seen)[0]}")
    print(f"Source batch files: {len(batch_files)}")
    print(f"Merged rows: {len(merged)}")
    print(f"Wrote: {out_file}")
    print(f"Wrote: {manifest_file}")

    if (missing_actor_ids or missing_filenames) and not args.allow_partial:
        message_parts = []
        if missing_actor_ids:
            message_parts.append(
                "Missing bilateral-import reporter coverage for enabled reporters: "
                + ", ".join(missing_actor_ids)
            )
        if missing_filenames:
            message_parts.append(
                "Missing expected bilateral-import batch files: "
                + ", ".join(missing_filenames)
            )
        raise ValueError("\n".join(message_parts))


if __name__ == "__main__":
    main()