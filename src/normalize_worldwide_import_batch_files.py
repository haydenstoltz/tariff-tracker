from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_REGISTRY_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"
DEFAULT_BATCH_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "imports_batches"
DEFAULT_ARCHIVE_DIR = DEFAULT_BATCH_DIR / "_archive"
DEFAULT_MANIFEST_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "imports_normalize_manifest.json"

REQUIRED_REGISTRY_COLS = [
    "year",
    "batch_id",
    "reporter_id",
    "reporter_name",
    "wto_reporter_code",
    "expected_batch_filename",
]

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


def load_registry(path: Path) -> tuple[dict[tuple[str, str], dict[str, str]], set[str]]:
    registry = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in registry.columns:
        registry[col] = registry[col].map(normalize_text)

    require_columns(registry, REQUIRED_REGISTRY_COLS, path.name)

    lookup: dict[tuple[str, str], dict[str, str]] = {}
    expected_filenames: set[str] = set()

    for _, row in registry.iterrows():
        year = normalize_text(row["year"])
        code = normalize_wto_code(row["wto_reporter_code"])
        key = (year, code)

        if not year:
            raise ValueError(f"Blank year in {path.name} for reporter_id={row['reporter_id']}")
        if not code:
            raise ValueError(
                f"Blank wto_reporter_code in {path.name} for reporter_id={row['reporter_id']}"
            )
        if key in lookup:
            raise ValueError(
                f"Duplicate year/wto_reporter_code in {path.name}: year={year}, code={row['wto_reporter_code']}"
            )

        expected_filename = normalize_text(row["expected_batch_filename"])
        if not expected_filename:
            raise ValueError(
                f"Blank expected_batch_filename in {path.name} for reporter_id={row['reporter_id']}"
            )

        lookup[key] = {
            "year": year,
            "batch_id": normalize_text(row["batch_id"]),
            "reporter_id": normalize_text(row["reporter_id"]),
            "reporter_name": normalize_text(row["reporter_name"]),
            "expected_batch_filename": expected_filename,
        }
        expected_filenames.add(expected_filename)

    return lookup, expected_filenames


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize grouped WTO bilateral-import batch files into canonical per-reporter files "
            "expected by the worldwide imports pipeline."
        )
    )
    parser.add_argument("--registry-file", default="", help="Path to worldwide_import_batch_registry.csv")
    parser.add_argument("--batch-dir", default="", help="Path to imports_batches directory")
    parser.add_argument("--archive-dir", default="", help="Directory to archive grouped source bundles")
    parser.add_argument("--manifest-file", default="", help="Path to normalization manifest JSON")
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Overwrite existing canonical reporter files if they already exist",
    )
    parser.add_argument(
        "--keep-source-files",
        action="store_true",
        help="Keep grouped source bundle files in place instead of archiving them",
    )
    args = parser.parse_args()

    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    batch_dir = resolve_path(args.batch_dir, DEFAULT_BATCH_DIR)
    archive_dir = resolve_path(args.archive_dir, DEFAULT_ARCHIVE_DIR)
    manifest_file = resolve_path(args.manifest_file, DEFAULT_MANIFEST_FILE)

    batch_dir.mkdir(parents=True, exist_ok=True)

    registry_lookup, expected_filenames = load_registry(registry_file)

    batch_files = sorted([path for path in batch_dir.glob("*.csv") if path.is_file()])
    if not batch_files:
        raise FileNotFoundError(f"No CSV files found in {batch_dir}")

    created_files: list[str] = []
    overwritten_files: list[str] = []
    skipped_existing_files: list[str] = []
    archived_files: list[str] = []
    kept_canonical_files: list[str] = []
    processed_sources: list[dict[str, object]] = []

    for path in batch_files:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

        require_columns(df, REQUIRED_IMPORTS_COLS, path.name)
        years_present = sorted({normalize_text(x) for x in df["year"].tolist() if normalize_text(x)})
        if not years_present:
            raise ValueError(f"No usable year values found in {path.name}")

        df["reporter_code_norm"] = df["reporter_code"].map(normalize_wto_code)
        reporter_codes_present = sorted(
            {code for code in df["reporter_code_norm"].tolist() if code}
        )

        if not reporter_codes_present:
            raise ValueError(f"No usable reporter_code values found in {path.name}")

        unknown_keys = sorted(
            [
                (y, code)
                for y in years_present
                for code in reporter_codes_present
                if not df[(df["year"] == y) & (df["reporter_code_norm"] == code)].empty
                and (y, code) not in registry_lookup
            ]
        )
        if unknown_keys:
            raise ValueError(
                f"{path.name} contains reporter/year keys not present in worldwide_import_batch_registry.csv: {unknown_keys}"
            )

        source_record = {
            "source_file": str(path),
            "years_present": years_present,
            "reporter_codes_present": reporter_codes_present,
            "reporter_ids_present": sorted(
                {
                    registry_lookup[(y, code)]["reporter_id"]
                    for y in years_present
                    for code in reporter_codes_present
                    if (y, code) in registry_lookup
                    and not df[(df["year"] == y) & (df["reporter_code_norm"] == code)].empty
                }
            ),
            "actions": [],
        }

        if path.name in expected_filenames and len(reporter_codes_present) == 1 and len(years_present) == 1:
            year = years_present[0]
            code = reporter_codes_present[0]
            expected_name = registry_lookup[(year, code)]["expected_batch_filename"]
            if path.name != expected_name:
                raise ValueError(
                    f"{path.name} is already canonical-looking but does not match registry expectation {expected_name}"
                )
            kept_canonical_files.append(path.name)
            source_record["actions"].append({"action": "kept_canonical", "filename": path.name})
            processed_sources.append(source_record)
            continue

        for year in years_present:
            for code in reporter_codes_present:
                sub = df[(df["year"] == year) & (df["reporter_code_norm"] == code)].copy()
                if sub.empty:
                    continue

                meta = registry_lookup[(year, code)]

                out_path = batch_dir / meta["expected_batch_filename"]
                already_exists = out_path.exists()

                if already_exists and out_path.resolve() != path.resolve() and not args.overwrite_existing:
                    skipped_existing_files.append(out_path.name)
                    source_record["actions"].append(
                        {
                            "action": "skipped_existing",
                            "filename": out_path.name,
                            "reporter_id": meta["reporter_id"],
                        }
                    )
                    continue

                sub = sub.drop(columns=["reporter_code_norm"])
                sub.to_csv(out_path, index=False)

            if already_exists:
                overwritten_files.append(out_path.name)
                source_record["actions"].append(
                    {
                        "action": "overwrote",
                        "filename": out_path.name,
                        "reporter_id": meta["reporter_id"],
                        "row_count": int(len(sub)),
                    }
                )
            else:
                created_files.append(out_path.name)
                source_record["actions"].append(
                    {
                        "action": "created",
                        "filename": out_path.name,
                        "reporter_id": meta["reporter_id"],
                        "row_count": int(len(sub)),
                    }
                )

        if not args.keep_source_files:
            archive_dir.mkdir(parents=True, exist_ok=True)
            archive_path = archive_dir / path.name
            if archive_path.exists():
                archive_path.unlink()
            shutil.move(str(path), str(archive_path))
            archived_files.append(path.name)
            source_record["actions"].append({"action": "archived_source", "filename": path.name})

        processed_sources.append(source_record)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "registry_file": str(registry_file),
        "batch_dir": str(batch_dir),
        "archive_dir": str(archive_dir),
        "source_file_count": len(batch_files),
        "created_files": sorted(created_files),
        "overwritten_files": sorted(overwritten_files),
        "skipped_existing_files": sorted(set(skipped_existing_files)),
        "kept_canonical_files": sorted(kept_canonical_files),
        "archived_files": sorted(archived_files),
        "processed_sources": processed_sources,
    }
    write_json(manifest_file, manifest)

    print(f"Source files processed: {len(batch_files)}")
    print(f"Canonical files created: {len(created_files)}")
    print(f"Canonical files overwritten: {len(overwritten_files)}")
    print(f"Canonical files skipped_existing: {len(set(skipped_existing_files))}")
    print(f"Canonical files kept as-is: {len(kept_canonical_files)}")
    print(f"Source bundle files archived: {len(archived_files)}")
    print(f"Wrote: {manifest_file}")


if __name__ == "__main__":
    main()