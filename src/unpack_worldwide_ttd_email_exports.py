from __future__ import annotations

import argparse
import io
import json
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SOURCE_DIR = Path.home() / "Downloads"
DEFAULT_STAGING_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "imports_batches"
DEFAULT_MANIFEST_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "email_unpack_manifest.json"

REQUIRED_IMPORT_COLS = [
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
    path = Path(path_str).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def unique_dest_path(dest_dir: Path, filename: str) -> Path:
    candidate = dest_dir / filename
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    counter = 2
    while True:
        alt = dest_dir / f"{stem}__{counter}{suffix}"
        if not alt.exists():
            return alt
        counter += 1


def try_read_csv_bytes(raw_bytes: bytes, label: str) -> tuple[pd.DataFrame, str]:
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

    last_error: Exception | None = None
    for encoding in encodings:
        try:
            frame = pd.read_csv(
                io.BytesIO(raw_bytes),
                dtype=str,
                keep_default_na=False,
                encoding=encoding,
            )
            for col in frame.columns:
                frame[col] = frame[col].map(normalize_text)
            return frame, encoding
        except Exception as exc:
            last_error = exc

    raise ValueError(f"{label}: unable to decode CSV with tried encodings {encodings}: {last_error}")


def validate_import_schema(frame: pd.DataFrame) -> list[str]:
    return [col for col in REQUIRED_IMPORT_COLS if col not in frame.columns]


def infer_years(df: pd.DataFrame) -> list[str]:
    return sorted({normalize_text(x) for x in df["year"].tolist() if normalize_text(x)})


def infer_reporter_codes(df: pd.DataFrame) -> list[str]:
    return sorted({normalize_text(x) for x in df["reporter_code"].tolist() if normalize_text(x)})


def stage_csv_bytes(
    raw_bytes: bytes,
    original_name: str,
    staging_dir: Path,
    allow_multi_year: bool,
    strict_schema: bool,
) -> dict[str, object]:
    frame, encoding_used = try_read_csv_bytes(raw_bytes, original_name)
    missing_cols = validate_import_schema(frame)

    if missing_cols:
        if strict_schema:
            raise ValueError(f"{original_name}: missing required columns {missing_cols}")
        return {
            "source_name": original_name,
            "status": "skipped_non_ttd",
            "encoding_used": encoding_used,
            "missing_columns": missing_cols,
        }

    years = infer_years(frame)
    reporter_codes = infer_reporter_codes(frame)

    if not allow_multi_year and len(years) != 1:
        raise ValueError(
            f"{original_name}: file contains multiple years {years}. "
            "Current pipeline expects one year per emailed export."
        )

    dest_path = unique_dest_path(staging_dir, original_name)
    dest_path.write_bytes(raw_bytes)

    return {
        "source_name": original_name,
        "staged_file": str(dest_path),
        "row_count": int(len(frame)),
        "years": years,
        "reporter_codes": reporter_codes,
        "encoding_used": encoding_used,
        "status": "staged",
    }


def handle_zip_file(
    path: Path,
    staging_dir: Path,
    allow_multi_year: bool,
    strict_schema: bool,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []

    with zipfile.ZipFile(path) as zf:
        members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
        if not members:
            if strict_schema:
                raise ValueError(f"{path.name}: zip contains no CSV files")
            return [
                {
                    "source_container": str(path),
                    "source_name": path.name,
                    "status": "skipped_non_ttd",
                    "reason": "zip contains no CSV files",
                }
            ]

        for member in members:
            raw_bytes = zf.read(member)
            member_name = Path(member).name
            record = stage_csv_bytes(
                raw_bytes=raw_bytes,
                original_name=member_name,
                staging_dir=staging_dir,
                allow_multi_year=allow_multi_year,
                strict_schema=strict_schema,
            )
            record["source_container"] = str(path)
            records.append(record)

    return records


def handle_csv_file(
    path: Path,
    staging_dir: Path,
    allow_multi_year: bool,
    strict_schema: bool,
) -> list[dict[str, object]]:
    raw_bytes = path.read_bytes()
    record = stage_csv_bytes(
        raw_bytes=raw_bytes,
        original_name=path.name,
        staging_dir=staging_dir,
        allow_multi_year=allow_multi_year,
        strict_schema=strict_schema,
    )
    record["source_container"] = str(path)
    return [record]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Unpack WTO TTD emailed CSV or ZIP attachments into the imports_batches inbox, "
            "validating and staging only bilateral-import files."
        )
    )
    parser.add_argument("--source-dir", default="", help="Directory containing downloaded email attachments")
    parser.add_argument("--staging-dir", default="", help="imports_batches staging directory")
    parser.add_argument("--manifest-file", default="", help="Output manifest JSON path")
    parser.add_argument("--glob", default="*", help="Filename glob inside source-dir")
    parser.add_argument("--allow-multi-year", action="store_true", help="Allow multi-year files to stage")
    parser.add_argument(
        "--strict-schema",
        action="store_true",
        help="Treat non-TTD files as hard failures instead of skipping them",
    )
    parser.add_argument(
        "--move-processed-to",
        default="",
        help="Optional directory to move successfully processed source files into after staging",
    )
    args = parser.parse_args()

    source_dir = resolve_path(args.source_dir, DEFAULT_SOURCE_DIR)
    staging_dir = resolve_path(args.staging_dir, DEFAULT_STAGING_DIR)
    manifest_file = resolve_path(args.manifest_file, DEFAULT_MANIFEST_FILE)
    processed_dir = resolve_path(args.move_processed_to, ROOT / args.move_processed_to) if args.move_processed_to.strip() else None

    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_dir}")

    staging_dir.mkdir(parents=True, exist_ok=True)
    if processed_dir:
        processed_dir.mkdir(parents=True, exist_ok=True)

    candidates = sorted(
        [
            path for path in source_dir.glob(args.glob)
            if path.is_file() and path.suffix.lower() in {".csv", ".zip"}
        ]
    )
    if not candidates:
        raise FileNotFoundError(f"No .csv or .zip files matched in {source_dir} with glob={args.glob}")

    manifest_records: list[dict[str, object]] = []

    for path in candidates:
        try:
            if path.suffix.lower() == ".zip":
                records = handle_zip_file(
                    path=path,
                    staging_dir=staging_dir,
                    allow_multi_year=args.allow_multi_year,
                    strict_schema=args.strict_schema,
                )
            else:
                records = handle_csv_file(
                    path=path,
                    staging_dir=staging_dir,
                    allow_multi_year=args.allow_multi_year,
                    strict_schema=args.strict_schema,
                )

            manifest_records.extend(records)

            if processed_dir and any(r.get("status") == "staged" for r in records):
                dest = unique_dest_path(processed_dir, path.name)
                shutil.move(str(path), str(dest))

            print(f"Processed: {path}")

        except Exception as exc:
            manifest_records.append(
                {
                    "source_container": str(path),
                    "source_name": path.name,
                    "status": "error",
                    "error": str(exc),
                }
            )
            print(f"FAILED: {path} -> {exc}")

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_dir": str(source_dir),
        "staging_dir": str(staging_dir),
        "record_count": len(manifest_records),
        "records": manifest_records,
    }
    write_json(manifest_file, manifest)

    staged_count = sum(1 for row in manifest_records if row.get("status") == "staged")
    skipped_count = sum(1 for row in manifest_records if row.get("status") == "skipped_non_ttd")
    err_count = sum(1 for row in manifest_records if row.get("status") == "error")

    print(f"Staged files: {staged_count}")
    print(f"Skipped non-TTD files: {skipped_count}")
    print(f"Errored files: {err_count}")
    print(f"Wrote: {manifest_file}")


if __name__ == "__main__":
    main()