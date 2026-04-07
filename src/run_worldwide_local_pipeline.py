from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SOURCE_DIR = Path.home() / "Downloads" / "WTO_TTD_IMPORTS"
DEFAULT_DOWNLOAD_ARCHIVE_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "download_source_archive"
DEFAULT_BATCH_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "imports_batches"
DEFAULT_REGISTRY_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"
DEFAULT_OUTPUTS_ROOT = ROOT / "outputs" / "worldwide"
DEFAULT_SITE_DATA_DIR = ROOT / "site" / "data"

REQUIRED_REGISTRY_COLS = [
    "year",
    "reporter_id",
    "expected_batch_filename",
]


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def resolve_path(path_str: str, default_path: Path) -> Path:
    if not path_str.strip():
        return default_path
    path = Path(path_str).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path


def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(str(x) for x in cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def read_registry(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in df.columns:
        df[col] = df[col].map(normalize_text)

    missing = [c for c in REQUIRED_REGISTRY_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {path.name}: {missing}")

    return df


def source_files_present(source_dir: Path, glob_pattern: str) -> list[Path]:
    if not source_dir.exists():
        return []
    return sorted(
        [
            path for path in source_dir.glob(glob_pattern)
            if path.is_file() and path.suffix.lower() in {".csv", ".zip"}
        ]
    )


def choose_build_year(
    registry_file: Path,
    batch_dir: Path,
    disabled_reporters: set[str],
) -> tuple[str, list[dict[str, object]], bool]:
    registry = read_registry(registry_file)

    if disabled_reporters:
        registry = registry[~registry["reporter_id"].str.upper().isin(disabled_reporters)].copy()

    if registry.empty:
        raise ValueError("No registry rows remain after applying disabled reporters")

    present_files = {path.name for path in batch_dir.glob("imports_*.csv") if path.is_file()}

    summary_rows: list[dict[str, object]] = []
    for year, group in registry.groupby("year", sort=True):
        expected_files = set(group["expected_batch_filename"].tolist())
        expected_count = len(expected_files)
        present_count = sum(1 for name in expected_files if name in present_files)
        missing_count = expected_count - present_count
        summary_rows.append(
            {
                "year": year,
                "expected_count": expected_count,
                "present_count": present_count,
                "missing_count": missing_count,
                "is_complete": expected_count > 0 and present_count == expected_count,
            }
        )

    if not summary_rows:
        raise ValueError("No year rows were available for build-year selection")

    complete_rows = [row for row in summary_rows if row["is_complete"]]
    if complete_rows:
        chosen = max(complete_rows, key=lambda row: int(str(row["year"])))
        return str(chosen["year"]), sorted(summary_rows, key=lambda r: int(str(r["year"]))), True

    covered_rows = [row for row in summary_rows if int(row["present_count"]) > 0]
    if not covered_rows:
        raise ValueError(
            "No canonical imports reporter files were found in imports_batches for any year. "
            "Unpack and normalize WTO files first."
        )

    chosen = max(
        covered_rows,
        key=lambda row: (int(row["present_count"]), int(str(row["year"]))),
    )
    return str(chosen["year"]), sorted(summary_rows, key=lambda r: int(str(r["year"]))), False


def print_year_summary(summary_rows: list[dict[str, object]]) -> None:
    print("\nYear coverage summary:")
    for row in summary_rows:
        print(
            f"- {row['year']}: present={row['present_count']} "
            f"expected={row['expected_count']} missing={row['missing_count']} "
            f"complete={'yes' if row['is_complete'] else 'no'}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "One-command local worldwide pipeline: optionally unpack new WTO downloads, "
            "normalize canonical reporter-year imports, choose the newest website-ready year, "
            "refresh live site data, sync top-level coverage/queue files, and optionally start localhost."
        )
    )
    parser.add_argument("--source-dir", default="", help="Download folder containing new WTO CSV/ZIP files")
    parser.add_argument("--source-glob", default="*", help="Glob pattern inside source-dir")
    parser.add_argument(
        "--download-archive-dir",
        default="",
        help="Directory to move processed source download files into after unpack",
    )
    parser.add_argument("--registry-file", default="", help="Path to worldwide_import_batch_registry.csv")
    parser.add_argument("--batch-dir", default="", help="Path to canonical imports_batches directory")
    parser.add_argument("--outputs-root", default="", help="outputs/worldwide root")
    parser.add_argument("--site-data-dir", default="", help="site/data directory")
    parser.add_argument(
        "--disable-reporters",
        default="DEU,FRA,ITA",
        help="Comma-separated reporter actor_ids excluded from the active live build",
    )
    parser.add_argument(
        "--skip-unpack",
        action="store_true",
        help="Skip the unpack step even if files are present in the downloads folder",
    )
    parser.add_argument(
        "--skip-normalize",
        action="store_true",
        help="Skip the normalize step",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Localhost port for python -m http.server",
    )
    parser.add_argument(
        "--no-serve",
        action="store_true",
        help="Do not launch localhost at the end",
    )
    args = parser.parse_args()

    py = sys.executable

    source_dir = resolve_path(args.source_dir, DEFAULT_SOURCE_DIR)
    download_archive_dir = resolve_path(args.download_archive_dir, DEFAULT_DOWNLOAD_ARCHIVE_DIR)
    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    batch_dir = resolve_path(args.batch_dir, DEFAULT_BATCH_DIR)
    outputs_root = resolve_path(args.outputs_root, DEFAULT_OUTPUTS_ROOT)
    site_data_dir = resolve_path(args.site_data_dir, DEFAULT_SITE_DATA_DIR)

    disabled_reporters = {
        normalize_text(x).upper()
        for x in normalize_text(args.disable_reporters).split(",")
        if normalize_text(x)
    }

    if not args.skip_unpack:
        matches = source_files_present(source_dir, args.source_glob)
        if matches:
            run(
                [
                    py,
                    "src/unpack_worldwide_ttd_email_exports.py",
                    "--source-dir",
                    str(source_dir),
                    "--glob",
                    args.source_glob,
                    "--allow-multi-year",
                    "--move-processed-to",
                    str(download_archive_dir),
                ]
            )
        else:
            print(f"\nNo new source files matched in {source_dir}; skipping unpack.")

    if not args.skip_normalize:
        run([py, "src/normalize_worldwide_import_batch_files.py"])

    chosen_year, summary_rows, used_complete_year = choose_build_year(
        registry_file=registry_file,
        batch_dir=batch_dir,
        disabled_reporters=disabled_reporters,
    )
    print_year_summary(summary_rows)

    if used_complete_year:
        print(f"\nSelected build year: {chosen_year} (newest complete enabled-reporter year)")
    else:
        print(
            f"\nSelected build year: {chosen_year} "
            f"(no complete year available; using best-covered year)"
        )

    run(
        [
            py,
            "src/run_worldwide_refresh.py",
            "--year",
            chosen_year,
            "--allow-partial-imports",
            "--skip-import-normalize",
            "--disable-reporters",
            ",".join(sorted(disabled_reporters)),
        ]
    )

    run(
        [
            py,
            "src/build_worldwide_import_batch_coverage.py",
            "--year",
            chosen_year,
            "--out-dir",
            str(outputs_root),
        ]
    )

    run(
        [
            py,
            "src/build_worldwide_import_acquisition_queue.py",
            "--coverage-file",
            str(outputs_root / "worldwide_import_batch_coverage.csv"),
            "--scores-file",
            str(outputs_root / "goods_trade_scores_live.csv"),
            "--out-dir",
            str(outputs_root),
        ]
    )

    run(
        [
            py,
            "src/export_worldwide_site_data.py",
            "--site-data-dir",
            str(site_data_dir),
        ]
    )

    print("\nLocal pipeline completed.")
    print(f"Live website year: {chosen_year}")
    print(f"Live site data: {site_data_dir}")

    if not args.no_serve:
        run(
            [
                py,
                "-m",
                "http.server",
                str(args.port),
                "--directory",
                "site",
            ]
        )
    else:
        print("\nVerify locally with:")
        print(f"python -m http.server {args.port} --directory site")


if __name__ == "__main__":
    main()