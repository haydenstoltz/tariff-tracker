from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_RAW_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd"
DEFAULT_OUTPUTS_ROOT = ROOT / "outputs" / "worldwide"
DEFAULT_SITE_DATA_DIR = ROOT / "site" / "data"
DEFAULT_HISTORY_SITE_ROOT = ROOT / "site" / "data" / "history" / "world"


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
    return str(value).strip()


def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(str(x) for x in cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def copy_if_exists(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def update_available_years(history_root: Path) -> None:
    years = sorted(
        [
            path.name
            for path in history_root.iterdir()
            if path.is_dir() and path.name.isdigit() and len(path.name) == 4
        ],
        reverse=True,
    )
    out_file = history_root / "available_years.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"years": years}, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build one selected worldwide processed year from the multi-year raw acquisition layer, "
            "archive outputs under by_year/<YEAR>, and optionally update the live site snapshot."
        )
    )
    parser.add_argument("--year", required=True, help="Target year, e.g. 2026")
    parser.add_argument("--raw-dir", default="", help="data/raw/worldwide/wto_ttd root")
    parser.add_argument("--outputs-root", default="", help="outputs/worldwide root")
    parser.add_argument("--site-data-dir", default="", help="Live site/data directory")
    parser.add_argument("--history-site-root", default="", help="History site snapshot root")
    parser.add_argument("--disable-reporters", default="DEU,FRA,ITA", help="Optional comma-separated reporter_ids to disable")
    parser.add_argument("--skip-source-pull", action="store_true", help="Skip WTO MFN API pull for this year")
    parser.add_argument("--skip-import-normalize", action="store_true", help="Skip normalization of grouped import bundles")
    parser.add_argument("--skip-preference-merge", action="store_true", help="Skip preferential tariff merge")
    parser.add_argument("--skip-bilateral-overrides", action="store_true", help="Skip bilateral overrides")
    parser.add_argument("--skip-import-queue", action="store_true", help="Skip missing-reporter acquisition queue")
    parser.add_argument("--skip-import-checklist", action="store_true", help="Skip missing-reporter checklist")
    parser.add_argument("--allow-partial-imports", action="store_true", help="Allow partial reporter coverage")
    parser.add_argument("--update-live-site", action="store_true", help="Write this year to site/data as the live snapshot")
    args = parser.parse_args()

    year = normalize_text(args.year)
    if not year.isdigit() or len(year) != 4:
        raise ValueError(f"--year must be a four-digit year, got: {year}")

    raw_dir = resolve_path(args.raw_dir, DEFAULT_RAW_DIR)
    outputs_root = resolve_path(args.outputs_root, DEFAULT_OUTPUTS_ROOT)
    site_data_dir = resolve_path(args.site_data_dir, DEFAULT_SITE_DATA_DIR)
    history_site_root = resolve_path(args.history_site_root, DEFAULT_HISTORY_SITE_ROOT)

    py = sys.executable

    year_out_dir = outputs_root / "by_year" / year
    history_year_dir = history_site_root / year
    year_out_dir.mkdir(parents=True, exist_ok=True)
    history_year_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_source_pull:
        source_pull_cmd = [py, "src/pull_worldwide_source_extracts.py", "--year", year]
        if normalize_text(args.disable_reporters):
            source_pull_cmd += ["--disable-reporters", normalize_text(args.disable_reporters)]
        run(source_pull_cmd)

    if not args.skip_import_normalize:
        run([py, "src/normalize_worldwide_import_batch_files.py"])

    run(
        [
            py,
            "src/build_worldwide_import_batch_coverage.py",
            "--year",
            year,
            "--out-dir",
            str(year_out_dir),
        ]
    )

    merge_cmd = [
        py,
        "src/merge_worldwide_bilateral_imports_batches.py",
        "--year",
        year,
        "--out-file",
        str(raw_dir / "inbox" / "imports_by_partner_latest.csv"),
        "--manifest-file",
        str(year_out_dir / "imports_merge_manifest.json"),
    ]
    if args.allow_partial_imports:
        merge_cmd.append("--allow-partial")
    run(merge_cmd)

    run(
        [
            py,
            "src/stage_worldwide_wto_ttd_raw.py",
            "--imports-file",
            str(raw_dir / "inbox" / "imports_by_partner_latest.csv"),
            "--mfn-file",
            str(raw_dir / "inbox" / "mfn_applied_total_latest.csv"),
            "--manifest-file",
            str(year_out_dir / "raw_refresh_manifest.json"),
        ]
    )

    run([py, "src/build_country_pair_registry.py"])

    pull_targets_cmd = [py, "src/build_worldwide_pull_targets.py", "--year", year]
    if normalize_text(args.disable_reporters):
        pull_targets_cmd += ["--disable-reporters", normalize_text(args.disable_reporters)]
    run(pull_targets_cmd)

    ingest_cmd = [
        py,
        "src/ingest_wto_ttd_exports.py",
        "--year",
        year,
        "--manifest-file",
        str(year_out_dir / "wto_ttd_ingest_manifest.json"),
    ]
    if args.allow_partial_imports:
        ingest_cmd.append("--allow-partial-imports")
    run(ingest_cmd)

    score_cmd = [py, "src/build_live_goods_trade_scores.py"]
    if args.allow_partial_imports:
        score_cmd.append("--allow-partial-imports")
    run(score_cmd)

    if not args.skip_preference_merge:
        run([py, "src/merge_worldwide_preferential_tariff_batches.py", "--year", year])

    bilateral_script = ROOT / "src" / "apply_worldwide_bilateral_preferences.py"
    if bilateral_script.exists() and not args.skip_bilateral_overrides:
        run([py, "src/apply_worldwide_bilateral_preferences.py"])

    if not args.skip_import_queue:
        run(
            [
                py,
                "src/build_worldwide_import_acquisition_queue.py",
                "--coverage-file",
                str(year_out_dir / "worldwide_import_batch_coverage.csv"),
                "--scores-file",
                str(outputs_root / "goods_trade_scores_live.csv"),
                "--out-dir",
                str(year_out_dir),
            ]
        )

    if not args.skip_import_checklist:
        queue_file = year_out_dir / "worldwide_import_acquisition_queue.csv"
        if queue_file.exists():
            run(
                [
                    py,
                    "src/build_worldwide_import_download_checklist.py",
                    "--queue-file",
                    str(queue_file),
                    "--out-file",
                    str(year_out_dir / "worldwide_import_download_checklist.md"),
                ]
            )

    run([py, "src/export_worldwide_site_data.py", "--site-data-dir", str(history_year_dir)])
    update_available_years(history_site_root)

    if args.update_live_site:
        run([py, "src/export_worldwide_site_data.py", "--site-data-dir", str(site_data_dir)])

    archive_names = [
        "country_pair_registry.csv",
        "wto_imports_by_partner_targets.csv",
        "wto_imports_by_partner_targets.json",
        "wto_mfn_reporter_totals.csv",
        "wto_mfn_reporter_totals.json",
        "goods_score_inputs_live.csv",
        "goods_trade_scores_live.csv",
        "goods_trade_scores_live.json",
    ]
    for name in archive_names:
        copy_if_exists(outputs_root / name, year_out_dir / name)

    copy_if_exists(raw_dir / "source_pull_manifest.json", year_out_dir / "source_pull_manifest.json")
    copy_if_exists(raw_dir / "email_unpack_manifest.json", year_out_dir / "email_unpack_manifest.json")
    copy_if_exists(raw_dir / "imports_normalize_manifest.json", year_out_dir / "imports_normalize_manifest.json")
    copy_if_exists(raw_dir / "preference_merge_manifest.json", year_out_dir / "preference_merge_manifest.json")

    print(f"\nYear refresh complete: {year}")
    print(f"Archived outputs: {year_out_dir}")
    print(f"Archived site snapshot: {history_year_dir}")
    if args.update_live_site:
        print(f"Live site updated at: {site_data_dir}")


if __name__ == "__main__":
    main()