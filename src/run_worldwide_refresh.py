from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(str(x) for x in cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh the live worldwide build for one selected year and archive that year's outputs."
        )
    )
    parser.add_argument("--site-data-dir", default="site/data", help="Live site data directory")
    parser.add_argument("--year", default="2026", help="Target year for the live build")
    parser.add_argument("--skip-source-pull", action="store_true", help="Skip WTO MFN API pulls")
    parser.add_argument("--skip-wits-pull", action="store_true", help="Skip WITS bilateral tariff/agreement pull")
    parser.add_argument("--skip-import-normalize", action="store_true", help="Skip normalization of grouped bilateral-import source bundles")
    parser.add_argument("--skip-import-queue", action="store_true", help="Skip missing-reporter import acquisition queue generation")
    parser.add_argument("--skip-import-checklist", action="store_true", help="Skip Markdown checklist generation for missing reporter downloads")
    parser.add_argument("--skip-preference-merge", action="store_true", help="Skip preferential-tariff batch merge")
    parser.add_argument("--skip-bilateral-overrides", action="store_true", help="Skip bilateral override layer")
    parser.add_argument("--allow-partial-imports", action="store_true", help="Allow missing active import reporters during merge")
    parser.add_argument(
        "--disable-reporters",
        default="",
        help="Comma-separated reporter actor_ids to disable when rebuilding pair pull targets",
    )
    args = parser.parse_args()

    py = sys.executable
    cmd = [
        py,
        "src/refresh_worldwide_year.py",
        "--year",
        args.year,
        "--site-data-dir",
        args.site_data_dir,
        "--update-live-site",
    ]

    if args.skip_source_pull:
        cmd.append("--skip-source-pull")
    if args.skip_wits_pull:
        cmd.append("--skip-wits-pull")
    if args.skip_import_normalize:
        cmd.append("--skip-import-normalize")
    if args.skip_import_queue:
        cmd.append("--skip-import-queue")
    if args.skip_import_checklist:
        cmd.append("--skip-import-checklist")
    if args.skip_preference_merge:
        cmd.append("--skip-preference-merge")
    if args.skip_bilateral_overrides:
        cmd.append("--skip-bilateral-overrides")
    if args.allow_partial_imports:
        cmd.append("--allow-partial-imports")
    if args.disable_reporters.strip():
        cmd += ["--disable-reporters", args.disable_reporters.strip()]

    run(cmd)


if __name__ == "__main__":
    main()
