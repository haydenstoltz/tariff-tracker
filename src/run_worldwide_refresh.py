from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Worldwide refresh pipeline with automatic MFN pulls, bilateral-import batch merge, "
            "raw staging, auto-generated pair targets, ingest, scoring, preferential-tariff batch merge, "
            "bilateral overrides, and site export."
        )
    )
    parser.add_argument("--site-data-dir", default="site/data", help="Site data output directory")
    parser.add_argument("--year", default="", help="Optional explicit raw year to ingest")
    parser.add_argument("--skip-source-pull", action="store_true", help="Skip WTO MFN API pulls")
    parser.add_argument("--skip-import-normalize", action="store_true", help="Skip normalization of grouped bilateral-import source bundles")
    parser.add_argument("--skip-import-coverage", action="store_true", help="Skip bilateral-import coverage report generation")
    parser.add_argument("--skip-import-queue", action="store_true", help="Skip missing-reporter import acquisition queue generation")
    parser.add_argument("--skip-import-checklist", action="store_true", help="Skip Markdown checklist generation for missing reporter downloads")
    parser.add_argument("--skip-import-merge", action="store_true", help="Skip bilateral-import batch merge")
    parser.add_argument("--skip-stage-raw", action="store_true", help="Skip raw staging")
    parser.add_argument("--skip-preference-merge", action="store_true", help="Skip preferential-tariff batch merge")
    parser.add_argument("--skip-bilateral-overrides", action="store_true", help="Skip bilateral override layer")
    parser.add_argument("--imports-file", default="", help="Optional explicit bilateral imports raw CSV for staging")
    parser.add_argument("--allow-partial-imports", action="store_true", help="Allow missing active import reporters during merge")
    parser.add_argument(
        "--disable-reporters",
        default="DEU,FRA,ITA",
        help="Comma-separated reporter actor_ids to disable when rebuilding pair pull targets",
    )
    args = parser.parse_args()

    py = sys.executable

    if not args.skip_source_pull:
        run([py, "src/pull_worldwide_source_extracts.py"])

    if not args.skip_stage_raw:
        if not args.imports_file.strip():
            if not args.skip_import_normalize:
                run([py, "src/normalize_worldwide_import_batch_files.py"])

            if not args.skip_import_coverage:
                run([py, "src/build_worldwide_import_batch_coverage.py"])

            if not args.skip_import_queue:
                run([py, "src/build_worldwide_import_acquisition_queue.py"])

            if not args.skip_import_checklist:
                run([py, "src/build_worldwide_import_download_checklist.py"])

            if not args.skip_import_merge:
                merge_cmd = [py, "src/merge_worldwide_bilateral_imports_batches.py"]
                if args.allow_partial_imports:
                    merge_cmd.append("--allow-partial")
                run(merge_cmd)

        stage_cmd = [py, "src/stage_worldwide_wto_ttd_raw.py"]
        if args.imports_file.strip():
            stage_cmd += ["--imports-file", args.imports_file]
        run(stage_cmd)

    run([py, "src/build_country_pair_registry.py"])

    pull_targets_cmd = [py, "src/build_worldwide_pull_targets.py"]
    if args.year.strip():
        pull_targets_cmd += ["--year", args.year.strip()]
    if args.disable_reporters.strip():
        pull_targets_cmd += ["--disable-reporters", args.disable_reporters.strip()]
    run(pull_targets_cmd)

    ingest_cmd = [py, "src/ingest_wto_ttd_exports.py"]
    if args.year.strip():
        ingest_cmd += ["--year", args.year.strip()]
    if args.allow_partial_imports:
        ingest_cmd.append("--allow-partial-imports")
    run(ingest_cmd)

    score_cmd = [py, "src/build_live_goods_trade_scores.py"]
    if args.allow_partial_imports:
        score_cmd.append("--allow-partial-imports")
    run(score_cmd)

    if not args.skip_preference_merge:
        pref_cmd = [py, "src/merge_worldwide_preferential_tariff_batches.py"]
        if args.year.strip():
            pref_cmd += ["--year", args.year.strip()]
        run(pref_cmd)

    bilateral_script = ROOT / "src" / "apply_worldwide_bilateral_preferences.py"
    if bilateral_script.exists() and not args.skip_bilateral_overrides:
        run([py, "src/apply_worldwide_bilateral_preferences.py"])

    run([py, "src/export_worldwide_site_data.py", "--site-data-dir", args.site_data_dir])

    print("\nWorldwide refresh completed.")
    print("Local verify command:")
    print("python -m http.server 8000 --directory site")
    print("\nKey files:")
    print("- data/raw/worldwide/wto_ttd/source_pull_manifest.json")
    print("- data/raw/worldwide/wto_ttd/imports_normalize_manifest.json")
    print("- data/raw/worldwide/wto_ttd/imports_merge_manifest.json")
    print("- data/raw/worldwide/wto_ttd/preference_merge_manifest.json")
    print("- data/raw/worldwide/wto_ttd/raw_refresh_manifest.json")
    print("- data/raw/worldwide/wto_ttd/wto_ttd_ingest_manifest.json")
    print("- data/metadata/world/pair_pull_targets.csv")
    print("- data/metadata/worldwide_bilateral_preferences.csv")
    print("- outputs/worldwide/worldwide_import_batch_coverage.csv")
    print("- outputs/worldwide/worldwide_import_batch_missing.csv")
    print("- outputs/worldwide/worldwide_import_acquisition_queue.csv")
    print("- outputs/worldwide/worldwide_import_download_checklist.md")
    print("- outputs/worldwide/bilateral_preference_coverage.csv")
    print("- outputs/worldwide/wto_imports_by_partner_targets.csv")
    print("- outputs/worldwide/wto_mfn_reporter_totals.csv")
    print("- outputs/worldwide/goods_trade_scores_live.csv")
    print("- site/data/world_goods_scores.json")
    print("- site/data/world_country_summary.json")
    print("- site/data/world_country_partner_detail.json")
    print("- site/data/world_refresh_manifest.json")


if __name__ == "__main__":
    main()