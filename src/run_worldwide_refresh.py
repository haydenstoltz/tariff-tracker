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
            "Manual-refresh worldwide pipeline. "
            "Stages the latest WTO raw exports, ingests target rows, rebuilds live scores, "
            "optionally applies bilateral preference overrides, and exports site data."
        )
    )
    parser.add_argument("--site-data-dir", default="site/data", help="Site data output directory")
    parser.add_argument("--year", default="", help="Optional explicit staged raw year to ingest")
    parser.add_argument("--skip-stage-raw", action="store_true", help="Skip raw inbox staging")
    parser.add_argument("--imports-file", default="", help="Optional explicit raw imports CSV for staging")
    parser.add_argument("--mfn-file", default="", help="Optional explicit raw MFN CSV for staging")
    parser.add_argument(
        "--skip-bilateral-overrides",
        action="store_true",
        help="Skip bilateral preferential override application even if the script exists",
    )
    args = parser.parse_args()

    py = sys.executable

    if not args.skip_stage_raw:
        stage_cmd = [py, "src/stage_worldwide_wto_ttd_raw.py"]
        if args.imports_file.strip():
            stage_cmd += ["--imports-file", args.imports_file]
        if args.mfn_file.strip():
            stage_cmd += ["--mfn-file", args.mfn_file]
        run(stage_cmd)

    run([py, "src/build_country_pair_registry.py"])

    ingest_cmd = [py, "src/ingest_wto_ttd_exports.py"]
    if args.year.strip():
        ingest_cmd += ["--year", args.year.strip()]
    run(ingest_cmd)

    run([py, "src/build_live_goods_trade_scores.py"])

    bilateral_script = ROOT / "src" / "apply_worldwide_bilateral_preferences.py"
    if bilateral_script.exists() and not args.skip_bilateral_overrides:
        run([py, "src/apply_worldwide_bilateral_preferences.py"])

    run([py, "src/export_worldwide_site_data.py", "--site-data-dir", args.site_data_dir])

    print("\nWorldwide refresh completed.")
    print("Local verify command:")
    print("python -m http.server 8000 --directory site")
    print("\nKey files:")
    print("- data/raw/worldwide/wto_ttd/raw_refresh_manifest.json")
    print("- data/raw/worldwide/wto_ttd/wto_ttd_ingest_manifest.json")
    print("- outputs/worldwide/wto_imports_by_partner_targets.csv")
    print("- outputs/worldwide/wto_mfn_reporter_totals.csv")
    print("- outputs/worldwide/goods_trade_scores_live.csv")
    print("- site/data/world_goods_scores.json")
    print("- site/data/world_country_summary.json")
    print("- site/data/world_country_partner_detail.json")
    print("- site/data/world_refresh_manifest.json")


if __name__ == "__main__":
    main()