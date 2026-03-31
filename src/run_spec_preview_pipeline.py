from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    if not path.is_absolute():
        path = ROOT / path
    return path


def copy_site_shell(base_site_dir: Path, out_site_dir: Path) -> None:
    if not base_site_dir.exists():
        raise FileNotFoundError(f"Base site directory not found: {base_site_dir}")

    if out_site_dir.exists():
        shutil.rmtree(out_site_dir)

    out_site_dir.mkdir(parents=True, exist_ok=True)

    for item in base_site_dir.iterdir():
        if item.name == "data":
            continue

        dest = out_site_dir / item.name
        if item.is_dir():
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)

    (out_site_dir / "data").mkdir(parents=True, exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preview-dir",
        default="outputs/spec_preview",
        help="Directory for spec-materialized preview metadata files.",
    )
    parser.add_argument(
        "--cache-file",
        default="data/processed/case_price_cache.csv",
        help="Reusable case price cache path.",
    )
    parser.add_argument(
        "--build-dir",
        default="outputs/spec_preview_build",
        help="Scratch build output root.",
    )
    parser.add_argument(
        "--preview-site-root",
        default="outputs/spec_site_preview/site",
        help="Runnable preview site root.",
    )
    parser.add_argument(
        "--base-site-dir",
        default="site",
        help="Current live site shell directory to copy from.",
    )
    args = parser.parse_args()

    preview_dir = resolve_path(args.preview_dir)
    cache_file = resolve_path(args.cache_file)
    build_dir = resolve_path(args.build_dir)
    preview_site_root = resolve_path(args.preview_site_root)
    base_site_dir = resolve_path(args.base_site_dir)

    chart_dir = build_dir / "charts" / "case_studies"
    table_dir = build_dir / "tables"
    preview_site_data_dir = preview_site_root / "data"

    py = sys.executable

    copy_site_shell(base_site_dir, preview_site_root)

    run([py, "src/materialize_case_specs.py", "--preview-dir", str(preview_dir)])

    run(
        [
            py,
            "src/build_case_price_cache.py",
            "--meta-file",
            str(preview_dir / "product_case_studies.csv"),
            "--seed-prices-file",
            "data/processed/prices_clean.csv",
            "--cache-file",
            str(cache_file),
        ]
    )

    run(
        [
            py,
            "src/make_product_case_studies.py",
            "--meta-file",
            str(preview_dir / "product_case_studies.csv"),
            "--prices-file",
            str(cache_file),
            "--out-chart-dir",
            str(chart_dir),
            "--out-table-dir",
            str(table_dir),
        ]
    )

    run(
        [
            py,
            "src/export_site_data.py",
            "--events-file",
            "data/metadata/tariff_events_master.csv",
            "--coverage-file",
            "data/metadata/event_case_coverage.csv",
            "--case-meta-file",
            str(preview_dir / "site_cases.csv"),
            "--case-stage-file",
            str(preview_dir / "case_stage_map.csv"),
            "--event-case-map-file",
            str(preview_dir / "event_case_map.csv"),
            "--final-summary-file",
            str(table_dir / "final_case_summary_table.csv"),
            "--panel-file",
            str(table_dir / "product_case_studies_panel.csv"),
            "--out-dir",
            str(preview_site_data_dir),
        ]
    )

    print("\nSpec preview pipeline completed.")
    print(f"Preview metadata: {preview_dir}")
    print(f"Price cache: {cache_file}")
    print(f"Scratch build outputs: {build_dir}")
    print(f"Runnable preview site: {preview_site_root}")
    print("\nLocal verify command:")
    print(f"python -m http.server 8000 --directory {preview_site_root}")


if __name__ == "__main__":
    main()