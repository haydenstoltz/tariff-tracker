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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preview-dir",
        default="outputs/spec_preview",
        help="Directory for spec-materialized metadata preview files.",
    )
    parser.add_argument(
        "--cache-file",
        default="data/processed/case_price_cache.csv",
        help="Path for the reusable case price cache.",
    )
    parser.add_argument(
        "--build-dir",
        default="outputs/spec_preview_build",
        help="Root directory for scratch build outputs.",
    )
    args = parser.parse_args()

    preview_dir = args.preview_dir
    cache_file = args.cache_file
    build_dir = Path(args.build_dir)
    chart_dir = str(build_dir / "charts" / "case_studies")
    table_dir = str(build_dir / "tables")

    py = sys.executable

    run([py, "src/materialize_case_specs.py", "--preview-dir", preview_dir])

    run([
        py,
        "src/build_case_price_cache.py",
        "--meta-file",
        f"{preview_dir}/product_case_studies.csv",
        "--seed-prices-file",
        "data/processed/prices_clean.csv",
        "--cache-file",
        cache_file,
    ])

    run([
        py,
        "src/make_product_case_studies.py",
        "--meta-file",
        f"{preview_dir}/product_case_studies.csv",
        "--prices-file",
        cache_file,
        "--out-chart-dir",
        chart_dir,
        "--out-table-dir",
        table_dir,
    ])

    print("\nSpec preview pipeline completed.")
    print(f"Preview metadata: {ROOT / preview_dir}")
    print(f"Price cache: {ROOT / cache_file}")
    print(f"Scratch build outputs: {ROOT / build_dir}")


if __name__ == "__main__":
    main()