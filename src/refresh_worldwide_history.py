from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(str(x) for x in cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh a multi-year worldwide historical set one year at a time, "
            "writing per-year archives and updating the live site only for the chosen live year."
        )
    )
    parser.add_argument("--start-year", type=int, default=1996, help="First year inclusive")
    parser.add_argument("--end-year", type=int, default=2026, help="Last year inclusive")
    parser.add_argument("--live-year", type=int, default=2026, help="Year to write to live site/data at the end")
    parser.add_argument("--disable-reporters", default="DEU,FRA,ITA", help="Optional comma-separated reporter_ids to disable")
    parser.add_argument("--skip-source-pull", action="store_true", help="Skip WTO MFN API pulls")
    parser.add_argument("--skip-import-normalize", action="store_true", help="Skip import normalization")
    parser.add_argument("--skip-preference-merge", action="store_true", help="Skip preferential tariff merge")
    parser.add_argument("--skip-bilateral-overrides", action="store_true", help="Skip bilateral overrides")
    parser.add_argument("--skip-import-queue", action="store_true", help="Skip missing-reporter acquisition queue")
    parser.add_argument("--skip-import-checklist", action="store_true", help="Skip missing-reporter checklist")
    parser.add_argument("--allow-partial-imports", action="store_true", help="Allow partial reporter coverage")
    args = parser.parse_args()

    if args.start_year > args.end_year:
        raise ValueError("--start-year must be <= --end-year")
    if not (args.start_year <= args.live_year <= args.end_year):
        raise ValueError("--live-year must fall within the requested range")

    py = sys.executable
    years = [str(y) for y in range(args.start_year, args.end_year + 1) if y != args.live_year]
    years.append(str(args.live_year))

    for year in years:
        cmd = [py, "src/refresh_worldwide_year.py", "--year", year]

        if normalize_text(args.disable_reporters):
            cmd += ["--disable-reporters", normalize_text(args.disable_reporters)]
        if args.skip_source_pull:
            cmd.append("--skip-source-pull")
        if args.skip_import_normalize:
            cmd.append("--skip-import-normalize")
        if args.skip_preference_merge:
            cmd.append("--skip-preference-merge")
        if args.skip_bilateral_overrides:
            cmd.append("--skip-bilateral-overrides")
        if args.skip_import_queue:
            cmd.append("--skip-import-queue")
        if args.skip_import_checklist:
            cmd.append("--skip-import-checklist")
        if args.allow_partial_imports:
            cmd.append("--allow-partial-imports")
        if int(year) == args.live_year:
            cmd.append("--update-live-site")

        run(cmd)

    print("\nHistorical refresh complete.")


if __name__ == "__main__":
    main()