from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_COVERAGE_FILE = ROOT / "outputs" / "worldwide" / "worldwide_import_batch_coverage.csv"
DEFAULT_SCORES_FILE = ROOT / "outputs" / "worldwide" / "goods_trade_scores_live.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

REQUIRED_COVERAGE_COLS = [
    "year",
    "batch_id",
    "reporter_id",
    "reporter_iso3",
    "reporter_name",
    "canonical_name",
    "wto_reporter_code",
    "expected_batch_filename",
    "expected_batch_path",
    "file_present",
    "source_family",
    "notes",
]

REQUIRED_SCORE_COLS = [
    "reporter_id",
    "partner_id",
    "trade_value_usd_m",
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


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a canonical acquisition queue for missing WTO bilateral-import reporter batches "
            "from the live import-batch coverage file, prioritized by currently observed trade importance."
        )
    )
    parser.add_argument("--coverage-file", default="", help="Path to worldwide_import_batch_coverage.csv")
    parser.add_argument("--scores-file", default="", help="Path to goods_trade_scores_live.csv")
    parser.add_argument("--out-dir", default="", help="Output directory")
    args = parser.parse_args()

    coverage_file = resolve_path(args.coverage_file, DEFAULT_COVERAGE_FILE)
    scores_file = resolve_path(args.scores_file, DEFAULT_SCORES_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    coverage = pd.read_csv(coverage_file, dtype=str, keep_default_na=False)
    for col in coverage.columns:
        coverage[col] = coverage[col].map(normalize_text)

    require_columns(coverage, REQUIRED_COVERAGE_COLS, coverage_file.name)

    missing = coverage[coverage["file_present"].str.lower() != "yes"].copy()
    missing["year_num"] = pd.to_numeric(missing["year"], errors="coerce")
    missing = missing.sort_values(["reporter_id", "year_num"], ascending=[True, False], kind="stable").reset_index(drop=True)

    observed_trade = pd.DataFrame(
        columns=[
            "reporter_id",
            "observed_partner_trade_usd_m_from_present_reporters",
            "observed_present_reporter_count_importing_from_partner",
        ]
    )

    if scores_file.exists():
        scores = pd.read_csv(scores_file, dtype=str, keep_default_na=False)
        for col in scores.columns:
            scores[col] = scores[col].map(normalize_text)

        require_columns(scores, REQUIRED_SCORE_COLS, scores_file.name)

        scores["trade_value_usd_m_num"] = pd.to_numeric(scores["trade_value_usd_m"], errors="coerce").fillna(0.0)

        observed_trade = (
            scores.groupby("partner_id", dropna=False)
            .agg(
                observed_partner_trade_usd_m_from_present_reporters=("trade_value_usd_m_num", "sum"),
                observed_present_reporter_count_importing_from_partner=("reporter_id", "nunique"),
            )
            .reset_index()
            .rename(columns={"partner_id": "reporter_id"})
        )

        observed_trade["reporter_id"] = observed_trade["reporter_id"].map(normalize_text)
        observed_trade["observed_partner_trade_usd_m_from_present_reporters"] = (
            observed_trade["observed_partner_trade_usd_m_from_present_reporters"].round(3)
        )

    duplicate_right = (
        observed_trade.loc[observed_trade["reporter_id"].duplicated(), "reporter_id"]
        .drop_duplicates()
        .tolist()
    )
    if duplicate_right:
        raise ValueError(
            "Observed trade rows are not unique by reporter_id. Duplicates: "
            + ", ".join(sorted(duplicate_right))
        )

    queue = missing.merge(
        observed_trade,
        on="reporter_id",
        how="left",
        validate="many_to_one",
    )

    queue["observed_partner_trade_usd_m_from_present_reporters"] = pd.to_numeric(
        queue["observed_partner_trade_usd_m_from_present_reporters"], errors="coerce"
    ).fillna(0.0)
    queue["observed_present_reporter_count_importing_from_partner"] = pd.to_numeric(
        queue["observed_present_reporter_count_importing_from_partner"], errors="coerce"
    ).fillna(0).astype(int)

    queue = queue.sort_values(
        [
            "observed_partner_trade_usd_m_from_present_reporters",
            "observed_present_reporter_count_importing_from_partner",
            "reporter_id",
            "year_num",
        ],
        ascending=[False, False, True, False],
        kind="stable",
    ).reset_index(drop=True)

    queue["priority_rank"] = range(1, len(queue) + 1)
    queue["priority_basis"] = queue.apply(
        lambda row: (
            "observed trade from present reporters"
            if float(row["observed_partner_trade_usd_m_from_present_reporters"]) > 0
            else "alphabetical fallback"
        ),
        axis=1,
    )

    queue["source_portal"] = "WTO Data portal"
    queue["source_section"] = "INDICATORS"
    queue["flow"] = "Imports by origin"
    queue["product_scope"] = "TOTAL / All products"
    queue["format"] = "CSV"
    queue["download_status"] = "pending"

    queue = queue[
        [
            "priority_rank",
            "year",
            "batch_id",
            "reporter_id",
            "reporter_iso3",
            "reporter_name",
            "canonical_name",
            "wto_reporter_code",
            "observed_partner_trade_usd_m_from_present_reporters",
            "observed_present_reporter_count_importing_from_partner",
            "priority_basis",
            "source_portal",
            "source_section",
            "flow",
            "product_scope",
            "format",
            "expected_batch_filename",
            "expected_batch_path",
            "download_status",
            "source_family",
            "notes",
        ]
    ]

    out_dir.mkdir(parents=True, exist_ok=True)
    queue_file = out_dir / "worldwide_import_acquisition_queue.csv"
    queue.to_csv(queue_file, index=False)

    print(f"Missing reporter batches queued: {len(queue)}")
    print(f"Wrote: {queue_file}")


if __name__ == "__main__":
    main()
