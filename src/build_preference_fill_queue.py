from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_COVERAGE_FILE = ROOT / "outputs" / "worldwide" / "bilateral_preference_coverage.csv"
DEFAULT_QUEUE_FILE = ROOT / "outputs" / "worldwide" / "preference_fill_queue.csv"
DEFAULT_SUMMARY_FILE = ROOT / "outputs" / "worldwide" / "preference_fill_summary.csv"
DEFAULT_TEMPLATE_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "preference_batches" / "templates"

REQUIRED_COVERAGE_COLS = [
    "year",
    "pair_id",
    "reporter_id",
    "reporter_name",
    "partner_id",
    "partner_name",
    "agreement_id",
    "agreement_name",
    "trade_value_usd_m",
    "override_present",
    "priority_trade_rank",
]

TEMPLATE_COLS = [
    "year",
    "pair_id",
    "reporter_id",
    "partner_id",
    "bilateral_preferential_tariff_pct",
    "bilateral_simple_avg_tariff_pct",
    "source_label",
    "source_url",
    "notes",
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


def safe_slug(text: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in normalize_text(text).upper())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def build_template_rows(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "year": df["year"].map(normalize_text),
            "pair_id": df["pair_id"].map(normalize_text),
            "reporter_id": df["reporter_id"].map(normalize_text),
            "partner_id": df["partner_id"].map(normalize_text),
            "bilateral_preferential_tariff_pct": "",
            "bilateral_simple_avg_tariff_pct": "",
            "source_label": "",
            "source_url": "",
            "notes": "",
        }
    )
    return out[TEMPLATE_COLS]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a prioritized fill queue and ready-to-fill template CSVs for missing bilateral "
            "preferential tariff overrides."
        )
    )
    parser.add_argument("--coverage-file", default="", help="Path to bilateral_preference_coverage.csv")
    parser.add_argument("--queue-file", default="", help="Path to preference_fill_queue.csv")
    parser.add_argument("--summary-file", default="", help="Path to preference_fill_summary.csv")
    parser.add_argument("--template-dir", default="", help="Directory for generated preference templates")
    parser.add_argument("--top-n", type=int, default=0, help="Optional limit for queue rows; 0 means all")
    args = parser.parse_args()

    coverage_file = resolve_path(args.coverage_file, DEFAULT_COVERAGE_FILE)
    queue_file = resolve_path(args.queue_file, DEFAULT_QUEUE_FILE)
    summary_file = resolve_path(args.summary_file, DEFAULT_SUMMARY_FILE)
    template_dir = resolve_path(args.template_dir, DEFAULT_TEMPLATE_DIR)

    coverage = pd.read_csv(coverage_file, dtype=str, keep_default_na=False)
    for col in coverage.columns:
        coverage[col] = coverage[col].map(normalize_text)

    require_columns(coverage, REQUIRED_COVERAGE_COLS, coverage_file.name)

    coverage["trade_value_usd_m_num"] = pd.to_numeric(coverage["trade_value_usd_m"], errors="coerce").fillna(0.0)
    coverage["priority_trade_rank_num"] = pd.to_numeric(coverage["priority_trade_rank"], errors="coerce")

    queue = coverage[
        (coverage["agreement_id"] != "")
        & (coverage["override_present"].str.lower() != "yes")
    ].copy()

    if queue.empty:
        raise ValueError("No missing preferential-tariff override rows found in bilateral_preference_coverage.csv")

    queue = queue.sort_values(
        ["trade_value_usd_m_num", "reporter_id", "partner_id"],
        ascending=[False, True, True],
        kind="stable",
    ).reset_index(drop=True)

    queue["suggested_batch_file"] = queue.apply(
        lambda row: f"preference_{safe_slug(row['reporter_id'])}_{normalize_text(row['year'])}.csv",
        axis=1,
    )

    queue_out = queue[
        [
            "year",
            "pair_id",
            "reporter_id",
            "reporter_name",
            "partner_id",
            "partner_name",
            "agreement_id",
            "agreement_name",
            "trade_value_usd_m",
            "priority_trade_rank",
            "suggested_batch_file",
        ]
    ].copy()

    if args.top_n > 0:
        queue_out = queue_out.head(args.top_n).copy()
        queue = queue.head(args.top_n).copy()

    summary = (
        queue.groupby(["year", "reporter_id", "reporter_name", "agreement_id", "agreement_name"], dropna=False)
        .agg(
            missing_pair_count=("pair_id", "count"),
            covered_trade_value_usd_m=("trade_value_usd_m_num", "sum"),
        )
        .reset_index()
        .sort_values(
            ["covered_trade_value_usd_m", "reporter_id", "agreement_id"],
            ascending=[False, True, True],
            kind="stable",
        )
    )

    queue_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    template_dir.mkdir(parents=True, exist_ok=True)

    queue_out.to_csv(queue_file, index=False)
    summary.to_csv(summary_file, index=False)

    all_template = build_template_rows(queue)
    all_template_path = template_dir / f"preference_template_all_missing_{normalize_text(queue['year'].iloc[0])}.csv"
    all_template.to_csv(all_template_path, index=False)

    for (year, reporter_id), group in queue.groupby(["year", "reporter_id"], dropna=False):
        template = build_template_rows(group)
        template_path = template_dir / f"preference_{safe_slug(reporter_id)}_{normalize_text(year)}.csv"
        template.to_csv(template_path, index=False)

    print(f"Missing override rows in queue: {len(queue_out)}")
    print(f"Wrote: {queue_file}")
    print(f"Wrote: {summary_file}")
    print(f"Wrote: {all_template_path}")
    print(f"Wrote reporter templates in: {template_dir}")


if __name__ == "__main__":
    main()