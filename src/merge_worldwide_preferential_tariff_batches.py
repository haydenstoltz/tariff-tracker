from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_BATCH_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "inbox" / "preference_batches"
DEFAULT_AGREEMENTS_FILE = ROOT / "data" / "metadata" / "world" / "trade_agreements.csv"
DEFAULT_SCORES_FILE = ROOT / "outputs" / "worldwide" / "goods_trade_scores_live.csv"
DEFAULT_OUT_FILE = ROOT / "data" / "metadata" / "worldwide_bilateral_preferences.csv"
DEFAULT_COVERAGE_FILE = ROOT / "outputs" / "worldwide" / "bilateral_preference_coverage.csv"
DEFAULT_MANIFEST_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "preference_merge_manifest.json"

REQUIRED_AGREEMENT_COLS = [
    "reporter_id",
    "partner_id",
    "agreement_id",
    "agreement_name",
    "status",
    "in_force_date",
]

REQUIRED_SCORE_COLS = [
    "year",
    "pair_id",
    "reporter_id",
    "reporter_name",
    "partner_id",
    "partner_name",
    "trade_value_usd_m",
    "agreement_id",
    "agreement_name",
    "rta_in_force",
]

REQUIRED_BATCH_COLS = [
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


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def latest_scored_year(scores: pd.DataFrame) -> str:
    years = sorted({normalize_text(x) for x in scores["year"].tolist() if normalize_text(x)})
    if not years:
        raise ValueError("No years found in goods_trade_scores_live.csv")
    return max(years)


def to_num_or_blank(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    try:
        n = float(text)
    except ValueError as exc:
        raise ValueError(f"Non-numeric preferential tariff value: {text}") from exc
    if n < 0 or n > 100:
        raise ValueError(f"Preferential tariff value outside 0-100 range: {text}")
    return f"{n:.6f}".rstrip("0").rstrip(".")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Merge reporter-batch bilateral preferential tariff files into the canonical "
            "worldwide_bilateral_preferences.csv override file and emit a coverage report."
        )
    )
    parser.add_argument("--batch-dir", default="", help="Directory containing preferential tariff batch CSVs")
    parser.add_argument("--agreements-file", default="", help="Path to trade_agreements.csv")
    parser.add_argument("--scores-file", default="", help="Path to goods_trade_scores_live.csv")
    parser.add_argument("--year", default="", help="Optional explicit year. Defaults to latest scored year.")
    parser.add_argument("--out-file", default="", help="Canonical override CSV output path")
    parser.add_argument("--coverage-file", default="", help="Coverage CSV output path")
    parser.add_argument("--manifest-file", default="", help="Manifest JSON output path")
    args = parser.parse_args()

    batch_dir = resolve_path(args.batch_dir, DEFAULT_BATCH_DIR)
    agreements_file = resolve_path(args.agreements_file, DEFAULT_AGREEMENTS_FILE)
    scores_file = resolve_path(args.scores_file, DEFAULT_SCORES_FILE)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)
    coverage_file = resolve_path(args.coverage_file, DEFAULT_COVERAGE_FILE)
    manifest_file = resolve_path(args.manifest_file, DEFAULT_MANIFEST_FILE)

    agreements = pd.read_csv(agreements_file, dtype=str, keep_default_na=False)
    scores = pd.read_csv(scores_file, dtype=str, keep_default_na=False)

    for df in [agreements, scores]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    require_columns(agreements, REQUIRED_AGREEMENT_COLS, agreements_file.name)
    require_columns(scores, REQUIRED_SCORE_COLS, scores_file.name)

    year = args.year.strip() or latest_scored_year(scores)

    active_agreements = agreements[agreements["status"].str.lower() == "in_force"].copy()
    active_agreements["pair_id"] = active_agreements["reporter_id"] + "__" + active_agreements["partner_id"]
    active_agreements = (
        active_agreements.sort_values(["pair_id", "in_force_date"], kind="stable")
        .drop_duplicates(subset=["pair_id"], keep="last")
        .reset_index(drop=True)
    )

    score_year = scores[scores["year"] == year].copy()
    if score_year.empty:
        raise ValueError(f"No score rows found for year {year}")

    scaffold = active_agreements.merge(
        score_year[
            [
                "year",
                "pair_id",
                "reporter_id",
                "reporter_name",
                "partner_id",
                "partner_name",
                "trade_value_usd_m",
                "agreement_id",
                "agreement_name",
                "rta_in_force",
            ]
        ],
        on=["pair_id", "reporter_id", "partner_id"],
        how="left",
        suffixes=("_agreement", "_score"),
        validate="one_to_one",
    )

    scaffold["year"] = year
    scaffold["agreement_id"] = scaffold["agreement_id_agreement"].where(
        scaffold["agreement_id_agreement"].astype(str).str.strip() != "",
        scaffold["agreement_id_score"],
    )
    scaffold["agreement_name"] = scaffold["agreement_name_agreement"].where(
        scaffold["agreement_name_agreement"].astype(str).str.strip() != "",
        scaffold["agreement_name_score"],
    )

    scaffold = scaffold[
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
        ]
    ].copy()

    batch_dir.mkdir(parents=True, exist_ok=True)
    batch_files = sorted([p for p in batch_dir.glob("*.csv") if p.is_file()])

    merged_batches = []
    batch_manifest = []

    for path in batch_files:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        for col in df.columns:
            df[col] = df[col].map(normalize_text)
        require_columns(df, REQUIRED_BATCH_COLS, path.name)

        if not df.empty:
            df["pair_id_expected"] = df["reporter_id"] + "__" + df["partner_id"]
            bad_pair = df[df["pair_id"] != df["pair_id_expected"]]
            if not bad_pair.empty:
                raise ValueError(
                    f"{path.name} has pair_id mismatches:\n"
                    + bad_pair[["pair_id", "pair_id_expected", "reporter_id", "partner_id"]].to_string(index=False)
                )

            df["bilateral_preferential_tariff_pct"] = df["bilateral_preferential_tariff_pct"].map(to_num_or_blank)
            df["bilateral_simple_avg_tariff_pct"] = df["bilateral_simple_avg_tariff_pct"].map(to_num_or_blank)

        merged_batches.append(df.drop(columns=["pair_id_expected"], errors="ignore"))
        batch_manifest.append(
            {
                "file": str(path),
                "row_count": int(len(df)),
            }
        )

    if merged_batches:
        collected = pd.concat(merged_batches, ignore_index=True)
        collected = collected.sort_values(["year", "pair_id"], kind="stable").drop_duplicates(
            subset=["year", "pair_id"], keep="last"
        )
    else:
        collected = pd.DataFrame(columns=REQUIRED_BATCH_COLS)

    overrides = scaffold.merge(
        collected[
            [
                "year",
                "pair_id",
                "bilateral_preferential_tariff_pct",
                "bilateral_simple_avg_tariff_pct",
                "source_label",
                "source_url",
                "notes",
            ]
        ],
        on=["year", "pair_id"],
        how="left",
        validate="one_to_one",
    )

    for col in [
        "bilateral_preferential_tariff_pct",
        "bilateral_simple_avg_tariff_pct",
        "source_label",
        "source_url",
        "notes",
    ]:
        overrides[col] = overrides[col].fillna("").map(normalize_text)

    overrides = overrides[
        [
            "year",
            "pair_id",
            "reporter_id",
            "reporter_name",
            "partner_id",
            "partner_name",
            "agreement_id",
            "agreement_name",
            "bilateral_preferential_tariff_pct",
            "bilateral_simple_avg_tariff_pct",
            "source_label",
            "source_url",
            "notes",
        ]
    ].sort_values(["year", "reporter_id", "partner_id"], kind="stable")

    coverage = overrides.copy()
    coverage["trade_value_usd_m"] = pd.to_numeric(scaffold["trade_value_usd_m"], errors="coerce")
    coverage["override_present"] = coverage["bilateral_preferential_tariff_pct"].map(lambda x: "yes" if normalize_text(x) else "no")
    coverage["priority_trade_rank"] = (
        coverage["trade_value_usd_m"]
        .rank(method="dense", ascending=False)
        .astype("Int64")
    )
    coverage = coverage.sort_values(
        ["override_present", "trade_value_usd_m", "reporter_id", "partner_id"],
        ascending=[True, False, True, True],
        kind="stable",
    )

    out_file.parent.mkdir(parents=True, exist_ok=True)
    coverage_file.parent.mkdir(parents=True, exist_ok=True)

    overrides.to_csv(out_file, index=False)
    coverage.to_csv(coverage_file, index=False)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "year": year,
        "batch_dir": str(batch_dir),
        "batch_file_count": len(batch_files),
        "batch_files": batch_manifest,
        "active_in_force_pairs": int(len(scaffold)),
        "override_rows": int(len(overrides)),
        "override_present_rows": int((coverage["override_present"] == "yes").sum()),
        "override_missing_rows": int((coverage["override_present"] == "no").sum()),
        "out_file": str(out_file),
        "coverage_file": str(coverage_file),
    }
    write_json(manifest_file, manifest)

    print(f"Year: {year}")
    print(f"Batch files read: {len(batch_files)}")
    print(f"Active in-force pairs: {len(scaffold)}")
    print(f"Override-present pairs: {(coverage['override_present'] == 'yes').sum()}")
    print(f"Override-missing pairs: {(coverage['override_present'] == 'no').sum()}")
    print(f"Wrote: {out_file}")
    print(f"Wrote: {coverage_file}")
    print(f"Wrote: {manifest_file}")


if __name__ == "__main__":
    main()