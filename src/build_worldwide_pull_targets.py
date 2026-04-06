from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_TERRITORIES_FILE = ROOT / "data" / "metadata" / "world" / "customs_territories.csv"
DEFAULT_OUT_FILE = ROOT / "data" / "metadata" / "world" / "pair_pull_targets.csv"

REQUIRED_TERRITORY_COLS = [
    "actor_id",
    "iso3",
    "display_name",
    "actor_type",
    "active_flag",
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
            "Build canonical reporter-partner pull targets for the worldwide WTO ingest path "
            "from the active customs territory universe."
        )
    )
    parser.add_argument(
        "--territories-file",
        default="",
        help="Path to customs_territories.csv",
    )
    parser.add_argument(
        "--out-file",
        default="",
        help="Output path for pair_pull_targets.csv",
    )
    parser.add_argument(
        "--year",
        default="2023",
        help="Target year to assign to all enabled pull rows. Default: 2023",
    )
    parser.add_argument(
        "--disable-reporters",
        default="",
        help="Optional comma-separated reporter actor_ids to mark disabled",
    )
    parser.add_argument(
        "--disable-pairs-file",
        default="",
        help=(
            "Optional CSV with columns reporter_id,partner_id to mark specific directed pairs disabled. "
            "Disabled rows stay in the output with enabled_flag='no'."
        ),
    )
    args = parser.parse_args()

    territories_file = resolve_path(args.territories_file, DEFAULT_TERRITORIES_FILE)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)

    year = normalize_text(args.year)
    if not year.isdigit() or len(year) != 4:
        raise ValueError(f"--year must be a four-digit year, got: {year}")

    territories = pd.read_csv(territories_file, dtype=str, keep_default_na=False)
    for col in territories.columns:
        territories[col] = territories[col].map(normalize_text)

    require_columns(territories, REQUIRED_TERRITORY_COLS, territories_file.name)

    active = territories[territories["active_flag"].str.lower() == "yes"].copy()
    if active.empty:
        raise ValueError("No active territories found in customs_territories.csv")

    active = active.sort_values(["actor_id"], kind="stable").reset_index(drop=True)

    reporter_df = active.rename(
        columns={
            "actor_id": "reporter_id",
            "iso3": "reporter_iso3",
            "display_name": "reporter_name",
            "actor_type": "reporter_type",
        }
    )
    partner_df = active.rename(
        columns={
            "actor_id": "partner_id",
            "iso3": "partner_iso3",
            "display_name": "partner_name",
            "actor_type": "partner_type",
        }
    )

    targets = (
        reporter_df.assign(_key=1)
        .merge(partner_df.assign(_key=1), on="_key", how="inner")
        .drop(columns="_key")
    )
    targets = targets[targets["reporter_id"] != targets["partner_id"]].copy()

    targets["year"] = year
    targets["pair_id"] = targets["reporter_id"] + "__" + targets["partner_id"]
    targets["pair_label"] = targets["reporter_name"] + " imports from " + targets["partner_name"]
    targets["enabled_flag"] = "yes"
    targets["notes"] = "auto-generated from active customs territories"

    disabled_reporters = {
        normalize_text(x).upper()
        for x in normalize_text(args.disable_reporters).split(",")
        if normalize_text(x)
    }
    if disabled_reporters:
        targets.loc[
            targets["reporter_id"].str.upper().isin(disabled_reporters),
            "enabled_flag",
        ] = "no"
        targets.loc[
            targets["reporter_id"].str.upper().isin(disabled_reporters),
            "notes",
        ] = "disabled via --disable-reporters"

    if normalize_text(args.disable_pairs_file):
        disable_pairs_file = resolve_path(args.disable_pairs_file, ROOT / normalize_text(args.disable_pairs_file))
        disable_df = pd.read_csv(disable_pairs_file, dtype=str, keep_default_na=False)
        for col in disable_df.columns:
            disable_df[col] = disable_df[col].map(normalize_text)

        required_disable_cols = ["reporter_id", "partner_id"]
        require_columns(disable_df, required_disable_cols, disable_pairs_file.name)

        disable_df["pair_id"] = disable_df["reporter_id"] + "__" + disable_df["partner_id"]
        disabled_pair_ids = set(disable_df["pair_id"].tolist())

        mask = targets["pair_id"].isin(disabled_pair_ids)
        targets.loc[mask, "enabled_flag"] = "no"
        targets.loc[mask, "notes"] = "disabled via disable-pairs file"

    targets = targets[
        [
            "year",
            "pair_id",
            "pair_label",
            "reporter_id",
            "reporter_iso3",
            "reporter_name",
            "reporter_type",
            "partner_id",
            "partner_iso3",
            "partner_name",
            "partner_type",
            "enabled_flag",
            "notes",
        ]
    ].sort_values(["year", "reporter_id", "partner_id"], kind="stable").reset_index(drop=True)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    targets.to_csv(out_file, index=False)

    enabled_count = int((targets["enabled_flag"].str.lower() == "yes").sum())
    disabled_count = int((targets["enabled_flag"].str.lower() != "yes").sum())

    print(f"Active territories: {len(active)}")
    print(f"Directed pairs written: {len(targets)}")
    print(f"Enabled pairs: {enabled_count}")
    print(f"Disabled pairs: {disabled_count}")
    print(f"Wrote: {out_file}")


if __name__ == "__main__":
    main()