from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_REVIEW_FILE = ROOT / "outputs" / "worldwide" / "ttd_reporter_support_review.csv"
DEFAULT_TERRITORIES_FILE = ROOT / "data" / "metadata" / "world" / "customs_territories.csv"

REVIEW_REQUIRED_COLS = [
    "actor_id",
    "review_status",
    "ttd_reporter_supported",
    "keep_active",
    "review_notes",
]

TERRITORY_REQUIRED_COLS = [
    "actor_id",
    "active_flag",
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


def normalize_yes_no_blank(value: str) -> str:
    v = normalize_text(value).lower()
    if v in {"yes", "no"}:
        return v
    if v == "":
        return ""
    raise ValueError(f"Expected yes/no/blank, got: {value}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Apply the manual WTO TTD reporter-support review back into "
            "customs_territories.csv by updating active_flag."
        )
    )
    parser.add_argument("--review-file", default="", help="Path to ttd_reporter_support_review.csv")
    parser.add_argument("--territories-file", default="", help="Path to customs_territories.csv")
    parser.add_argument(
        "--require-reviewed",
        action="store_true",
        help="Require every row in the review file to have review_status=reviewed before applying.",
    )
    args = parser.parse_args()

    review_file = resolve_path(args.review_file, DEFAULT_REVIEW_FILE)
    territories_file = resolve_path(args.territories_file, DEFAULT_TERRITORIES_FILE)

    review = pd.read_csv(review_file, dtype=str, keep_default_na=False)
    territories = pd.read_csv(territories_file, dtype=str, keep_default_na=False)

    for df in [review, territories]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    require_columns(review, REVIEW_REQUIRED_COLS, review_file.name)
    require_columns(territories, TERRITORY_REQUIRED_COLS, territories_file.name)

    review["review_status_norm"] = review["review_status"].map(lambda x: normalize_text(x).lower())
    review["ttd_reporter_supported_norm"] = review["ttd_reporter_supported"].map(normalize_yes_no_blank)
    review["keep_active_norm"] = review["keep_active"].map(normalize_yes_no_blank)

    if review["actor_id"].duplicated().any():
        dupes = sorted(review.loc[review["actor_id"].duplicated(), "actor_id"].tolist())
        raise ValueError(f"Duplicate actor_id values in review file: {dupes}")

    if args.require_reviewed:
        not_reviewed = sorted(review.loc[review["review_status_norm"] != "reviewed", "actor_id"].tolist())
        if not_reviewed:
            raise ValueError(f"Rows not marked reviewed: {not_reviewed[:25]}")

    actionable = review[review["keep_active_norm"].isin(["yes", "no"])].copy()
    if actionable.empty:
        raise ValueError("No actionable rows found. Fill keep_active with yes or no for reviewed actors.")

    review_map = actionable.set_index("actor_id").to_dict(orient="index")

    changed = []
    for idx, row in territories.iterrows():
        actor_id = normalize_text(row["actor_id"])
        if actor_id not in review_map:
            continue

        keep_active = review_map[actor_id]["keep_active_norm"]
        supported = review_map[actor_id]["ttd_reporter_supported_norm"]
        review_notes = normalize_text(review_map[actor_id]["review_notes"])

        new_flag = "yes" if keep_active == "yes" else "no"
        old_flag = normalize_text(territories.at[idx, "active_flag"]).lower()

        if old_flag != new_flag:
            changed.append((actor_id, old_flag, new_flag))

        territories.at[idx, "active_flag"] = new_flag

        existing_notes = normalize_text(territories.at[idx, "notes"])
        support_note = (
            f"WTO TTD reporter support reviewed: supported={supported or 'blank'}; "
            f"keep_active={keep_active}."
        )
        if review_notes:
            support_note = f"{support_note} {review_notes}"

        territories.at[idx, "notes"] = (
            f"{existing_notes} | {support_note}" if existing_notes else support_note
        )

    territories.to_csv(territories_file, index=False)

    print(f"Actionable review rows applied: {len(actionable)}")
    print(f"Territory active_flag changes: {len(changed)}")
    if changed:
        print("Sample changes:")
        for actor_id, old_flag, new_flag in changed[:25]:
            print(f"- {actor_id}: {old_flag} -> {new_flag}")
    print(f"Wrote: {territories_file}")


if __name__ == "__main__":
    main()
