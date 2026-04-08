from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_TERRITORIES_FILE = ROOT / "data" / "metadata" / "world" / "customs_territories.csv"
DEFAULT_CODE_MAP_FILE = ROOT / "data" / "metadata" / "world" / "wto_actor_code_map.csv"
DEFAULT_OUT_FILE = ROOT / "outputs" / "worldwide" / "ttd_reporter_support_review.csv"

TERRITORY_REQUIRED_COLS = [
    "actor_id",
    "iso3",
    "display_name",
    "actor_type",
    "active_flag",
]

CODE_MAP_REQUIRED_COLS = [
    "actor_id",
    "wto_partner_code",
    "canonical_name",
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
            "Build a manual WTO TTD reporter-support review sheet from the current "
            "customs territory universe and actor-code map."
        )
    )
    parser.add_argument("--territories-file", default="", help="Path to customs_territories.csv")
    parser.add_argument("--code-map-file", default="", help="Path to wto_actor_code_map.csv")
    parser.add_argument("--out-file", default="", help="Output path")
    args = parser.parse_args()

    territories_file = resolve_path(args.territories_file, DEFAULT_TERRITORIES_FILE)
    code_map_file = resolve_path(args.code_map_file, DEFAULT_CODE_MAP_FILE)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)

    territories = pd.read_csv(territories_file, dtype=str, keep_default_na=False)
    code_map = pd.read_csv(code_map_file, dtype=str, keep_default_na=False)

    for df in [territories, code_map]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    require_columns(territories, TERRITORY_REQUIRED_COLS, territories_file.name)
    require_columns(code_map, CODE_MAP_REQUIRED_COLS, code_map_file.name)

    active = territories[territories["active_flag"].str.lower() == "yes"].copy()
    if active.empty:
        raise ValueError("No active reporters found in customs_territories.csv")

    code_map["wto_partner_code"] = code_map["wto_partner_code"].map(lambda x: normalize_text(x).zfill(3))

    merged = active.merge(
        code_map[["actor_id", "wto_partner_code", "canonical_name"]],
        on="actor_id",
        how="left",
        validate="one_to_one",
    )

    merged["review_status"] = "pending"
    merged["ttd_reporter_supported"] = ""
    merged["keep_active"] = ""
    merged["review_notes"] = ""

    out = merged[
        [
            "actor_id",
            "iso3",
            "display_name",
            "actor_type",
            "active_flag",
            "wto_partner_code",
            "canonical_name",
            "review_status",
            "ttd_reporter_supported",
            "keep_active",
            "review_notes",
        ]
    ].rename(
        columns={
            "active_flag": "current_active_flag",
            "wto_partner_code": "wto_reporter_code",
        }
    )

    out = out.sort_values(["actor_id"], kind="stable").reset_index(drop=True)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_file, index=False)

    print(f"Active reporters queued for review: {len(out)}")
    print(f"Wrote: {out_file}")


if __name__ == "__main__":
    main()
