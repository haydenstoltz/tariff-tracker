from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TERRITORIES_FILE = ROOT / "data" / "metadata" / "world" / "customs_territories.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

REQUIRED_COLUMNS = [
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


def require_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--territories-file", default="", help="Path to customs_territories.csv")
    parser.add_argument("--out-dir", default="", help="Output directory for derived country-pair registry")
    args = parser.parse_args()

    territories_file = resolve_path(args.territories_file, DEFAULT_TERRITORIES_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    territories = pd.read_csv(territories_file, dtype=str, keep_default_na=False)
    require_columns(territories, REQUIRED_COLUMNS, territories_file.name)

    for col in territories.columns:
        territories[col] = territories[col].map(normalize_text)

    territories["active_flag"] = territories["active_flag"].str.lower()
    active = territories[territories["active_flag"] == "yes"].copy()

    if active.empty:
        raise ValueError("No active customs territories found")

    reporter = active.rename(
        columns={
            "actor_id": "reporter_id",
            "iso3": "reporter_iso3",
            "display_name": "reporter_name",
            "actor_type": "reporter_type",
        }
    )
    partner = active.rename(
        columns={
            "actor_id": "partner_id",
            "iso3": "partner_iso3",
            "display_name": "partner_name",
            "actor_type": "partner_type",
        }
    )

    registry = (
        reporter.assign(_key=1)
        .merge(partner.assign(_key=1), on="_key", how="inner")
        .drop(columns="_key")
    )
    registry = registry[registry["reporter_id"] != registry["partner_id"]].copy()

    registry["pair_id"] = registry["reporter_id"] + "__" + registry["partner_id"]
    registry["pair_label"] = registry["reporter_name"] + " imports from " + registry["partner_name"]
    registry["pair_scope"] = "goods"
    registry["score_status"] = "unbuilt"

    registry = registry[
        [
            "pair_id",
            "pair_label",
            "pair_scope",
            "reporter_id",
            "reporter_iso3",
            "reporter_name",
            "reporter_type",
            "partner_id",
            "partner_iso3",
            "partner_name",
            "partner_type",
            "score_status",
        ]
    ].sort_values(["reporter_id", "partner_id"], kind="stable")

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "country_pair_registry.csv"
    json_path = out_dir / "country_pair_registry.json"

    registry.to_csv(csv_path, index=False)
    write_json(json_path, registry.to_dict(orient="records"))

    print(f"Active territories: {len(active)}")
    print(f"Directed pairs: {len(registry)}")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {json_path}")


if __name__ == "__main__":
    main()