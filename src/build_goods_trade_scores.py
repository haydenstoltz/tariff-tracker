from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_FILE = ROOT / "outputs" / "worldwide" / "country_pair_registry.csv"
DEFAULT_INPUTS_FILE = ROOT / "data" / "metadata" / "world" / "goods_score_inputs_seed.csv"
DEFAULT_AGREEMENTS_FILE = ROOT / "data" / "metadata" / "world" / "trade_agreements.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

REQUIRED_INPUT_COLUMNS = [
    "year",
    "reporter_id",
    "partner_id",
    "trade_value_usd_m",
    "trade_weighted_applied_tariff_pct",
    "simple_avg_mfn_pct",
    "preference_margin_pct",
    "ntm_penalty_points",
    "trade_remedy_penalty_points",
    "data_quality",
    "tariff_source_key",
    "trade_source_key",
    "ntm_source_key",
]

REQUIRED_AGREEMENT_COLUMNS = [
    "agreement_id",
    "agreement_name",
    "reporter_id",
    "partner_id",
    "in_force_date",
    "status",
    "goods_coverage",
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


def clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def score_row(row: pd.Series) -> pd.Series:
    applied = float(row["trade_weighted_applied_tariff_pct"])
    ntm_penalty = float(row["ntm_penalty_points"])
    trade_remedy_penalty = float(row["trade_remedy_penalty_points"])

    tariff_component = clamp(100.0 - (4.0 * applied))
    goods_score_v1 = clamp(tariff_component - ntm_penalty - trade_remedy_penalty)

    row["tariff_component"] = round(tariff_component, 3)
    row["goods_score_v1"] = round(goods_score_v1, 3)
    row["score_method"] = (
        "v1 = clamp(100 - 4*trade_weighted_applied_tariff_pct "
        "- ntm_penalty_points - trade_remedy_penalty_points)"
    )
    return row


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry-file", default="", help="Path to country_pair_registry.csv")
    parser.add_argument("--inputs-file", default="", help="Path to goods_score_inputs_seed.csv")
    parser.add_argument("--agreements-file", default="", help="Path to trade_agreements.csv")
    parser.add_argument("--out-dir", default="", help="Output directory for goods score outputs")
    args = parser.parse_args()

    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    inputs_file = resolve_path(args.inputs_file, DEFAULT_INPUTS_FILE)
    agreements_file = resolve_path(args.agreements_file, DEFAULT_AGREEMENTS_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    registry = pd.read_csv(registry_file, dtype=str, keep_default_na=False)
    inputs = pd.read_csv(inputs_file, dtype=str, keep_default_na=False)
    agreements = pd.read_csv(agreements_file, dtype=str, keep_default_na=False)

    require_columns(inputs, REQUIRED_INPUT_COLUMNS, inputs_file.name)
    require_columns(agreements, REQUIRED_AGREEMENT_COLUMNS, agreements_file.name)

    for df in [registry, inputs, agreements]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    numeric_cols = [
        "trade_value_usd_m",
        "trade_weighted_applied_tariff_pct",
        "simple_avg_mfn_pct",
        "preference_margin_pct",
        "ntm_penalty_points",
        "trade_remedy_penalty_points",
    ]
    for col in numeric_cols:
        inputs[col] = pd.to_numeric(inputs[col], errors="raise")

    inputs["pair_id"] = inputs["reporter_id"] + "__" + inputs["partner_id"]
    inputs = inputs.drop(columns=["reporter_id", "partner_id"])

    active_agreements = agreements[agreements["status"].str.lower() == "in_force"].copy()
    active_agreements["pair_id"] = active_agreements["reporter_id"] + "__" + active_agreements["partner_id"]
    active_agreements = active_agreements.drop(columns=["reporter_id", "partner_id", "status"])
    active_agreements = active_agreements.sort_values(["pair_id", "in_force_date"], kind="stable")
    active_agreements = active_agreements.drop_duplicates(subset=["pair_id"], keep="last")
    active_agreements = active_agreements[
        ["pair_id", "agreement_id", "agreement_name", "goods_coverage", "in_force_date"]
    ]

    registry_keep = [
        "pair_id",
        "pair_label",
        "reporter_id",
        "reporter_iso3",
        "reporter_name",
        "partner_id",
        "partner_iso3",
        "partner_name",
    ]
    scored = registry[registry_keep].merge(inputs, on="pair_id", how="left", validate="one_to_many")
    scored = scored.merge(active_agreements, on="pair_id", how="left", validate="many_to_one")
    scored = scored.fillna("")

    built = scored[scored["year"] != ""].copy()
    if built.empty:
        raise ValueError("No score input rows matched the country-pair registry")

    built = built.apply(score_row, axis=1)
    built["rta_in_force"] = built["agreement_id"].map(lambda x: "yes" if normalize_text(x) else "no")
    built["score_status"] = "built"

    keep_cols = [
        "pair_id",
        "pair_label",
        "reporter_id",
        "reporter_iso3",
        "reporter_name",
        "partner_id",
        "partner_iso3",
        "partner_name",
        "year",
        "trade_value_usd_m",
        "trade_weighted_applied_tariff_pct",
        "simple_avg_mfn_pct",
        "preference_margin_pct",
        "ntm_penalty_points",
        "trade_remedy_penalty_points",
        "tariff_component",
        "goods_score_v1",
        "rta_in_force",
        "agreement_id",
        "agreement_name",
        "goods_coverage",
        "data_quality",
        "tariff_source_key",
        "trade_source_key",
        "ntm_source_key",
        "score_method",
        "score_status",
        "notes",
    ]

    built = built[keep_cols].sort_values(
        ["year", "reporter_id", "partner_id"],
        ascending=[False, True, True],
        kind="stable",
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "goods_trade_scores.csv"
    json_path = out_dir / "goods_trade_scores.json"

    built.to_csv(csv_path, index=False)
    write_json(json_path, built.to_dict(orient="records"))

    print(f"Scored rows: {len(built)}")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {json_path}")


if __name__ == "__main__":
    main()