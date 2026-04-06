from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_REGISTRY_FILE = ROOT / "outputs" / "worldwide" / "country_pair_registry.csv"
DEFAULT_TARGETS_FILE = ROOT / "data" / "metadata" / "world" / "pair_pull_targets.csv"
DEFAULT_IMPORTS_FILE = ROOT / "outputs" / "worldwide" / "wto_imports_by_partner_targets.csv"
DEFAULT_MFN_FILE = ROOT / "outputs" / "worldwide" / "wto_mfn_reporter_totals.csv"
DEFAULT_AGREEMENTS_FILE = ROOT / "data" / "metadata" / "world" / "trade_agreements.csv"
DEFAULT_PENALTIES_FILE = ROOT / "data" / "metadata" / "world" / "pair_penalties.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"


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


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry-file", default="", help="Path to country_pair_registry.csv")
    parser.add_argument("--targets-file", default="", help="Path to pair_pull_targets.csv")
    parser.add_argument("--imports-file", default="", help="Path to WTO imports target file")
    parser.add_argument("--mfn-file", default="", help="Path to WTO reporter MFN totals file")
    parser.add_argument("--agreements-file", default="", help="Path to trade_agreements.csv")
    parser.add_argument("--penalties-file", default="", help="Path to pair_penalties.csv")
    parser.add_argument("--out-dir", default="", help="Output directory")
    parser.add_argument(
        "--allow-partial-imports",
        action="store_true",
        help="Restrict scoring to enabled target pairs that have import rows in the imports input file",
    )
    args = parser.parse_args()

    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    targets_file = resolve_path(args.targets_file, DEFAULT_TARGETS_FILE)
    imports_file = resolve_path(args.imports_file, DEFAULT_IMPORTS_FILE)
    mfn_file = resolve_path(args.mfn_file, DEFAULT_MFN_FILE)
    agreements_file = resolve_path(args.agreements_file, DEFAULT_AGREEMENTS_FILE)
    penalties_file = resolve_path(args.penalties_file, DEFAULT_PENALTIES_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    out_dir.mkdir(parents=True, exist_ok=True)

    for stale_name in [
        "goods_score_inputs_live.csv",
        "goods_trade_scores_live.csv",
        "goods_trade_scores_live.json",
    ]:
        stale_path = out_dir / stale_name
        if stale_path.exists():
            stale_path.unlink()

    registry = pd.read_csv(registry_file, dtype=str, keep_default_na=False)
    targets = pd.read_csv(targets_file, dtype=str, keep_default_na=False)
    imports_df = pd.read_csv(imports_file, dtype=str, keep_default_na=False)
    mfn_df = pd.read_csv(mfn_file, dtype=str, keep_default_na=False)
    agreements = pd.read_csv(agreements_file, dtype=str, keep_default_na=False)
    penalties = pd.read_csv(penalties_file, dtype=str, keep_default_na=False)

    for df in [registry, targets, imports_df, mfn_df, agreements, penalties]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    targets = targets[targets["enabled_flag"].str.lower() == "yes"].copy()
    targets["pair_id"] = targets["reporter_id"] + "__" + targets["partner_id"]

    registry_keep = [
        "pair_id",
    ]
    registry = registry[registry_keep].drop_duplicates(subset=["pair_id"])

    imports_df["pair_id"] = imports_df["reporter_id"] + "__" + imports_df["partner_id"]

    if args.allow_partial_imports:
        imports_df = imports_df[imports_df["trade_value_usd"].map(normalize_text) != ""].copy()
        available_pair_ids = set(imports_df["pair_id"].tolist())
        targets = targets[targets["pair_id"].isin(available_pair_ids)].copy()
        if targets.empty:
            raise ValueError(
                "No enabled target pairs remain after filtering to available imports data"
            )

    active_agreements = agreements[agreements["status"].str.lower() == "in_force"].copy()
    active_agreements["pair_id"] = active_agreements["reporter_id"] + "__" + active_agreements["partner_id"]
    active_agreements = (
        active_agreements.sort_values(["pair_id", "in_force_date"], kind="stable")
        .drop_duplicates(subset=["pair_id"], keep="last")
        [["pair_id", "agreement_id", "agreement_name", "goods_coverage", "in_force_date"]]
    )

    penalties["pair_id"] = penalties["reporter_id"] + "__" + penalties["partner_id"]
    penalties["ntm_penalty_points"] = pd.to_numeric(
        penalties["ntm_penalty_points"], errors="coerce"
    ).fillna(0.0)
    penalties["trade_remedy_penalty_points"] = pd.to_numeric(
        penalties["trade_remedy_penalty_points"], errors="coerce"
    ).fillna(0.0)
    penalties = penalties[
        ["pair_id", "ntm_penalty_points", "trade_remedy_penalty_points", "penalty_basis", "notes"]
    ].rename(columns={"notes": "penalty_notes"})

    built = (
        targets.merge(registry, on=["pair_id"], how="left", validate="many_to_one")
        .merge(
            imports_df[
                [
                    "year",
                    "reporter_id",
                    "partner_id",
                    "pair_id",
                    "trade_value_usd",
                    "reporter_name_wto",
                    "partner_name_wto",
                    "classification",
                    "classification_version",
                    "product_code",
                    "mtn_categories",
                ]
            ],
            on=["year", "reporter_id", "partner_id", "pair_id"],
            how="left",
            validate="one_to_one",
        )
        .merge(
            mfn_df[
                [
                    "year",
                    "reporter_id",
                    "simple_average_mfn_pct",
                    "trade_weighted_mfn_pct",
                    "duty_free_share_ratio",
                    "duty_scheme_code",
                    "duty_scheme_name",
                ]
            ],
            on=["year", "reporter_id"],
            how="left",
            validate="many_to_one",
        )
        .merge(active_agreements, on="pair_id", how="left", validate="many_to_one")
        .merge(penalties, on="pair_id", how="left", validate="many_to_one")
    )

    missing_imports = built[built["trade_value_usd"] == ""]
    if not missing_imports.empty:
        raise ValueError(
            "Missing WTO bilateral import values for target pairs:\n"
            + missing_imports[["year", "reporter_id", "partner_id"]].to_string(index=False)
        )

    missing_mfn = built[built["trade_weighted_mfn_pct"] == ""]
    if not missing_mfn.empty:
        raise ValueError(
            "Missing WTO reporter MFN values for target reporters:\n"
            + missing_mfn[["year", "reporter_id"]].drop_duplicates().to_string(index=False)
        )

    built["trade_value_usd"] = pd.to_numeric(built["trade_value_usd"], errors="raise")
    built["trade_value_usd_m"] = built["trade_value_usd"] / 1_000_000.0
    built["simple_average_mfn_pct"] = pd.to_numeric(built["simple_average_mfn_pct"], errors="raise")
    built["trade_weighted_mfn_pct"] = pd.to_numeric(built["trade_weighted_mfn_pct"], errors="raise")
    built["duty_free_share_ratio"] = pd.to_numeric(built["duty_free_share_ratio"], errors="coerce")
    built["ntm_penalty_points"] = pd.to_numeric(built["ntm_penalty_points"], errors="coerce").fillna(0.0)
    built["trade_remedy_penalty_points"] = pd.to_numeric(
        built["trade_remedy_penalty_points"], errors="coerce"
    ).fillna(0.0)

    built["trade_weighted_applied_tariff_pct"] = built["trade_weighted_mfn_pct"]
    built["simple_avg_effectively_applied_pct"] = built["simple_average_mfn_pct"]

    built["tariff_component"] = built["trade_weighted_applied_tariff_pct"].map(
        lambda x: round(clamp(100.0 - (4.0 * float(x))), 3)
    )

    def score_one(row: pd.Series) -> float:
        score = (
            100.0
            - (4.0 * float(row["trade_weighted_applied_tariff_pct"]))
            - float(row["ntm_penalty_points"])
            - float(row["trade_remedy_penalty_points"])
        )
        return round(clamp(score), 3)

    built["goods_score_live_v1"] = built.apply(score_one, axis=1)
    built["rta_in_force"] = built["agreement_id"].map(lambda x: "yes" if normalize_text(x) else "no")
    built["tariff_agreement_count"] = built["agreement_id"].map(lambda x: 1 if normalize_text(x) else 0)

    built["score_method"] = (
        "wto_ttd_v1 = clamp(100 - 4*reporter_trade_weighted_mfn_pct "
        "- ntm_penalty_points - trade_remedy_penalty_points)"
    )
    built["source_stack"] = (
        "Manual bilateral imports raw extract + WTO Timeseries API reporter MFN all-products "
        "+ local agreement and penalty registries"
    )
    built["score_status"] = "built"

    built["notes"] = built.apply(
        lambda row: " | ".join(
            [
                x
                for x in [
                    "Reporter-level MFN tariff baseline applied; not yet bilateral preferential tariff data.",
                    normalize_text(row.get("penalty_notes", "")),
                ]
                if x
            ]
        ),
        axis=1,
    )

    keep_cols = [
        "year",
        "pair_id",
        "pair_label",
        "reporter_id",
        "reporter_iso3",
        "reporter_name",
        "partner_id",
        "partner_iso3",
        "partner_name",
        "trade_value_usd",
        "trade_value_usd_m",
        "trade_weighted_applied_tariff_pct",
        "simple_avg_effectively_applied_pct",
        "tariff_agreement_count",
        "ntm_penalty_points",
        "trade_remedy_penalty_points",
        "tariff_component",
        "goods_score_live_v1",
        "rta_in_force",
        "agreement_id",
        "agreement_name",
        "goods_coverage",
        "in_force_date",
        "score_method",
        "source_stack",
        "score_status",
        "penalty_basis",
        "notes",
    ]

    built = built[keep_cols].sort_values(["year", "reporter_id", "partner_id"], kind="stable")

    inputs_csv = out_dir / "goods_score_inputs_live.csv"
    scores_csv = out_dir / "goods_trade_scores_live.csv"
    scores_json = out_dir / "goods_trade_scores_live.json"

    built.to_csv(inputs_csv, index=False)
    built.to_csv(scores_csv, index=False)
    write_json(scores_json, built.to_dict(orient="records"))

    print(f"Rows built: {len(built)}")
    print(f"Wrote: {inputs_csv}")
    print(f"Wrote: {scores_csv}")
    print(f"Wrote: {scores_json}")


if __name__ == "__main__":
    main()