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
DEFAULT_WITS_INDICATORS_FILE = ROOT / "outputs" / "worldwide" / "wits_pair_indicators_totals.csv"
DEFAULT_AGREEMENTS_FILE = ROOT / "data" / "metadata" / "world" / "trade_agreements.csv"
DEFAULT_PENALTIES_FILE = ROOT / "data" / "metadata" / "world" / "pair_penalties.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

WITS_REQUIRED_COLUMNS = [
    "year",
    "reporter_id",
    "partner_id",
    "wits_import_trade_value_usd",
    "wits_bilateral_weighted_tariff_pct",
    "wits_bilateral_simple_tariff_pct",
    "wits_tariff_agreement_count",
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


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    return df


def note_join(parts: list[str]) -> str:
    return " | ".join([p for p in parts if normalize_text(p)])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry-file", default="", help="Path to country_pair_registry.csv")
    parser.add_argument("--targets-file", default="", help="Path to pair_pull_targets.csv")
    parser.add_argument("--imports-file", default="", help="Path to WTO imports target file")
    parser.add_argument("--mfn-file", default="", help="Path to WTO reporter MFN totals file")
    parser.add_argument("--wits-indicators-file", default="", help="Path to WITS pair indicators totals CSV")
    parser.add_argument("--agreements-file", default="", help="Path to trade_agreements.csv")
    parser.add_argument("--penalties-file", default="", help="Path to pair_penalties.csv")
    parser.add_argument("--out-dir", default="", help="Output directory")
    parser.add_argument(
        "--allow-partial-imports",
        action="store_true",
        help="Allow partial pair coverage by dropping rows missing both trade value and usable tariff data",
    )
    args = parser.parse_args()

    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    targets_file = resolve_path(args.targets_file, DEFAULT_TARGETS_FILE)
    imports_file = resolve_path(args.imports_file, DEFAULT_IMPORTS_FILE)
    mfn_file = resolve_path(args.mfn_file, DEFAULT_MFN_FILE)
    wits_indicators_file = resolve_path(args.wits_indicators_file, DEFAULT_WITS_INDICATORS_FILE)
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

    if wits_indicators_file.exists():
        wits_df = pd.read_csv(wits_indicators_file, dtype=str, keep_default_na=False)
    else:
        wits_df = pd.DataFrame(columns=WITS_REQUIRED_COLUMNS)

    for df in [registry, targets, imports_df, mfn_df, agreements, penalties, wits_df]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    wits_df = ensure_columns(wits_df, WITS_REQUIRED_COLUMNS)

    targets = targets[targets["enabled_flag"].str.lower() == "yes"].copy()
    targets["pair_id"] = targets["reporter_id"] + "__" + targets["partner_id"]

    registry = registry[["pair_id"]].drop_duplicates(subset=["pair_id"])

    imports_df["pair_id"] = imports_df["reporter_id"] + "__" + imports_df["partner_id"]

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

    wits_df["pair_id"] = wits_df["reporter_id"] + "__" + wits_df["partner_id"]

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
        .merge(
            wits_df[
                [
                    "year",
                    "reporter_id",
                    "partner_id",
                    "pair_id",
                    "wits_import_trade_value_usd",
                    "wits_bilateral_weighted_tariff_pct",
                    "wits_bilateral_simple_tariff_pct",
                    "wits_tariff_agreement_count",
                    "wits_rta_in_force",
                ]
            ],
            on=["year", "reporter_id", "partner_id", "pair_id"],
            how="left",
            validate="one_to_one",
        )
        .merge(active_agreements, on="pair_id", how="left", validate="many_to_one")
        .merge(penalties, on="pair_id", how="left", validate="many_to_one")
    )

    built["trade_value_usd_imports_num"] = pd.to_numeric(built["trade_value_usd"], errors="coerce")
    built["wits_import_trade_value_usd_num"] = pd.to_numeric(
        built["wits_import_trade_value_usd"], errors="coerce"
    )

    built["trade_value_usd_num"] = built["trade_value_usd_imports_num"].combine_first(
        built["wits_import_trade_value_usd_num"]
    )

    built["trade_value_source"] = built.apply(
        lambda row: (
            "wto_ttd_imports_batch"
            if pd.notna(row["trade_value_usd_imports_num"])
            else ("wits_mprt_trd_vl_total_fallback" if pd.notna(row["wits_import_trade_value_usd_num"]) else "")
        ),
        axis=1,
    )

    missing_trade = built[built["trade_value_usd_num"].isna()][["year", "reporter_id", "partner_id"]].copy()
    if not missing_trade.empty and not args.allow_partial_imports:
        raise ValueError(
            "Missing trade value rows for target pairs after WTO+WITS merge:\n"
            + missing_trade.head(50).to_string(index=False)
        )

    if args.allow_partial_imports and not missing_trade.empty:
        built.loc[built["trade_value_usd_num"].isna(), "trade_value_usd_num"] = 0.0
        built.loc[
            built["trade_value_source"].map(normalize_text) == "",
            "trade_value_source",
        ] = "imputed_zero_no_trade_data"

    built["simple_average_mfn_pct_num"] = pd.to_numeric(built["simple_average_mfn_pct"], errors="coerce")
    built["trade_weighted_mfn_pct_num"] = pd.to_numeric(built["trade_weighted_mfn_pct"], errors="coerce")
    built["duty_free_share_ratio"] = pd.to_numeric(built["duty_free_share_ratio"], errors="coerce")

    built["wits_bilateral_weighted_tariff_pct_num"] = pd.to_numeric(
        built["wits_bilateral_weighted_tariff_pct"], errors="coerce"
    )
    built["wits_bilateral_simple_tariff_pct_num"] = pd.to_numeric(
        built["wits_bilateral_simple_tariff_pct"], errors="coerce"
    )
    built["wits_tariff_agreement_count_num"] = pd.to_numeric(
        built["wits_tariff_agreement_count"], errors="coerce"
    )

    built["trade_weighted_applied_tariff_pct_num"] = built["wits_bilateral_weighted_tariff_pct_num"].combine_first(
        built["trade_weighted_mfn_pct_num"]
    )
    built["simple_avg_effectively_applied_pct_num"] = built["wits_bilateral_simple_tariff_pct_num"].combine_first(
        built["simple_average_mfn_pct_num"]
    )

    built["tariff_basis"] = built.apply(
        lambda row: (
            "wits_bilateral_applied"
            if pd.notna(row["wits_bilateral_weighted_tariff_pct_num"])
            else ("reporter_mfn_fallback" if pd.notna(row["trade_weighted_mfn_pct_num"]) else "")
        ),
        axis=1,
    )

    missing_tariff = built[built["trade_weighted_applied_tariff_pct_num"].isna()][
        ["year", "reporter_id", "partner_id"]
    ].copy()
    if not missing_tariff.empty and not args.allow_partial_imports:
        raise ValueError(
            "Missing tariff rows for target pairs after bilateral+MFN merge:\n"
            + missing_tariff.head(50).to_string(index=False)
        )

    if args.allow_partial_imports and not missing_tariff.empty:
        reporter_weighted_median = built.groupby("reporter_id")[
            "trade_weighted_applied_tariff_pct_num"
        ].transform("median")
        reporter_simple_median = built.groupby("reporter_id")[
            "simple_avg_effectively_applied_pct_num"
        ].transform("median")

        global_weighted_median = built["trade_weighted_applied_tariff_pct_num"].median(skipna=True)
        global_simple_median = built["simple_avg_effectively_applied_pct_num"].median(skipna=True)

        missing_mask = built["trade_weighted_applied_tariff_pct_num"].isna()
        reporter_fill_mask = missing_mask & reporter_weighted_median.notna()
        global_fill_mask = missing_mask & ~reporter_weighted_median.notna()

        built.loc[missing_mask, "trade_weighted_applied_tariff_pct_num"] = (
            reporter_weighted_median.fillna(global_weighted_median).fillna(0.0)[missing_mask]
        )
        built.loc[missing_mask, "simple_avg_effectively_applied_pct_num"] = (
            reporter_simple_median.fillna(global_simple_median).fillna(0.0)[missing_mask]
        )

        built.loc[reporter_fill_mask, "tariff_basis"] = "imputed_reporter_median_fallback"
        built.loc[global_fill_mask, "tariff_basis"] = "imputed_global_median_fallback"

    built["trade_value_usd"] = built["trade_value_usd_num"]
    built["trade_value_usd_m"] = built["trade_value_usd_num"] / 1_000_000.0

    built["ntm_penalty_points"] = pd.to_numeric(built["ntm_penalty_points"], errors="coerce").fillna(0.0)
    built["trade_remedy_penalty_points"] = pd.to_numeric(
        built["trade_remedy_penalty_points"], errors="coerce"
    ).fillna(0.0)

    built["tariff_component"] = built["trade_weighted_applied_tariff_pct_num"].map(
        lambda x: round(clamp(100.0 - (4.0 * float(x))), 3)
    )

    built["goods_score_live_v1"] = built.apply(
        lambda row: round(
            clamp(
                100.0
                - (4.0 * float(row["trade_weighted_applied_tariff_pct_num"]))
                - float(row["ntm_penalty_points"])
                - float(row["trade_remedy_penalty_points"])
            ),
            3,
        ),
        axis=1,
    )

    static_agreement = built["agreement_id"].map(lambda x: normalize_text(x) != "")
    wits_agreement = built["wits_tariff_agreement_count_num"].fillna(0).map(lambda x: float(x) > 0)

    built["rta_in_force"] = (static_agreement | wits_agreement).map(lambda flag: "yes" if flag else "no")

    built["tariff_agreement_count"] = built["wits_tariff_agreement_count_num"].combine_first(
        static_agreement.map(lambda flag: 1.0 if flag else 0.0)
    )

    built.loc[
        built["agreement_id"].map(normalize_text).eq("") & wits_agreement,
        "agreement_id",
    ] = "WITS_TRF_NMBR_AGGRMNT"

    built.loc[
        built["agreement_name"].map(normalize_text).eq("") & wits_agreement,
        "agreement_name",
    ] = built["tariff_agreement_count"].map(
        lambda x: f"WITS reported agreement count ({int(float(x))})" if pd.notna(x) else ""
    )

    built["score_method"] = (
        "wto_ttd_v1 = clamp(100 - 4*effective_applied_tariff_pct "
        "- ntm_penalty_points - trade_remedy_penalty_points)"
    )
    built["source_stack"] = (
        "WTO TTD bilateral imports + WITS pair indicator fallbacks "
        "+ WTO reporter MFN + WITS bilateral tariffs/agreements + local penalty registry"
    )
    built["score_status"] = "built"

    built["notes"] = built.apply(
        lambda row: note_join(
            [
                "Trade value sourced from WITS fallback." if row["trade_value_source"] == "wits_mprt_trd_vl_total_fallback" else "",
                "Trade value unavailable from WTO/WITS; set to 0 for full matrix coverage."
                if row["trade_value_source"] == "imputed_zero_no_trade_data"
                else "",
                "Bilateral tariff missing; reporter MFN fallback applied." if row["tariff_basis"] == "reporter_mfn_fallback" else "",
                "Bilateral and MFN tariff missing; used reporter median tariff fallback."
                if row["tariff_basis"] == "imputed_reporter_median_fallback"
                else "",
                "Bilateral and MFN tariff missing; used global median tariff fallback."
                if row["tariff_basis"] == "imputed_global_median_fallback"
                else "",
                normalize_text(row.get("penalty_notes", "")),
            ]
        ),
        axis=1,
    )

    built["reporter_mfn_weighted_applied_tariff_pct"] = built["trade_weighted_mfn_pct_num"]
    built["reporter_mfn_simple_avg_effectively_applied_pct"] = built["simple_average_mfn_pct_num"]

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
        "trade_weighted_applied_tariff_pct_num",
        "simple_avg_effectively_applied_pct_num",
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
        "trade_value_source",
        "tariff_basis",
        "reporter_mfn_weighted_applied_tariff_pct",
        "reporter_mfn_simple_avg_effectively_applied_pct",
        "wits_bilateral_weighted_tariff_pct_num",
        "wits_bilateral_simple_tariff_pct_num",
        "wits_tariff_agreement_count_num",
    ]

    built = built[keep_cols].rename(
        columns={
            "trade_weighted_applied_tariff_pct_num": "trade_weighted_applied_tariff_pct",
            "simple_avg_effectively_applied_pct_num": "simple_avg_effectively_applied_pct",
            "wits_bilateral_weighted_tariff_pct_num": "wits_bilateral_weighted_tariff_pct",
            "wits_bilateral_simple_tariff_pct_num": "wits_bilateral_simple_tariff_pct",
            "wits_tariff_agreement_count_num": "wits_tariff_agreement_count",
        }
    )

    built = built.sort_values(["year", "reporter_id", "partner_id"], kind="stable")

    inputs_csv = out_dir / "goods_score_inputs_live.csv"
    scores_csv = out_dir / "goods_trade_scores_live.csv"
    scores_json = out_dir / "goods_trade_scores_live.json"

    built.to_csv(inputs_csv, index=False)
    built.to_csv(scores_csv, index=False)
    write_json(scores_json, built.to_dict(orient="records"))

    print(f"Rows built: {len(built)}")
    print(f"Rows using WITS bilateral tariffs: {int((built['tariff_basis'] == 'wits_bilateral_applied').sum())}")
    print(f"Rows using MFN fallback tariffs: {int((built['tariff_basis'] == 'reporter_mfn_fallback').sum())}")
    print(f"Rows using WITS trade fallback: {int((built['trade_value_source'] == 'wits_mprt_trd_vl_total_fallback').sum())}")
    print(f"Wrote: {inputs_csv}")
    print(f"Wrote: {scores_csv}")
    print(f"Wrote: {scores_json}")


if __name__ == "__main__":
    main()
