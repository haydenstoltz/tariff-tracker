from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SCORES_FILE = ROOT / "outputs" / "worldwide" / "goods_trade_scores_live.csv"
DEFAULT_OVERRIDES_FILE = ROOT / "data" / "metadata" / "worldwide_bilateral_preferences.csv"
DEFAULT_COVERAGE_FILE = ROOT / "outputs" / "worldwide" / "worldwide_bilateral_preference_coverage.csv"

SCORE_REQUIRED_COLUMNS = [
    "year",
    "pair_id",
    "pair_label",
    "reporter_id",
    "reporter_name",
    "partner_id",
    "partner_name",
    "trade_value_usd_m",
    "trade_weighted_applied_tariff_pct",
    "simple_avg_effectively_applied_pct",
    "ntm_penalty_points",
    "trade_remedy_penalty_points",
    "goods_score_live_v1",
    "rta_in_force",
    "agreement_id",
    "agreement_name",
    "notes",
]

OVERRIDE_REQUIRED_COLUMNS = [
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


def to_float_or_none(value: object) -> float | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def format_num(value: float | None, digits: int = 3) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in df.columns:
        df[col] = df[col].map(normalize_text)
    return df


def build_override_template(scores: pd.DataFrame) -> pd.DataFrame:
    template = scores[scores["rta_in_force"].str.lower() == "yes"].copy()

    if template.empty:
        return pd.DataFrame(columns=OVERRIDE_REQUIRED_COLUMNS)

    keep_cols = [
        "year",
        "pair_id",
        "reporter_id",
        "reporter_name",
        "partner_id",
        "partner_name",
        "agreement_id",
        "agreement_name",
    ]
    template = template[keep_cols].drop_duplicates().copy()

    template["bilateral_preferential_tariff_pct"] = ""
    template["bilateral_simple_avg_tariff_pct"] = ""
    template["source_label"] = ""
    template["source_url"] = ""
    template["notes"] = ""

    template = template.sort_values(
        ["year", "reporter_name", "partner_name", "pair_id"],
        ascending=[False, True, True, True],
        kind="stable",
    ).reset_index(drop=True)

    return template[OVERRIDE_REQUIRED_COLUMNS]


def recompute_live_v1_score(
    weighted_tariff_pct: float | None,
    ntm_penalty_points: float | None,
    trade_remedy_penalty_points: float | None,
) -> float | None:
    if weighted_tariff_pct is None:
        return None

    ntm = 0.0 if ntm_penalty_points is None else ntm_penalty_points
    remedy = 0.0 if trade_remedy_penalty_points is None else trade_remedy_penalty_points

    # Clamp to the same 0-100 scale used by the live scorer.
    score = 100.0 - (4.0 * weighted_tariff_pct) - ntm - remedy
    return round(max(0.0, min(100.0, score)), 3)


def load_override_maps(overrides: pd.DataFrame) -> tuple[dict[tuple[str, str], dict], dict[str, dict]]:
    exact_map: dict[tuple[str, str], dict] = {}
    pair_default_map: dict[str, dict] = {}

    for _, row in overrides.iterrows():
        row_dict = {col: normalize_text(row.get(col, "")) for col in overrides.columns}
        pair_id = row_dict["pair_id"]
        year = row_dict["year"]

        if not pair_id:
            continue

        if year:
            exact_map[(year, pair_id)] = row_dict
        else:
            pair_default_map[pair_id] = row_dict

    return exact_map, pair_default_map


def build_coverage(scores: pd.DataFrame) -> pd.DataFrame:
    work = scores.copy()
    work["trade_value_usd_m_num"] = pd.to_numeric(work["trade_value_usd_m"], errors="coerce").fillna(0.0)

    rows: list[dict[str, object]] = []

    grouped = work.groupby(["year", "reporter_id", "reporter_name"], dropna=False, sort=True)

    for (year, reporter_id, reporter_name), g in grouped:
        total_pairs = int(len(g))
        agreement_pairs = int((g["rta_in_force"].str.lower() == "yes").sum())
        bilateral_pairs = int((g["tariff_basis"] == "bilateral_preferential").sum())

        total_trade = float(g["trade_value_usd_m_num"].sum())
        agreement_trade = float(g.loc[g["rta_in_force"].str.lower() == "yes", "trade_value_usd_m_num"].sum())
        bilateral_trade = float(g.loc[g["tariff_basis"] == "bilateral_preferential", "trade_value_usd_m_num"].sum())

        rows.append(
            {
                "year": year,
                "reporter_id": reporter_id,
                "reporter_name": reporter_name,
                "total_pairs": total_pairs,
                "agreement_pairs": agreement_pairs,
                "bilateralized_pairs": bilateral_pairs,
                "agreement_pair_coverage_pct": round((100.0 * bilateral_pairs / agreement_pairs), 3) if agreement_pairs else "",
                "total_trade_value_usd_m": round(total_trade, 3),
                "agreement_trade_value_usd_m": round(agreement_trade, 3),
                "bilateralized_trade_value_usd_m": round(bilateral_trade, 3),
                "overall_trade_bilateralized_share_pct": round((100.0 * bilateral_trade / total_trade), 3) if total_trade else "",
                "agreement_trade_bilateralized_share_pct": round((100.0 * bilateral_trade / agreement_trade), 3) if agreement_trade else "",
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(
            ["year", "reporter_name"],
            ascending=[False, True],
            kind="stable",
        ).reset_index(drop=True)

    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Apply pair-specific bilateral preferential tariff overrides to the worldwide score CSV. "
            "If the override file does not exist, a template will be created from current in-force agreement pairs."
        )
    )
    parser.add_argument("--scores-file", default="", help="Path to outputs/worldwide/goods_trade_scores_live.csv")
    parser.add_argument("--overrides-file", default="", help="Path to data/metadata/worldwide_bilateral_preferences.csv")
    parser.add_argument("--coverage-file", default="", help="Path to output coverage CSV")
    args = parser.parse_args()

    scores_file = resolve_path(args.scores_file, DEFAULT_SCORES_FILE)
    overrides_file = resolve_path(args.overrides_file, DEFAULT_OVERRIDES_FILE)
    coverage_file = resolve_path(args.coverage_file, DEFAULT_COVERAGE_FILE)

    scores = read_csv(scores_file)
    require_columns(scores, SCORE_REQUIRED_COLUMNS, scores_file.name)

    if not overrides_file.exists():
        template = build_override_template(scores)
        overrides_file.parent.mkdir(parents=True, exist_ok=True)
        template.to_csv(overrides_file, index=False)
        print(f"Wrote bilateral preference template: {overrides_file}")
        print("Populate bilateral_preferential_tariff_pct and optional bilateral_simple_avg_tariff_pct using official pair-level data, then rerun this script.")
        return

    overrides = read_csv(overrides_file)
    require_columns(overrides, OVERRIDE_REQUIRED_COLUMNS, overrides_file.name)

    exact_map, pair_default_map = load_override_maps(overrides)

    updated_rows: list[dict[str, str]] = []
    bilateralized_count = 0

    for _, row in scores.iterrows():
        out = {col: normalize_text(row.get(col, "")) for col in scores.columns}

        year = out["year"]
        pair_id = out["pair_id"]

        override = exact_map.get((year, pair_id)) or pair_default_map.get(pair_id)

        existing_tariff_basis = normalize_text(out.get("tariff_basis", ""))
        original_weighted = to_float_or_none(out.get("trade_weighted_applied_tariff_pct", ""))
        original_simple = to_float_or_none(out.get("simple_avg_effectively_applied_pct", ""))
        existing_reporter_mfn_weighted = to_float_or_none(out.get("reporter_mfn_weighted_applied_tariff_pct", ""))
        existing_reporter_mfn_simple = to_float_or_none(out.get("reporter_mfn_simple_avg_effectively_applied_pct", ""))
        ntm = to_float_or_none(out.get("ntm_penalty_points", ""))
        remedy = to_float_or_none(out.get("trade_remedy_penalty_points", ""))

        pref_weighted = to_float_or_none(override.get("bilateral_preferential_tariff_pct", "")) if override else None
        pref_simple = to_float_or_none(override.get("bilateral_simple_avg_tariff_pct", "")) if override else None

        reporter_mfn_weighted = (
            existing_reporter_mfn_weighted
            if existing_reporter_mfn_weighted is not None
            else original_weighted
        )
        reporter_mfn_simple = (
            existing_reporter_mfn_simple
            if existing_reporter_mfn_simple is not None
            else original_simple
        )

        if pref_weighted is not None:
            effective_weighted = pref_weighted
            tariff_basis = "bilateral_preferential"
            bilateralized_count += 1
        else:
            effective_weighted = original_weighted
            tariff_basis = existing_tariff_basis or "reporter_mfn_fallback"

        effective_simple = pref_simple if pref_simple is not None else original_simple
        recomputed_score = recompute_live_v1_score(effective_weighted, ntm, remedy)

        out["reporter_mfn_weighted_applied_tariff_pct"] = format_num(reporter_mfn_weighted)
        out["reporter_mfn_simple_avg_effectively_applied_pct"] = format_num(reporter_mfn_simple)
        out["bilateral_preferential_tariff_pct"] = format_num(pref_weighted)
        out["bilateral_simple_avg_tariff_pct"] = format_num(pref_simple)
        out["tariff_basis"] = tariff_basis
        out["preference_source_label"] = normalize_text(override.get("source_label", "")) if override else ""
        out["preference_source_url"] = normalize_text(override.get("source_url", "")) if override else ""
        out["preference_notes"] = normalize_text(override.get("notes", "")) if override else ""

        out["trade_weighted_applied_tariff_pct"] = format_num(effective_weighted)
        out["simple_avg_effectively_applied_pct"] = format_num(effective_simple)
        out["goods_score_live_v1"] = format_num(recomputed_score)

        updated_rows.append(out)

    updated = pd.DataFrame(updated_rows)

    preferred_order = list(scores.columns)
    extra_cols = [
        "reporter_mfn_weighted_applied_tariff_pct",
        "reporter_mfn_simple_avg_effectively_applied_pct",
        "bilateral_preferential_tariff_pct",
        "bilateral_simple_avg_tariff_pct",
        "tariff_basis",
        "preference_source_label",
        "preference_source_url",
        "preference_notes",
    ]
    ordered_cols = preferred_order + [c for c in extra_cols if c not in preferred_order]
    ordered_cols += [c for c in updated.columns if c not in ordered_cols]
    updated = updated[ordered_cols]

    updated.to_csv(scores_file, index=False)

    coverage = build_coverage(updated)
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    coverage.to_csv(coverage_file, index=False)

    print(f"Updated score file: {scores_file}")
    print(f"Wrote coverage file: {coverage_file}")
    print(f"Rows processed: {len(updated)}")
    print(f"Rows using bilateral preferential overrides: {bilateralized_count}")


if __name__ == "__main__":
    main()
