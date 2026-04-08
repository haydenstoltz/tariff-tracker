from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SCORES_FILE = ROOT / "outputs" / "worldwide" / "goods_trade_scores_live.csv"
DEFAULT_REGISTRY_FILE = ROOT / "outputs" / "worldwide" / "country_pair_registry.csv"
DEFAULT_TARGETS_FILE = ROOT / "data" / "metadata" / "world" / "pair_pull_targets.csv"
DEFAULT_IMPORT_COVERAGE_FILE = ROOT / "outputs" / "worldwide" / "worldwide_import_batch_coverage.csv"
DEFAULT_IMPORT_QUEUE_FILE = ROOT / "outputs" / "worldwide" / "worldwide_import_acquisition_queue.csv"
DEFAULT_SITE_DATA_DIR = ROOT / "site" / "data"
DEFAULT_ACTOR_CODE_MAP_FILE = ROOT / "data" / "metadata" / "world" / "wto_actor_code_map.csv"
DEFAULT_REPORTER_GEO_OVERRIDES_FILE = ROOT / "data" / "metadata" / "world" / "world_reporter_geography_overrides.csv"

ACTOR_CODE_MAP_REQUIRED_COLUMNS = [
    "actor_id",
    "canonical_name",
]

REPORTER_GEO_OVERRIDE_REQUIRED_COLUMNS = [
    "reporter_id",
    "map_status",
    "fill_mode",
    "click_mode",
    "feature_name_candidates",
    "notes",
]

DEFAULT_FEATURE_NAME_CANDIDATE_OVERRIDES = {
    "United States": ["United States of America"],
    "Congo, The Democratic Republic of the": ["Democratic Republic of the Congo"],
    "Congo": ["Republic of the Congo"],
    "Bolivia, Plurinational State of": ["Bolivia"],
    "Iran, Islamic Republic of": ["Iran"],
    "Lao People's Democratic Republic": ["Laos"],
    "Moldova, Republic of": ["Moldova"],
    "Tanzania, United Republic of": ["Tanzania"],
    "Viet Nam": ["Vietnam"],
    "Brunei Darussalam": ["Brunei"],
    "Cabo Verde": ["Cape Verde"],
    "Syrian Arab Republic": ["Syria"],
    "Venezuela, Bolivarian Republic of": ["Venezuela"],
    "Korea, Democratic People's Republic of": ["North Korea"],
    "Taiwan, Province of China": ["Taiwan"],
    "Eswatini": ["eSwatini", "Swaziland"],
    "North Macedonia": ["North Macedonia", "Macedonia"],
    "Macao": ["Macao", "Macau"],
    "Hong Kong": ["Hong Kong", "Hong Kong SAR"],
}

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
    "tariff_agreement_count",
    "ntm_penalty_points",
    "trade_remedy_penalty_points",
    "goods_score_live_v1",
    "rta_in_force",
    "agreement_id",
    "agreement_name",
    "score_status",
    "notes",
]

REGISTRY_REQUIRED_COLUMNS = [
    "pair_id",
    "pair_label",
    "reporter_id",
    "reporter_name",
    "partner_id",
    "partner_name",
]

TARGETS_REQUIRED_COLUMNS = [
    "year",
    "reporter_id",
    "partner_id",
    "enabled_flag",
]

IMPORT_COVERAGE_REQUIRED_COLUMNS = [
    "year",
    "reporter_id",
    "expected_batch_filename",
    "file_present",
]

IMPORT_QUEUE_REQUIRED_COLUMNS = [
    "priority_rank",
    "year",
    "batch_id",
    "reporter_id",
    "reporter_iso3",
    "reporter_name",
    "canonical_name",
    "wto_reporter_code",
    "observed_partner_trade_usd_m_from_present_reporters",
    "observed_present_reporter_count_importing_from_partner",
    "priority_basis",
    "source_portal",
    "source_section",
    "flow",
    "product_scope",
    "format",
    "expected_batch_filename",
    "expected_batch_path",
    "download_status",
    "source_family",
    "notes",
]

NUMERIC_COLUMNS = [
    "trade_value_usd_m",
    "trade_weighted_applied_tariff_pct",
    "simple_avg_effectively_applied_pct",
    "tariff_agreement_count",
    "ntm_penalty_points",
    "trade_remedy_penalty_points",
    "goods_score_live_v1",
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


def to_number_or_none(value: object) -> float | int | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        n = float(text)
    except ValueError:
        return None
    if n.is_integer():
        return int(n)
    return round(n, 3)


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def unique_strings(values: list[object]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = normalize_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def split_feature_name_candidates(value: object) -> list[str]:
    if isinstance(value, list):
        return unique_strings(value)
    return unique_strings(str(value or "").split("|"))


def default_feature_name_candidates(display_name: str, canonical_name: str) -> list[str]:
    base_names = unique_strings([display_name, canonical_name])
    candidates = list(base_names)
    for name in base_names:
        candidates.extend(DEFAULT_FEATURE_NAME_CANDIDATE_OVERRIDES.get(name, []))
    return unique_strings(candidates)


def collect_reporter_display_names(
    scores: pd.DataFrame,
    registry: pd.DataFrame,
    targets: pd.DataFrame,
    import_coverage: pd.DataFrame,
    import_queue: pd.DataFrame,
    actor_code_map: pd.DataFrame,
) -> dict[str, str]:
    reporter_names: dict[str, str] = {}

    def remember(reporter_id: object, name: object) -> None:
        key = normalize_text(reporter_id).upper()
        label = normalize_text(name)
        if not key:
            return
        if key not in reporter_names or (not reporter_names[key] and label):
            reporter_names[key] = label

    for _, row in scores.iterrows():
        remember(row["reporter_id"], row["reporter_name"])

    for _, row in registry.iterrows():
        remember(row["reporter_id"], row["reporter_name"])

    for _, row in import_queue.iterrows():
        remember(
            row["reporter_id"],
            row.get("reporter_name", "") or row.get("canonical_name", ""),
        )

    additional_reporter_ids = pd.concat(
        [
            targets["reporter_id"],
            import_coverage["reporter_id"],
        ],
        ignore_index=True,
    )

    for reporter_id in additional_reporter_ids.tolist():
        key = normalize_text(reporter_id).upper()
        if key and key not in reporter_names:
            reporter_names[key] = ""

    for _, row in actor_code_map.iterrows():
        remember(row["actor_id"], row["canonical_name"])

    return reporter_names


def build_reporter_geo_rows(
    reporter_names: dict[str, str],
    actor_code_map: pd.DataFrame,
    geo_overrides: pd.DataFrame,
) -> list[dict]:
    canonical_name_by_id = {
        normalize_text(row["actor_id"]).upper(): normalize_text(row["canonical_name"])
        for _, row in actor_code_map.iterrows()
        if normalize_text(row["actor_id"])
    }

    override_by_id = {
        normalize_text(row["reporter_id"]).upper(): row
        for _, row in geo_overrides.iterrows()
        if normalize_text(row["reporter_id"])
    }

    rows: list[dict] = []

    for reporter_id in sorted(reporter_names):
        display_name = normalize_text(reporter_names.get(reporter_id, "")) or canonical_name_by_id.get(reporter_id, "")
        canonical_name = canonical_name_by_id.get(reporter_id, display_name)
        override = override_by_id.get(reporter_id)

        if override is not None:
            feature_name_candidates = split_feature_name_candidates(override["feature_name_candidates"])
            if not feature_name_candidates:
                feature_name_candidates = default_feature_name_candidates(display_name, canonical_name)

            map_status = normalize_text(override["map_status"]) or ("mapped" if feature_name_candidates else "non_mappable")
            fill_mode = normalize_text(override["fill_mode"]) or ("fill" if feature_name_candidates else "none")
            click_mode = normalize_text(override["click_mode"]) or ("select" if feature_name_candidates else "none")
            mapping_basis = "override"
            notes = normalize_text(override["notes"])
        else:
            feature_name_candidates = default_feature_name_candidates(display_name, canonical_name)
            map_status = "mapped" if feature_name_candidates else "non_mappable"
            fill_mode = "fill" if feature_name_candidates else "none"
            click_mode = "select" if feature_name_candidates else "none"
            mapping_basis = "canonical_default"
            notes = ""

        rows.append(
            {
                "reporter_id": reporter_id,
                "display_name": display_name,
                "canonical_name": canonical_name,
                "map_status": map_status,
                "fill_mode": fill_mode,
                "click_mode": click_mode,
                "feature_name_candidates": feature_name_candidates,
                "mapping_basis": mapping_basis,
                "notes": notes,
            }
        )

    return rows


def weighted_average(value_series: pd.Series, weight_series: pd.Series) -> float | None:
    frame = pd.DataFrame(
        {
            "value": pd.to_numeric(value_series, errors="coerce"),
            "weight": pd.to_numeric(weight_series, errors="coerce"),
        }
    )
    frame = frame[frame["value"].notna() & frame["weight"].notna() & (frame["weight"] > 0)].copy()
    if frame.empty:
        return None
    return round(float((frame["value"] * frame["weight"]).sum() / frame["weight"].sum()), 3)


def pct_or_none(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return round(float(100.0 * numerator / denominator), 3)


def score_sort_desc(group: pd.DataFrame) -> pd.DataFrame:
    return group.sort_values(
        by=["goods_score_live_v1_num", "trade_value_usd_m_num", "partner_name"],
        ascending=[False, False, True],
        na_position="last",
        kind="stable",
    )


def score_sort_asc(group: pd.DataFrame) -> pd.DataFrame:
    return group.sort_values(
        by=["goods_score_live_v1_num", "trade_value_usd_m_num", "partner_name"],
        ascending=[True, False, True],
        na_position="last",
        kind="stable",
    )


def trade_sort_desc(group: pd.DataFrame) -> pd.DataFrame:
    return group.sort_values(
        by=["trade_value_usd_m_num", "goods_score_live_v1_num", "partner_name"],
        ascending=[False, False, True],
        na_position="last",
        kind="stable",
    )


def build_score_rows(scores: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for _, row in scores.iterrows():
        rows.append(
            {
                "year": normalize_text(row["year"]),
                "pair_id": normalize_text(row["pair_id"]),
                "pair_label": normalize_text(row["pair_label"]),
                "reporter_id": normalize_text(row["reporter_id"]),
                "reporter_name": normalize_text(row["reporter_name"]),
                "partner_id": normalize_text(row["partner_id"]),
                "partner_name": normalize_text(row["partner_name"]),
                "trade_value_usd_m": to_number_or_none(row["trade_value_usd_m"]),
                "trade_weighted_applied_tariff_pct": to_number_or_none(row["trade_weighted_applied_tariff_pct"]),
                "simple_avg_effectively_applied_pct": to_number_or_none(row["simple_avg_effectively_applied_pct"]),
                "tariff_agreement_count": to_number_or_none(row["tariff_agreement_count"]),
                "ntm_penalty_points": to_number_or_none(row["ntm_penalty_points"]),
                "trade_remedy_penalty_points": to_number_or_none(row["trade_remedy_penalty_points"]),
                "goods_score_live_v1": to_number_or_none(row["goods_score_live_v1"]),
                "rta_in_force": normalize_text(row["rta_in_force"]),
                "agreement_id": normalize_text(row["agreement_id"]),
                "agreement_name": normalize_text(row["agreement_name"]),
                "score_status": normalize_text(row["score_status"]),
                "notes": normalize_text(row["notes"]),
            }
        )
    return rows


def build_pair_rows(registry: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for _, row in registry.iterrows():
        rows.append(
            {
                "pair_id": normalize_text(row["pair_id"]),
                "pair_label": normalize_text(row["pair_label"]),
                "reporter_id": normalize_text(row["reporter_id"]),
                "reporter_name": normalize_text(row["reporter_name"]),
                "partner_id": normalize_text(row["partner_id"]),
                "partner_name": normalize_text(row["partner_name"]),
            }
        )
    return rows

def build_import_queue_rows(queue: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    if queue.empty:
        return rows

    queue = queue.copy()
    queue["priority_rank_num"] = pd.to_numeric(queue["priority_rank"], errors="coerce")
    queue = queue.sort_values(
        by=["priority_rank_num", "reporter_id"],
        ascending=[True, True],
        kind="stable",
        na_position="last",
    )

    for _, row in queue.iterrows():
        rows.append(
            {
                "priority_rank": to_number_or_none(row["priority_rank"]),
                "year": normalize_text(row["year"]),
                "batch_id": normalize_text(row["batch_id"]),
                "reporter_id": normalize_text(row["reporter_id"]),
                "reporter_iso3": normalize_text(row["reporter_iso3"]),
                "reporter_name": normalize_text(row["reporter_name"]),
                "canonical_name": normalize_text(row["canonical_name"]),
                "wto_reporter_code": normalize_text(row["wto_reporter_code"]),
                "observed_partner_trade_usd_m_from_present_reporters": to_number_or_none(
                    row["observed_partner_trade_usd_m_from_present_reporters"]
                ),
                "observed_present_reporter_count_importing_from_partner": to_number_or_none(
                    row["observed_present_reporter_count_importing_from_partner"]
                ),
                "priority_basis": normalize_text(row["priority_basis"]),
                "source_portal": normalize_text(row["source_portal"]),
                "source_section": normalize_text(row["source_section"]),
                "flow": normalize_text(row["flow"]),
                "product_scope": normalize_text(row["product_scope"]),
                "format": normalize_text(row["format"]),
                "expected_batch_filename": normalize_text(row["expected_batch_filename"]),
                "expected_batch_path": normalize_text(row["expected_batch_path"]),
                "download_status": normalize_text(row["download_status"]),
                "source_family": normalize_text(row["source_family"]),
                "notes": normalize_text(row["notes"]),
            }
        )

    return rows

def build_country_outputs(scores: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    summary_rows: list[dict] = []
    detail_rows: list[dict] = []

    grouped = scores.groupby(["year", "reporter_id", "reporter_name"], sort=True, dropna=False)

    for (year, reporter_id, reporter_name), group in grouped:
        group = group.copy()

        total_trade = float(group["trade_value_usd_m_num"].fillna(0).sum())
        covered_partner_count = int(group["partner_id"].astype(str).str.strip().ne("").sum())

        best_group = score_sort_desc(group)
        worst_group = score_sort_asc(group)
        trade_rank_group = trade_sort_desc(group)
        score_rank_group = score_sort_desc(group)

        trade_rank_map = {
            normalize_text(row["pair_id"]): idx
            for idx, (_, row) in enumerate(trade_rank_group.iterrows(), start=1)
        }
        score_rank_map = {
            normalize_text(row["pair_id"]): idx
            for idx, (_, row) in enumerate(score_rank_group.iterrows(), start=1)
        }

        best_row = best_group.iloc[0] if not best_group.empty else None
        worst_row = worst_group.iloc[0] if not worst_group.empty else None

        agreement_trade = float(
            group.loc[group["rta_in_force"].astype(str).str.lower() == "yes", "trade_value_usd_m_num"]
            .fillna(0)
            .sum()
        )

        summary_rows.append(
            {
                "reporter_id": normalize_text(reporter_id),
                "reporter_name": normalize_text(reporter_name),
                "year": normalize_text(year),
                "covered_partner_count": covered_partner_count,
                "total_trade_value_usd_m": round(total_trade, 3),
                "weighted_goods_score": weighted_average(group["goods_score_live_v1_num"], group["trade_value_usd_m_num"]),
                "weighted_applied_tariff_pct": weighted_average(
                    group["trade_weighted_applied_tariff_pct_num"],
                    group["trade_value_usd_m_num"],
                ),
                "agreement_trade_share_pct": pct_or_none(agreement_trade, total_trade),
                "worst_partner_id": normalize_text(worst_row["partner_id"]) if worst_row is not None else "",
                "worst_partner_name": normalize_text(worst_row["partner_name"]) if worst_row is not None else "",
                "worst_partner_score": to_number_or_none(
                    worst_row["goods_score_live_v1_num"] if worst_row is not None else None
                ),
                "best_partner_id": normalize_text(best_row["partner_id"]) if best_row is not None else "",
                "best_partner_name": normalize_text(best_row["partner_name"]) if best_row is not None else "",
                "best_partner_score": to_number_or_none(
                    best_row["goods_score_live_v1_num"] if best_row is not None else None
                ),
            }
        )

        for _, row in trade_rank_group.iterrows():
            pair_id = normalize_text(row["pair_id"])
            trade_value = float(row["trade_value_usd_m_num"]) if pd.notna(row["trade_value_usd_m_num"]) else 0.0

            detail_rows.append(
                {
                    "year": normalize_text(row["year"]),
                    "pair_id": pair_id,
                    "pair_label": normalize_text(row["pair_label"]),
                    "reporter_id": normalize_text(row["reporter_id"]),
                    "reporter_name": normalize_text(row["reporter_name"]),
                    "partner_id": normalize_text(row["partner_id"]),
                    "partner_name": normalize_text(row["partner_name"]),
                    "trade_value_usd_m": to_number_or_none(row["trade_value_usd_m_num"]),
                    "reporter_total_trade_value_usd_m": round(total_trade, 3),
                    "reporter_trade_share_pct": pct_or_none(trade_value, total_trade),
                    "trade_weighted_applied_tariff_pct": to_number_or_none(row["trade_weighted_applied_tariff_pct_num"]),
                    "simple_avg_effectively_applied_pct": to_number_or_none(row["simple_avg_effectively_applied_pct_num"]),
                    "tariff_agreement_count": to_number_or_none(row["tariff_agreement_count_num"]),
                    "ntm_penalty_points": to_number_or_none(row["ntm_penalty_points_num"]),
                    "trade_remedy_penalty_points": to_number_or_none(row["trade_remedy_penalty_points_num"]),
                    "goods_score_live_v1": to_number_or_none(row["goods_score_live_v1_num"]),
                    "rta_in_force": normalize_text(row["rta_in_force"]),
                    "agreement_id": normalize_text(row["agreement_id"]),
                    "agreement_name": normalize_text(row["agreement_name"]),
                    "score_status": normalize_text(row["score_status"]),
                    "notes": normalize_text(row["notes"]),
                    "partner_trade_rank_desc": trade_rank_map.get(pair_id),
                    "partner_score_rank_high_to_low": score_rank_map.get(pair_id),
                }
            )

    summary_rows.sort(key=lambda r: (r["year"], r["reporter_name"]), reverse=True)
    detail_rows.sort(
        key=lambda r: (
            r["year"],
            r["reporter_name"],
            -(r["partner_trade_rank_desc"] or 999999),
            r["partner_name"],
        ),
        reverse=False,
    )

    return summary_rows, detail_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scores-file", default="", help="Path to goods_trade_scores_live.csv")
    parser.add_argument("--registry-file", default="", help="Path to country_pair_registry.csv")
    parser.add_argument("--targets-file", default="", help="Path to pair_pull_targets.csv")
    parser.add_argument("--import-coverage-file", default="", help="Path to worldwide_import_batch_coverage.csv")
    parser.add_argument("--import-queue-file", default="", help="Path to worldwide_import_acquisition_queue.csv")
    parser.add_argument("--site-data-dir", default="", help="Path to site/data")
    parser.add_argument("--actor-code-map-file", default="", help="Path to wto_actor_code_map.csv")
    parser.add_argument("--reporter-geo-overrides-file", default="", help="Path to world_reporter_geography_overrides.csv")
    args = parser.parse_args()

    scores_file = resolve_path(args.scores_file, DEFAULT_SCORES_FILE)
    registry_file = resolve_path(args.registry_file, DEFAULT_REGISTRY_FILE)
    targets_file = resolve_path(args.targets_file, DEFAULT_TARGETS_FILE)
    import_coverage_file = resolve_path(args.import_coverage_file, DEFAULT_IMPORT_COVERAGE_FILE)
    import_queue_file = resolve_path(args.import_queue_file, DEFAULT_IMPORT_QUEUE_FILE)
    site_data_dir = resolve_path(args.site_data_dir, DEFAULT_SITE_DATA_DIR)
    actor_code_map_file = resolve_path(args.actor_code_map_file, DEFAULT_ACTOR_CODE_MAP_FILE)
    reporter_geo_overrides_file = resolve_path(args.reporter_geo_overrides_file, DEFAULT_REPORTER_GEO_OVERRIDES_FILE)

    scores = pd.read_csv(scores_file, dtype=str, keep_default_na=False)
    registry = pd.read_csv(registry_file, dtype=str, keep_default_na=False)
    targets = pd.read_csv(targets_file, dtype=str, keep_default_na=False)
    import_coverage = pd.read_csv(import_coverage_file, dtype=str, keep_default_na=False)
    actor_code_map = pd.read_csv(actor_code_map_file, dtype=str, keep_default_na=False)

    if reporter_geo_overrides_file.exists():
        reporter_geo_overrides = pd.read_csv(reporter_geo_overrides_file, dtype=str, keep_default_na=False)
        require_columns(
            reporter_geo_overrides,
            REPORTER_GEO_OVERRIDE_REQUIRED_COLUMNS,
            "world_reporter_geography_overrides.csv",
        )
    else:
        reporter_geo_overrides = pd.DataFrame(columns=REPORTER_GEO_OVERRIDE_REQUIRED_COLUMNS)

    require_columns(actor_code_map, ACTOR_CODE_MAP_REQUIRED_COLUMNS, "wto_actor_code_map.csv")

    if import_queue_file.exists():
        import_queue = pd.read_csv(import_queue_file, dtype=str, keep_default_na=False)
        require_columns(import_queue, IMPORT_QUEUE_REQUIRED_COLUMNS, "worldwide_import_acquisition_queue.csv")
    else:
        import_queue = pd.DataFrame(columns=IMPORT_QUEUE_REQUIRED_COLUMNS)

    require_columns(scores, SCORE_REQUIRED_COLUMNS, "goods_trade_scores_live.csv")
    require_columns(registry, REGISTRY_REQUIRED_COLUMNS, "country_pair_registry.csv")
    require_columns(targets, TARGETS_REQUIRED_COLUMNS, "pair_pull_targets.csv")
    require_columns(import_coverage, IMPORT_COVERAGE_REQUIRED_COLUMNS, "worldwide_import_batch_coverage.csv")

    for df in [scores, registry, targets, import_coverage, import_queue, actor_code_map, reporter_geo_overrides]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    for col in NUMERIC_COLUMNS:
        scores[f"{col}_num"] = pd.to_numeric(scores[col], errors="coerce")

    score_rows = build_score_rows(scores)
    pair_rows = build_pair_rows(registry)
    country_summary_rows, country_partner_detail_rows = build_country_outputs(scores)

    reporter_display_names = collect_reporter_display_names(
        scores=scores,
        registry=registry,
        targets=targets,
        import_coverage=import_coverage,
        import_queue=import_queue,
        actor_code_map=actor_code_map,
    )
    reporter_geo_rows = build_reporter_geo_rows(
        reporter_names=reporter_display_names,
        actor_code_map=actor_code_map,
        geo_overrides=reporter_geo_overrides,
    )

    score_years = sorted(
        {normalize_text(row["year"]) for row in score_rows if normalize_text(row["year"])}
    )
    coverage_years = sorted(
        {normalize_text(x) for x in import_coverage["year"].tolist() if normalize_text(x)}
    )
    queue_years = sorted(
        {normalize_text(x) for x in import_queue["year"].tolist() if normalize_text(x)}
    )
    target_years = sorted(
        {normalize_text(x) for x in targets["year"].tolist() if normalize_text(x)}
    )

    latest_year = ""
    if score_years:
        latest_year = score_years[-1]
    elif coverage_years:
        latest_year = coverage_years[-1]
    elif queue_years:
        latest_year = queue_years[-1]
    elif target_years:
        latest_year = target_years[-1]

    targets_for_year = targets.copy()
    coverage_for_year = import_coverage.copy()
    queue_for_year = import_queue.copy()

    if latest_year:
        targets_for_year = targets_for_year[targets_for_year["year"] == latest_year].copy()
        coverage_for_year = coverage_for_year[coverage_for_year["year"] == latest_year].copy()
        queue_for_year = queue_for_year[queue_for_year["year"] == latest_year].copy()

    enabled_targets = targets_for_year[targets_for_year["enabled_flag"].str.lower() == "yes"].copy()
    enabled_pair_target_count = int(len(enabled_targets))

    if not coverage_for_year.empty:
        enabled_import_reporters = sorted(
            coverage_for_year["reporter_id"].drop_duplicates().tolist()
        )
    else:
        enabled_import_reporters = sorted(
            enabled_targets["reporter_id"].drop_duplicates().tolist()
        )

    present_import_reporters = sorted(
        coverage_for_year.loc[
            coverage_for_year["file_present"].str.lower() == "yes",
            "reporter_id",
        ].drop_duplicates().tolist()
    )
    missing_import_reporters = sorted(
        set(enabled_import_reporters) - set(present_import_reporters)
    )

    import_queue_rows = build_import_queue_rows(queue_for_year)

    manifest = {
        "dataset": "world_goods_scores",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "score_rows": len(score_rows),
        "pair_rows": len(pair_rows),
        "country_summary_rows": len(country_summary_rows),
        "country_partner_detail_rows": len(country_partner_detail_rows),
        "reporter_count": len({row["reporter_id"] for row in country_summary_rows if row["reporter_id"]}),
        "latest_year": latest_year,
        "score_method": "live_v1",
        "build_scope": "partial" if missing_import_reporters else "full",
        "enabled_pair_target_count": enabled_pair_target_count,
        "enabled_import_reporter_count": len(enabled_import_reporters),
        "present_import_reporter_count": len(present_import_reporters),
        "missing_import_reporter_count": len(missing_import_reporters),
        "present_import_reporter_ids": present_import_reporters,
        "missing_import_reporter_ids": missing_import_reporters,
        "import_queue_rows": len(import_queue_rows),
            "reporter_geography_rows": len(reporter_geo_rows),
        "reporter_geography_special_case_count": len(
            [row for row in reporter_geo_rows if normalize_text(row["map_status"]).lower() != "mapped"]
        ),
        "reporter_geography_non_mappable_count": len(
            [row for row in reporter_geo_rows if normalize_text(row["map_status"]).lower() == "non_mappable"]
        ),
        "reporter_geography_non_mappable_ids": [
            row["reporter_id"]
            for row in reporter_geo_rows
            if normalize_text(row["map_status"]).lower() == "non_mappable"
        ],
    }

    write_json(site_data_dir / "world_goods_scores.json", score_rows)
    write_json(site_data_dir / "world_pair_registry.json", pair_rows)
    write_json(site_data_dir / "world_country_summary.json", country_summary_rows)
    write_json(site_data_dir / "world_country_partner_detail.json", country_partner_detail_rows)
    write_json(site_data_dir / "world_import_acquisition_queue.json", import_queue_rows)
    write_json(site_data_dir / "world_reporter_geography.json", reporter_geo_rows)
    write_json(site_data_dir / "world_refresh_manifest.json", manifest)

    print(f"Wrote: {site_data_dir / 'world_goods_scores.json'}")
    print(f"Wrote: {site_data_dir / 'world_pair_registry.json'}")
    print(f"Wrote: {site_data_dir / 'world_country_summary.json'}")
    print(f"Wrote: {site_data_dir / 'world_country_partner_detail.json'}")
    print(f"Wrote: {site_data_dir / 'world_import_acquisition_queue.json'}")
    print(f"Wrote: {site_data_dir / 'world_reporter_geography.json'}")
    print(f"Wrote: {site_data_dir / 'world_refresh_manifest.json'}")


if __name__ == "__main__":
    main()