from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pandas as pd
from pandas.tseries.offsets import MonthEnd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_PREVIEW_META_DIR = ROOT / "outputs" / "spec_preview"
DEFAULT_PREVIEW_BUILD_TABLE_DIR = ROOT / "outputs" / "spec_preview_build" / "tables"
DEFAULT_BASE_SITE_DIR = ROOT / "site"
DEFAULT_OUT_SITE_DIR = ROOT / "outputs" / "spec_site_preview" / "site"


def resolve_path(path_str: str, default_path: Path) -> Path:
    if not path_str.strip():
        return default_path
    path = Path(path_str)
    if not path.is_absolute():
        path = ROOT / path
    return path


def normalize_text(x: object) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def to_float_or_none(x: object, digits: int = 3):
    if pd.isna(x) or str(x).strip() == "":
        return None
    return round(float(x), digits)


def to_int_or_none(x: object):
    if pd.isna(x) or str(x).strip() == "":
        return None
    return int(float(x))


def month_end(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(ts).to_period("M").to_timestamp("M")


def month_match_mask(series: pd.Series, ts: pd.Timestamp) -> pd.Series:
    series = pd.to_datetime(series, errors="coerce")
    target = month_end(ts).to_period("M")
    return series.dt.to_period("M") == target


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def copy_site_shell(base_site_dir: Path, out_site_dir: Path) -> None:
    out_site_dir.mkdir(parents=True, exist_ok=True)

    for filename in ["index.html", "app.js", "style.css"]:
        src = base_site_dir / filename
        if not src.exists():
            raise FileNotFoundError(f"Missing site shell file: {src}")
        shutil.copy2(src, out_site_dir / filename)


def get_month_value(df: pd.DataFrame, column: str, ts: pd.Timestamp):
    row = df.loc[month_match_mask(df["date"], ts)]
    if row.empty:
        return None
    return float(row[column].iloc[0])


def compute_placebo_stats(
    merged: pd.DataFrame,
    event_month: pd.Timestamp,
    horizon: int,
    actual_effect: float | None,
) -> tuple[int | None, float | None]:
    if actual_effect is None:
        return None, None

    placebo_effects = []
    months = sorted(merged["date"].dt.to_period("M").drop_duplicates().tolist())
    event_period = event_month.to_period("M")

    for placebo_period in months:
        if placebo_period >= event_period:
            continue

        placebo_month = placebo_period.to_timestamp("M")
        target_month = placebo_month + MonthEnd(horizon)

        if target_month.to_period("M") >= event_period:
            continue

        start_val = get_month_value(merged, "relative_effect", placebo_month)
        end_val = get_month_value(merged, "relative_effect", target_month)

        if start_val is None or end_val is None:
            continue

        placebo_effects.append(end_val - start_val)

    if not placebo_effects:
        return 0, None

    p_abs = sum(abs(x) >= abs(actual_effect) for x in placebo_effects) / len(placebo_effects)
    return len(placebo_effects), round(float(p_abs), 3)


def build_case_outputs(
    site_row: pd.Series,
    panel_df: pd.DataFrame,
    charts_dir: Path,
    csv_dir: Path,
) -> dict:
    case_id = normalize_text(site_row["case_id"])
    case_name = normalize_text(site_row["case_name"])
    source_type = normalize_text(site_row["source_type"])
    treatment_label = normalize_text(site_row["treatment_label"])
    control_label = normalize_text(site_row["control_label"])
    event_month = month_end(pd.to_datetime(site_row["effective_date"]))

    case_panel = panel_df[
        (panel_df["case_name"].astype(str).str.strip() == case_name)
        & (panel_df["source_type"].astype(str).str.strip() == source_type)
    ].copy()

    if case_panel.empty:
        raise ValueError(
            f"No matching panel rows for case_id='{case_id}', case_name='{case_name}', source_type='{source_type}'"
        )

    treatment = (
        case_panel[case_panel["series_label"].astype(str).str.strip() == treatment_label][["date", "rebased_100"]]
        .rename(columns={"rebased_100": "treatment"})
        .copy()
    )

    control = (
        case_panel[case_panel["series_label"].astype(str).str.strip() == control_label][["date", "rebased_100"]]
        .rename(columns={"rebased_100": "control"})
        .copy()
    )

    if treatment.empty:
        raise ValueError(
            f"No treatment rows found for case_id='{case_id}', treatment_label='{treatment_label}'"
        )
    if control.empty:
        raise ValueError(
            f"No control rows found for case_id='{case_id}', control_label='{control_label}'"
        )

    merged = treatment.merge(control, on="date", how="inner").sort_values("date").copy()
    if merged.empty:
        raise ValueError(f"No overlapping treatment/control dates for case_id='{case_id}'")

    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    merged = merged.dropna(subset=["date"]).copy()
    merged["relative_effect"] = merged["treatment"] - merged["control"]

    pre_event = merged.loc[merged["date"] < event_month, "relative_effect"]
    pre_event_gap_std_pp = round(float(pre_event.std(ddof=0)), 3) if not pre_event.empty else None

    post_event = merged.loc[merged["date"] >= event_month].copy()
    if post_event.empty:
        peak_post_gap_pp = None
        peak_post_gap_month = ""
    else:
        peak_idx = post_event["relative_effect"].abs().idxmax()
        peak_post_gap_pp = round(float(post_event.loc[peak_idx, "relative_effect"]), 3)
        peak_post_gap_month = pd.Timestamp(post_event.loc[peak_idx, "date"]).strftime("%Y-%m")

    effect_3m = get_month_value(merged, "relative_effect", event_month + MonthEnd(3))
    effect_6m = get_month_value(merged, "relative_effect", event_month + MonthEnd(6))
    effect_12m = get_month_value(merged, "relative_effect", event_month + MonthEnd(12))

    placebo_n_3m, placebo_p_abs_3m = compute_placebo_stats(merged, event_month, 3, effect_3m)
    placebo_n_6m, placebo_p_abs_6m = compute_placebo_stats(merged, event_month, 6, effect_6m)

    chart_payload = {
        "labels": merged["date"].dt.strftime("%Y-%m").tolist(),
        "treatment": [round(float(x), 3) for x in merged["treatment"].tolist()],
        "control": [round(float(x), 3) for x in merged["control"].tolist()],
        "relative_effect": [round(float(x), 3) for x in merged["relative_effect"].tolist()],
    }
    write_json(charts_dir / f"{case_id}.json", chart_payload)

    csv_out = merged.copy()
    csv_out["month"] = csv_out["date"].dt.strftime("%Y-%m")
    csv_out = csv_out[["month", "treatment", "control", "relative_effect"]]
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_out.to_csv(csv_dir / f"{case_id}.csv", index=False)

    return {
        "case_id": case_id,
        "case_name": case_name,
        "status": normalize_text(site_row["site_status"]),
        "source_type": source_type,
        "treatment_series": treatment_label,
        "control_series": control_label,
        "effect_3m_pp": to_float_or_none(effect_3m),
        "effect_6m_pp": to_float_or_none(effect_6m),
        "effect_12m_pp": to_float_or_none(effect_12m),
        "pre_event_gap_std_pp": pre_event_gap_std_pp,
        "peak_post_gap_pp": peak_post_gap_pp,
        "peak_post_gap_month": peak_post_gap_month,
        "placebo_n_3m": placebo_n_3m,
        "placebo_p_abs_3m": placebo_p_abs_3m,
        "placebo_n_6m": placebo_n_6m,
        "placebo_p_abs_6m": placebo_p_abs_6m,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preview-meta-dir",
        default="",
        help="Directory containing spec-materialized preview metadata CSVs.",
    )
    parser.add_argument(
        "--preview-build-table-dir",
        default="",
        help="Directory containing preview-build case-study tables.",
    )
    parser.add_argument(
        "--base-site-dir",
        default="",
        help="Base site shell directory to copy index.html/app.js/style.css from.",
    )
    parser.add_argument(
        "--out-site-dir",
        default="",
        help="Output directory for runnable preview site.",
    )
    args = parser.parse_args()

    preview_meta_dir = resolve_path(args.preview_meta_dir, DEFAULT_PREVIEW_META_DIR)
    preview_build_table_dir = resolve_path(args.preview_build_table_dir, DEFAULT_PREVIEW_BUILD_TABLE_DIR)
    base_site_dir = resolve_path(args.base_site_dir, DEFAULT_BASE_SITE_DIR)
    out_site_dir = resolve_path(args.out_site_dir, DEFAULT_OUT_SITE_DIR)

    site_cases_file = preview_meta_dir / "site_cases.csv"
    panel_file = preview_build_table_dir / "product_case_studies_panel.csv"
    final_summary_file = preview_build_table_dir / "final_case_summary_table.csv"

    if not site_cases_file.exists():
        raise FileNotFoundError(f"Missing preview metadata file: {site_cases_file}")
    if not panel_file.exists():
        raise FileNotFoundError(f"Missing preview build panel file: {panel_file}")

    meta = pd.read_csv(site_cases_file, keep_default_na=False)
    panel_df = pd.read_csv(panel_file, parse_dates=["date"], keep_default_na=False)

    required_meta_cols = [
        "event_id",
        "event_title",
        "authority",
        "country",
        "announced_date",
        "effective_date",
        "site_status",
        "case_id",
        "case_name",
        "source_type",
        "treatment_label",
        "control_label",
        "caveat",
        "robustness_note",
        "method_note",
    ]
    missing_meta = [c for c in required_meta_cols if c not in meta.columns]
    if missing_meta:
        raise ValueError(f"Missing metadata columns in preview site_cases.csv: {missing_meta}")

    required_panel_cols = [
        "case_name",
        "source_type",
        "series_label",
        "date",
        "rebased_100",
    ]
    missing_panel = [c for c in required_panel_cols if c not in panel_df.columns]
    if missing_panel:
        raise ValueError(f"Missing panel columns in preview product_case_studies_panel.csv: {missing_panel}")

    meta["announced_date"] = pd.to_datetime(meta["announced_date"], errors="coerce")
    meta["effective_date"] = pd.to_datetime(meta["effective_date"], errors="coerce")
    if meta["announced_date"].isna().any():
        bad = meta.loc[meta["announced_date"].isna(), "case_id"].tolist()
        raise ValueError(f"Invalid announced_date in preview site_cases.csv for case_ids: {bad}")
    if meta["effective_date"].isna().any():
        bad = meta.loc[meta["effective_date"].isna(), "case_id"].tolist()
        raise ValueError(f"Invalid effective_date in preview site_cases.csv for case_ids: {bad}")

    copy_site_shell(base_site_dir, out_site_dir)

    out_data_dir = out_site_dir / "data"
    charts_dir = out_data_dir / "charts"
    csv_dir = out_data_dir / "csv"

    if out_data_dir.exists():
        shutil.rmtree(out_data_dir)
    out_data_dir.mkdir(parents=True, exist_ok=True)

    tariffs = []
    cases = []
    summary = {}
    final_rows = []

    tariff_rows = (
        meta[
            [
                "event_id",
                "event_title",
                "authority",
                "country",
                "announced_date",
                "effective_date",
                "site_status",
            ]
        ]
        .drop_duplicates()
        .copy()
    )

    for _, row in tariff_rows.iterrows():
        tariffs.append(
            {
                "event_id": normalize_text(row["event_id"]),
                "title": normalize_text(row["event_title"]),
                "authority": normalize_text(row["authority"]),
                "country": normalize_text(row["country"]),
                "announced_date": row["announced_date"].strftime("%Y-%m-%d"),
                "effective_date": row["effective_date"].strftime("%Y-%m-%d"),
                "status": normalize_text(row["site_status"]),
            }
        )

    for _, row in meta.sort_values(["event_id", "case_id"]).iterrows():
        case_id = normalize_text(row["case_id"])

        final_row = build_case_outputs(
            site_row=row,
            panel_df=panel_df,
            charts_dir=charts_dir,
            csv_dir=csv_dir,
        )
        final_rows.append(final_row)

        summary[case_id] = {
            "m3": to_float_or_none(final_row["effect_3m_pp"]),
            "m6": to_float_or_none(final_row["effect_6m_pp"]),
            "m12": to_float_or_none(final_row["effect_12m_pp"]),
            "sign": "positive" if (final_row["effect_6m_pp"] is not None and final_row["effect_6m_pp"] >= 0) else "negative",
            "pre_event_gap_std_pp": to_float_or_none(final_row["pre_event_gap_std_pp"]),
            "peak_post_gap_pp": to_float_or_none(final_row["peak_post_gap_pp"]),
            "peak_post_gap_month": normalize_text(final_row["peak_post_gap_month"]),
            "placebo_n_3m": to_int_or_none(final_row["placebo_n_3m"]),
            "placebo_p_abs_3m": to_float_or_none(final_row["placebo_p_abs_3m"]),
            "placebo_n_6m": to_int_or_none(final_row["placebo_n_6m"]),
            "placebo_p_abs_6m": to_float_or_none(final_row["placebo_p_abs_6m"]),
        }

        cases.append(
            {
                "case_id": case_id,
                "event_id": normalize_text(row["event_id"]),
                "case_name": normalize_text(row["case_name"]),
                "source_type": normalize_text(row["source_type"]),
                "treatment_label": normalize_text(row["treatment_label"]),
                "control_label": normalize_text(row["control_label"]),
                "chart_file": f"./data/charts/{case_id}.json",
                "csv_file": f"./data/csv/{case_id}.csv",
                "caveat": normalize_text(row["caveat"]),
                "robustness_note": normalize_text(row["robustness_note"]),
                "method_note": normalize_text(row["method_note"]),
            }
        )

    final_df = pd.DataFrame(final_rows).sort_values(["case_name", "source_type"]).reset_index(drop=True)
    preview_build_table_dir.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(final_summary_file, index=False)

    write_json(out_data_dir / "tariffs.json", tariffs)
    write_json(out_data_dir / "cases.json", cases)
    write_json(out_data_dir / "summary.json", summary)

    print("Wrote preview site data:")
    print(f"- {out_data_dir / 'tariffs.json'}")
    print(f"- {out_data_dir / 'cases.json'}")
    print(f"- {out_data_dir / 'summary.json'}")
    print(f"- {charts_dir}")
    print(f"- {csv_dir}")
    print(f"- {final_summary_file}")
    print(f"- site shell copied to {out_site_dir}")


if __name__ == "__main__":
    main()