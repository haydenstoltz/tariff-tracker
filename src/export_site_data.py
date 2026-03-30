from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

EVENTS_FILE = ROOT / "data" / "metadata" / "tariff_events_master.csv"
CASE_META_FILE = ROOT / "data" / "metadata" / "site_cases.csv"
EVENT_CASE_MAP_FILE = ROOT / "data" / "metadata" / "event_case_map.csv"
FINAL_SUMMARY_FILE = ROOT / "outputs" / "tables" / "final_case_summary_table.csv"
PANEL_FILE = ROOT / "outputs" / "tables" / "product_case_studies_panel.csv"

OUT_DIR = ROOT / "site" / "data"
CHARTS_DIR = OUT_DIR / "charts"
CSV_DIR = OUT_DIR / "csv"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def normalize_text(x: object) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def normalize_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]) or str(out[col].dtype) == "string":
            out[col] = out[col].map(normalize_text)
    return out


def to_float_or_none(x: object, digits: int = 3):
    if pd.isna(x) or str(x).strip() == "":
        return None
    return round(float(x), digits)


def to_int_or_none(x: object):
    if pd.isna(x) or str(x).strip() == "":
        return None
    return int(float(x))


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def require_unique(df: pd.DataFrame, col: str, label: str) -> None:
    dupes = df.loc[df[col].duplicated(), col].astype(str).tolist()
    if dupes:
        raise ValueError(f"Duplicate {col} values in {label}: {dupes}")


def require_nonempty(df: pd.DataFrame, cols: list[str], label_col: str, label: str) -> None:
    problems = []
    for col in cols:
        bad = df[df[col].astype(str).str.strip() == ""]
        if not bad.empty:
            labels = bad[label_col].astype(str).tolist() if label_col in bad.columns else bad.index.astype(str).tolist()
            problems.append(f"{col}: {labels}")
    if problems:
        raise ValueError(f"Blank required values found in {label} -> " + " | ".join(problems))


def fmt_date_or_blank(x: object) -> str:
    ts = pd.to_datetime(x, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m-%d")


def main() -> None:
    events = normalize_object_columns(pd.read_csv(EVENTS_FILE, keep_default_na=False))
    case_meta = normalize_object_columns(pd.read_csv(CASE_META_FILE, keep_default_na=False))
    event_case_map = normalize_object_columns(pd.read_csv(EVENT_CASE_MAP_FILE, keep_default_na=False))
    final_df = normalize_object_columns(pd.read_csv(FINAL_SUMMARY_FILE, keep_default_na=False))
    panel_df = normalize_object_columns(pd.read_csv(PANEL_FILE, keep_default_na=False))

    require_columns(
        events,
        [
            "event_id",
            "event_title",
            "authority",
            "country_scope",
            "product_scope",
            "announced_date",
            "effective_date",
            "end_date",
            "status_bucket",
            "currently_active",
            "historical_flag",
            "rate_summary",
            "legal_source_label",
            "legal_source_url",
            "notes",
        ],
        "tariff_events_master.csv",
    )
    require_columns(
        case_meta,
        [
            "case_id",
            "case_name",
            "source_type",
            "treatment_label",
            "control_label",
            "confidence_tier",
            "rationale_short",
            "caveat",
            "robustness_note",
            "method_note",
            "site_status",
        ],
        "site_cases.csv",
    )
    require_columns(
        event_case_map,
        [
            "event_id",
            "case_id",
            "display_order",
            "primary_case_flag",
        ],
        "event_case_map.csv",
    )
    require_columns(
        final_df,
        [
            "case_name",
            "effect_3m_pp",
            "effect_6m_pp",
            "effect_12m_pp",
            "pre_event_gap_std_pp",
            "peak_post_gap_pp",
            "peak_post_gap_month",
            "placebo_n_3m",
            "placebo_p_abs_3m",
            "placebo_n_6m",
            "placebo_p_abs_6m",
        ],
        "final_case_summary_table.csv",
    )
    require_columns(
        panel_df,
        [
            "case_name",
            "source_type",
            "series_label",
            "date",
            "rebased_100",
        ],
        "product_case_studies_panel.csv",
    )

    require_unique(events, "event_id", "tariff_events_master.csv")
    require_unique(case_meta, "case_id", "site_cases.csv")

    require_nonempty(
        events,
        [
            "event_id",
            "event_title",
            "authority",
            "status_bucket",
            "legal_source_label",
            "legal_source_url",
        ],
        "event_id",
        "tariff_events_master.csv",
    )
    require_nonempty(
        case_meta,
        [
            "case_id",
            "case_name",
            "source_type",
            "treatment_label",
            "control_label",
            "site_status",
        ],
        "case_id",
        "site_cases.csv",
    )
    require_nonempty(
        event_case_map,
        [
            "event_id",
            "case_id",
            "display_order",
            "primary_case_flag",
        ],
        "case_id",
        "event_case_map.csv",
    )

    events["announced_date"] = pd.to_datetime(events["announced_date"], errors="coerce")
    events["effective_date"] = pd.to_datetime(events["effective_date"], errors="coerce")
    events["end_date"] = pd.to_datetime(events["end_date"], errors="coerce")
    panel_df["date"] = pd.to_datetime(panel_df["date"], errors="coerce")

    if events["effective_date"].isna().any():
        bad = events.loc[events["effective_date"].isna(), "event_id"].tolist()
        raise ValueError(f"Invalid effective_date values in tariff_events_master.csv for event_ids: {bad}")
    if panel_df["date"].isna().any():
        raise ValueError("Invalid date values found in product_case_studies_panel.csv")

    event_case_map["display_order"] = pd.to_numeric(event_case_map["display_order"], errors="coerce")
    if event_case_map["display_order"].isna().any():
        bad = event_case_map.loc[event_case_map["display_order"].isna(), "case_id"].tolist()
        raise ValueError(f"Invalid display_order values in event_case_map.csv for case_ids: {bad}")
    event_case_map["display_order"] = event_case_map["display_order"].astype(int)

    valid_site_status = {"live", "archived"}
    case_meta["site_status"] = case_meta["site_status"].str.lower()
    bad_site_status = case_meta.loc[~case_meta["site_status"].isin(valid_site_status), "case_id"].tolist()
    if bad_site_status:
        raise ValueError(
            f"Invalid site_status values in site_cases.csv for case_ids: {bad_site_status}. "
            f"Allowed values: {sorted(valid_site_status)}"
        )

    live_case_meta = case_meta[case_meta["site_status"] == "live"].copy()

    # site_cases.csv may still carry an event_id column from the older schema.
    # The event_case_map file is now the source of truth for case -> event linkage.
    # Drop the duplicated event_id here so the merge preserves a single canonical event_id.
    if "event_id" in live_case_meta.columns:
        live_case_meta = live_case_meta.drop(columns=["event_id"])

    unknown_event_ids = sorted(set(event_case_map["event_id"]) - set(events["event_id"]))
    if unknown_event_ids:
        raise ValueError(f"event_case_map.csv references unknown event_id values: {unknown_event_ids}")

    unknown_case_ids = sorted(set(event_case_map["case_id"]) - set(case_meta["case_id"]))
    if unknown_case_ids:
        raise ValueError(f"event_case_map.csv references unknown case_id values: {unknown_case_ids}")

    mapped_live = (
        event_case_map.merge(live_case_meta, on="case_id", how="inner", validate="one_to_one")
        .merge(events, on="event_id", how="left", validate="many_to_one", suffixes=("", "_event"))
        .copy()
    )

    final_df["case_name_norm"] = final_df["case_name"].astype(str).str.strip()
    panel_df["case_name_norm"] = panel_df["case_name"].astype(str).str.strip()
    panel_df["source_type_norm"] = panel_df["source_type"].astype(str).str.strip()
    panel_df["series_label_norm"] = panel_df["series_label"].astype(str).str.strip()

    live_case_count = mapped_live.groupby("event_id").size().to_dict()

    events = events.copy()
    events["original_order"] = range(len(events))
    events["has_live_cases"] = events["event_id"].map(lambda x: x in live_case_count)
    events["live_case_count"] = events["event_id"].map(lambda x: int(live_case_count.get(x, 0)))
    events = events.sort_values(
        by=["has_live_cases", "original_order"],
        ascending=[False, True],
        kind="stable",
    )

    tariffs = []
    for _, row in events.iterrows():
        tariffs.append(
            {
                "event_id": normalize_text(row["event_id"]),
                "title": normalize_text(row["event_title"]),
                "authority": normalize_text(row["authority"]),
                "country": normalize_text(row["country_scope"]),
                "country_scope": normalize_text(row["country_scope"]),
                "product_scope": normalize_text(row["product_scope"]),
                "announced_date": fmt_date_or_blank(row["announced_date"]),
                "effective_date": fmt_date_or_blank(row["effective_date"]),
                "end_date": fmt_date_or_blank(row["end_date"]),
                "status": normalize_text(row["status_bucket"]),
                "status_bucket": normalize_text(row["status_bucket"]),
                "currently_active": normalize_text(row["currently_active"]),
                "historical_flag": normalize_text(row["historical_flag"]),
                "rate_summary": normalize_text(row["rate_summary"]),
                "legal_source_label": normalize_text(row["legal_source_label"]),
                "legal_source_url": normalize_text(row["legal_source_url"]),
                "notes": normalize_text(row["notes"]),
                "has_live_cases": bool(row["has_live_cases"]),
                "live_case_count": int(row["live_case_count"]),
            }
        )

    cases = []
    summary = {}

    mapped_live = mapped_live.sort_values(["event_id", "display_order", "case_id"], kind="stable").copy()

    for _, row in mapped_live.iterrows():
        event_id = normalize_text(row["event_id"])
        case_id = normalize_text(row["case_id"])
        case_name = normalize_text(row["case_name"])
        source_type = normalize_text(row["source_type"])
        treatment_label = normalize_text(row["treatment_label"])
        control_label = normalize_text(row["control_label"])

        final_row = final_df[final_df["case_name_norm"] == case_name].copy()
        if final_row.empty:
            raise ValueError(f"No matching row in final_case_summary_table.csv for case_name='{case_name}'")
        final_row = final_row.iloc[0]

        m3 = to_float_or_none(final_row["effect_3m_pp"])
        m6 = to_float_or_none(final_row["effect_6m_pp"])
        m12 = to_float_or_none(final_row["effect_12m_pp"])

        if m3 is None or m6 is None or m12 is None:
            raise ValueError(
                f"Missing one or more summary effect values for live case_name='{case_name}'. "
                "effect_3m_pp, effect_6m_pp, and effect_12m_pp are all required."
            )

        peak_month_raw = pd.to_datetime(final_row["peak_post_gap_month"], errors="coerce")
        peak_month_fmt = (
            peak_month_raw.strftime("%Y-%m")
            if not pd.isna(peak_month_raw)
            else normalize_text(final_row["peak_post_gap_month"])
        )

        summary[case_id] = {
            "m3": m3,
            "m6": m6,
            "m12": m12,
            "sign": "positive" if m6 >= 0 else "negative",
            "pre_event_gap_std_pp": to_float_or_none(final_row["pre_event_gap_std_pp"]),
            "peak_post_gap_pp": to_float_or_none(final_row["peak_post_gap_pp"]),
            "peak_post_gap_month": peak_month_fmt,
            "placebo_n_3m": to_int_or_none(final_row["placebo_n_3m"]),
            "placebo_p_abs_3m": to_float_or_none(final_row["placebo_p_abs_3m"]),
            "placebo_n_6m": to_int_or_none(final_row["placebo_n_6m"]),
            "placebo_p_abs_6m": to_float_or_none(final_row["placebo_p_abs_6m"]),
        }

        cases.append(
            {
                "case_id": case_id,
                "event_id": event_id,
                "case_name": case_name,
                "source_type": source_type,
                "treatment_label": treatment_label,
                "control_label": control_label,
                "chart_file": f"./data/charts/{case_id}.json",
                "csv_file": f"./data/csv/{case_id}.csv",
                "caveat": normalize_text(row["caveat"]),
                "robustness_note": normalize_text(row["robustness_note"]),
                "method_note": normalize_text(row["method_note"]),
                "confidence_tier": normalize_text(row.get("confidence_tier", "")),
                "rationale_short": normalize_text(row.get("rationale_short", "")),
                "display_order": int(row["display_order"]),
                "primary_case_flag": normalize_text(row["primary_case_flag"]),
            }
        )

        case_panel = panel_df[
            (panel_df["case_name_norm"] == case_name)
            & (panel_df["source_type_norm"] == source_type)
        ].copy()

        if case_panel.empty:
            raise ValueError(
                f"No matching rows in product_case_studies_panel.csv for case_name='{case_name}', "
                f"source_type='{source_type}'"
            )

        treatment = (
            case_panel[case_panel["series_label_norm"] == treatment_label][["date", "rebased_100"]]
            .rename(columns={"rebased_100": "treatment"})
            .copy()
        )
        control = (
            case_panel[case_panel["series_label_norm"] == control_label][["date", "rebased_100"]]
            .rename(columns={"rebased_100": "control"})
            .copy()
        )

        if treatment.empty:
            raise ValueError(
                f"No treatment rows found for case_name='{case_name}', treatment_label='{treatment_label}'"
            )
        if control.empty:
            raise ValueError(
                f"No control rows found for case_name='{case_name}', control_label='{control_label}'"
            )

        merged = treatment.merge(control, on="date", how="inner").sort_values("date").copy()
        if merged.empty:
            raise ValueError(
                f"No overlapping treatment/control dates for case_name='{case_name}', "
                f"treatment_label='{treatment_label}', control_label='{control_label}'"
            )

        merged["relative_effect"] = merged["treatment"] - merged["control"]

        chart_payload = {
            "labels": merged["date"].dt.strftime("%Y-%m").tolist(),
            "treatment": [round(float(x), 3) for x in merged["treatment"].tolist()],
            "control": [round(float(x), 3) for x in merged["control"].tolist()],
            "relative_effect": [round(float(x), 3) for x in merged["relative_effect"].tolist()],
        }

        write_json(CHARTS_DIR / f"{case_id}.json", chart_payload)

        csv_out = merged.copy()
        csv_out["month"] = csv_out["date"].dt.strftime("%Y-%m")
        csv_out = csv_out[["month", "treatment", "control", "relative_effect"]]
        CSV_DIR.mkdir(parents=True, exist_ok=True)
        csv_out.to_csv(CSV_DIR / f"{case_id}.csv", index=False)

    write_json(OUT_DIR / "tariffs.json", tariffs)
    write_json(OUT_DIR / "cases.json", cases)
    write_json(OUT_DIR / "summary.json", summary)

    print("Wrote:")
    print(OUT_DIR / "tariffs.json")
    print(OUT_DIR / "cases.json")
    print(OUT_DIR / "summary.json")
    print(CHARTS_DIR)
    print(CSV_DIR)


if __name__ == "__main__":
    main()