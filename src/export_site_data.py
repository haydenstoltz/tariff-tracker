from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

META_FILE = ROOT / "data" / "metadata" / "site_cases.csv"
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


def require_nonempty(df: pd.DataFrame, cols: list[str], label_col: str) -> None:
    problems = []
    for col in cols:
        bad = df[df[col].astype(str).str.strip() == ""]
        if not bad.empty:
            labels = bad[label_col].astype(str).tolist() if label_col in bad.columns else bad.index.astype(str).tolist()
            problems.append(f"{col}: {labels}")
    if problems:
        raise ValueError("Blank required values found -> " + " | ".join(problems))


def main() -> None:
    meta = normalize_object_columns(pd.read_csv(META_FILE, keep_default_na=False))
    final_df = normalize_object_columns(pd.read_csv(FINAL_SUMMARY_FILE, keep_default_na=False))
    panel_df = normalize_object_columns(pd.read_csv(PANEL_FILE, keep_default_na=False))

    required_meta_cols = [
        "event_id",
        "event_title",
        "authority",
        "country",
        "announced_date",
        "effective_date",
        "event_date_type",
        "event_source_label",
        "event_source_url",
        "event_status",
        "site_status",
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
    ]
    missing_meta = [c for c in required_meta_cols if c not in meta.columns]
    if missing_meta:
        raise ValueError(f"Missing metadata columns in site_cases.csv: {missing_meta}")

    required_final_cols = [
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
    ]
    missing_final = [c for c in required_final_cols if c not in final_df.columns]
    if missing_final:
        raise ValueError(f"Missing summary columns in final_case_summary_table.csv: {missing_final}")

    required_panel_cols = [
        "case_name",
        "source_type",
        "series_label",
        "date",
        "rebased_100",
    ]
    missing_panel = [c for c in required_panel_cols if c not in panel_df.columns]
    if missing_panel:
        raise ValueError(f"Missing panel columns in product_case_studies_panel.csv: {missing_panel}")

    if meta["case_id"].duplicated().any():
        dupes = meta.loc[meta["case_id"].duplicated(), "case_id"].tolist()
        raise ValueError(f"Duplicate case_id values in site_cases.csv: {dupes}")

    require_nonempty(
        meta,
        [
            "event_id",
            "event_title",
            "authority",
            "country",
            "announced_date",
            "effective_date",
            "event_date_type",
            "event_source_label",
            "event_source_url",
            "event_status",
            "site_status",
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
        ],
        "case_id",
    )

    meta["announced_date"] = pd.to_datetime(meta["announced_date"], errors="coerce")
    meta["effective_date"] = pd.to_datetime(meta["effective_date"], errors="coerce")

    if meta["announced_date"].isna().any():
        bad = meta.loc[meta["announced_date"].isna(), "case_id"].tolist()
        raise ValueError(f"Invalid announced_date in site_cases.csv for case_ids: {bad}")

    if meta["effective_date"].isna().any():
        bad = meta.loc[meta["effective_date"].isna(), "case_id"].tolist()
        raise ValueError(f"Invalid effective_date in site_cases.csv for case_ids: {bad}")

    if (meta["announced_date"] > meta["effective_date"]).any():
        bad = meta.loc[meta["announced_date"] > meta["effective_date"], "case_id"].tolist()
        raise ValueError(f"announced_date is after effective_date for case_ids: {bad}")

    meta["site_status"] = meta["site_status"].str.lower()
    allowed_site_status = {"live", "archived"}
    bad_site_status = meta.loc[~meta["site_status"].isin(allowed_site_status), "case_id"].tolist()
    if bad_site_status:
        raise ValueError(
            f"Invalid site_status values in site_cases.csv for case_ids: {bad_site_status}. "
            f"Allowed values: {sorted(allowed_site_status)}"
        )

    panel_df["date"] = pd.to_datetime(panel_df["date"], errors="coerce")
    if panel_df["date"].isna().any():
        raise ValueError("Invalid date values found in product_case_studies_panel.csv")

    live_meta = meta[meta["site_status"] == "live"].copy()
    if live_meta.empty:
        raise ValueError("No live cases found in site_cases.csv")

    tariffs = []
    cases = []
    summary = {}

    tariff_rows = (
        live_meta[
            [
                "event_id",
                "event_title",
                "authority",
                "country",
                "announced_date",
                "effective_date",
                "event_date_type",
                "event_source_label",
                "event_source_url",
                "event_status",
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
                "event_date_type": normalize_text(row["event_date_type"]),
                "event_source_label": normalize_text(row["event_source_label"]),
                "event_source_url": normalize_text(row["event_source_url"]),
                "status": normalize_text(row["event_status"]),
                "event_status": normalize_text(row["event_status"]),
            }
        )

    final_df["case_name_norm"] = final_df["case_name"].astype(str).str.strip()
    panel_df["case_name_norm"] = panel_df["case_name"].astype(str).str.strip()
    panel_df["source_type_norm"] = panel_df["source_type"].astype(str).str.strip()
    panel_df["series_label_norm"] = panel_df["series_label"].astype(str).str.strip()

    for _, row in live_meta.iterrows():
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
                "confidence_tier": normalize_text(row["confidence_tier"]),
                "rationale_short": normalize_text(row["rationale_short"]),
                "site_status": normalize_text(row["site_status"]),
                "event_date_type": normalize_text(row["event_date_type"]),
                "event_source_label": normalize_text(row["event_source_label"]),
                "event_source_url": normalize_text(row["event_source_url"]),
                "announced_date": row["announced_date"].strftime("%Y-%m-%d"),
                "effective_date": row["effective_date"].strftime("%Y-%m-%d"),
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

        event_period = row["effective_date"].to_period("M")
        if not merged["date"].dt.to_period("M").eq(event_period).any():
            raise ValueError(
                f"Effective date month {event_period} is not present in the merged chart window "
                f"for case_name='{case_name}'"
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