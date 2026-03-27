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


def to_float_or_none(x: object, digits: int = 3):
    if pd.isna(x) or str(x).strip() == "":
        return None
    return round(float(x), digits)


def to_int_or_none(x: object):
    if pd.isna(x) or str(x).strip() == "":
        return None
    return int(float(x))


def main() -> None:
    meta = pd.read_csv(META_FILE, keep_default_na=False)
    final_df = pd.read_csv(FINAL_SUMMARY_FILE, keep_default_na=False)
    panel_df = pd.read_csv(PANEL_FILE, keep_default_na=False)

    required_meta_cols = [
        "event_id",
        "event_title",
        "authority",
        "country",
        "announced_date",
        "effective_date",
        "status",
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

    meta["announced_date"] = pd.to_datetime(meta["announced_date"], errors="coerce")
    meta["effective_date"] = pd.to_datetime(meta["effective_date"], errors="coerce")
    if meta["announced_date"].isna().any():
        bad = meta.loc[meta["announced_date"].isna(), "case_id"].tolist()
        raise ValueError(f"Invalid announced_date in site_cases.csv for case_ids: {bad}")
    if meta["effective_date"].isna().any():
        bad = meta.loc[meta["effective_date"].isna(), "case_id"].tolist()
        raise ValueError(f"Invalid effective_date in site_cases.csv for case_ids: {bad}")

    panel_df["date"] = pd.to_datetime(panel_df["date"], errors="coerce")
    if panel_df["date"].isna().any():
        raise ValueError("Invalid date values found in product_case_studies_panel.csv")

    tariffs = []
    cases = []
    summary = {}

    tariff_rows = (
        meta[
            [
                "event_id",
                "event_title",
                "authority",
                "country",
                "announced_date",
                "effective_date",
                "status",
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
                "status": normalize_text(row["status"]),
            }
        )

    for _, row in meta.iterrows():
        event_id = normalize_text(row["event_id"])
        case_id = normalize_text(row["case_id"])
        case_name = normalize_text(row["case_name"])
        source_type = normalize_text(row["source_type"])
        treatment_label = normalize_text(row["treatment_label"])
        control_label = normalize_text(row["control_label"])
        caveat = normalize_text(row["caveat"])
        robustness_note = normalize_text(row["robustness_note"])
        method_note = normalize_text(row["method_note"])

        final_row = final_df[final_df["case_name"].astype(str).str.strip() == case_name].copy()
        if final_row.empty:
            raise ValueError(f"No matching row in final_case_summary_table.csv for case_name='{case_name}'")
        final_row = final_row.iloc[0]

        peak_month_raw = pd.to_datetime(final_row["peak_post_gap_month"], errors="coerce")
        peak_month_fmt = peak_month_raw.strftime("%Y-%m") if not pd.isna(peak_month_raw) else normalize_text(final_row["peak_post_gap_month"])

        m3 = to_float_or_none(final_row["effect_3m_pp"])
        m6 = to_float_or_none(final_row["effect_6m_pp"])
        m12 = to_float_or_none(final_row["effect_12m_pp"])

        summary[case_id] = {
            "m3": m3,
            "m6": m6,
            "m12": m12,
            "sign": "positive" if (m6 is not None and m6 >= 0) else "negative",
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
                "caveat": caveat,
                "robustness_note": robustness_note,
                "method_note": method_note,
            }
        )

        case_panel = panel_df[
            (panel_df["case_name"].astype(str).str.strip() == case_name)
            & (panel_df["source_type"].astype(str).str.strip() == source_type)
        ].copy()

        if case_panel.empty:
            raise ValueError(
                f"No matching rows in product_case_studies_panel.csv for case_name='{case_name}', source_type='{source_type}'"
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
                f"No treatment rows found for case_name='{case_name}', treatment_label='{treatment_label}'"
            )
        if control.empty:
            raise ValueError(
                f"No control rows found for case_name='{case_name}', control_label='{control_label}'"
            )

        merged = treatment.merge(control, on="date", how="inner").sort_values("date").copy()
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