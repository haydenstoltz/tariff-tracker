import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests
from pandas.tseries.offsets import MonthEnd

import os

ROOT = Path(__file__).resolve().parents[1]
META_FILE = ROOT / "data" / "metadata" / "product_case_studies.csv"
OUT_CHART_DIR = ROOT / "outputs" / "charts" / "case_studies"
OUT_TABLE_DIR = ROOT / "outputs" / "tables"
BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


def sanitize_filename(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def to_month_end(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.to_period("M").to_timestamp("M")

def resolve_reference_row(
    g: pd.DataFrame,
    requested_date: pd.Timestamp,
    context: str,
) -> tuple[pd.Timestamp, pd.DataFrame]:
    requested_date = to_month_end(pd.Timestamp(requested_date))

    exact = g.loc[g["date"] == requested_date].copy()
    if not exact.empty:
        return requested_date, exact

    prior = g.loc[g["date"] < requested_date].sort_values("date").copy()
    if not prior.empty:
        resolved_date = pd.Timestamp(prior["date"].iloc[-1])
        print(
            f"[warn] {context}: requested {requested_date.date()} not found; "
            f"using nearest prior month {resolved_date.date()}"
        )
        resolved_row = g.loc[g["date"] == resolved_date].copy()
        return resolved_date, resolved_row

    later = g.loc[g["date"] > requested_date].sort_values("date").copy()
    if not later.empty:
        resolved_date = pd.Timestamp(later["date"].iloc[0])
        print(
            f"[warn] {context}: requested {requested_date.date()} not found; "
            f"using nearest later month {resolved_date.date()}"
        )
        resolved_row = g.loc[g["date"] == resolved_date].copy()
        return resolved_date, resolved_row

    raise ValueError(f"{context}: no usable observations found")


def fetch_bls_series(series_ids: list[str], start_year: int, end_year: int) -> pd.DataFrame:
    reg_key = os.getenv("BLS_API_KEY", "").strip()
    max_years_per_query = 20 if reg_key else 10

    if not series_ids:
        raise ValueError("No series IDs provided")

    rows = []

    for chunk_start in range(start_year, end_year + 1, max_years_per_query):
        chunk_end = min(chunk_start + max_years_per_query - 1, end_year)

        payload = {
            "seriesid": series_ids,
            "startyear": str(chunk_start),
            "endyear": str(chunk_end),
        }
        if reg_key:
            payload["registrationkey"] = reg_key

        print(f"[bls] requesting years {chunk_start}-{chunk_end} for {len(series_ids)} series")

        r = requests.post(BLS_URL, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()

        status = data.get("status")
        if status != "REQUEST_SUCCEEDED":
            raise RuntimeError(f"BLS request failed for {chunk_start}-{chunk_end}: {data}")

        series_list = data.get("Results", {}).get("series", [])
        for series in series_list:
            sid = series.get("seriesID", "")
            for item in series.get("data", []):
                period = str(item.get("period", ""))

                if not period.startswith("M") or period == "M13":
                    continue

                try:
                    value = float(item["value"])
                except (KeyError, TypeError, ValueError):
                    continue

                try:
                    year = int(item["year"])
                    month = int(period[1:])
                except (TypeError, ValueError):
                    continue

                date = pd.to_datetime(
                    {"year": [year], "month": [month], "day": [1]}
                )[0] + pd.offsets.MonthEnd(0)

                rows.append(
                    {
                        "series_id": sid,
                        "date": date,
                        "level": value,
                    }
                )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No usable monthly observations returned from BLS.")

    return df.drop_duplicates().sort_values(["series_id", "date"]).reset_index(drop=True)


def build_case_summary(case_df: pd.DataFrame, case_name: str, event_date: pd.Timestamp) -> pd.DataFrame:
    horizons = [3, 6, 12]
    out_rows = []

    event_month = to_month_end(event_date)

    for series_id, g in case_df.groupby("series_id"):
        g = g.sort_values("date").reset_index(drop=True)
        label = g["series_label"].iloc[0]
        source_type = g["source_type"].iloc[0]
        base_date, base_row = resolve_reference_row(
            g,
            g["base_date"].iloc[0],
            f"Summary base date for case='{case_name}', series='{series_id}'",
        )

        event_row = g.loc[g["date"] == event_month]

        if event_row.empty:
            continue

        event_level = float(event_row["rebased_100"].iloc[0])
        base_level = float(base_row["rebased_100"].iloc[0])

        for horizon in horizons:
            target_month = event_month + MonthEnd(horizon)
            target_row = g.loc[g["date"] == target_month]
            if target_row.empty:
                continue

            target_level = float(target_row["rebased_100"].iloc[0])

            out_rows.append(
                {
                    "case_name": case_name,
                    "series_id": series_id,
                    "series_label": label,
                    "source_type": source_type,
                    "base_month": base_date.date(),
                    "event_month": event_month.date(),
                    "horizon_months": horizon,
                    "target_month": target_month.date(),
                    "index_change_from_base": target_level - base_level,
                    "pct_change_from_base": (target_level / base_level - 1.0) * 100.0,
                    "index_change_from_event": target_level - event_level,
                    "pct_change_from_event": (target_level / event_level - 1.0) * 100.0,
                }
            )

    return pd.DataFrame(out_rows)


def build_relative_case_summary(case_df: pd.DataFrame, case_name: str, event_date: pd.Timestamp) -> pd.DataFrame:
    horizons = [3, 6, 12]
    out_rows = []
    event_month = to_month_end(event_date)

    # Use notes to identify controls. Anything with "control" in notes is treated as control.
    # Everything else within a source_type is treated as a treatment/proxy.
    for source_type, source_df in case_df.groupby("source_type"):
        source_df = source_df.copy()
        source_df["is_control"] = source_df["notes"].fillna("").str.lower().str.contains("control")

        treatment_labels = source_df.loc[~source_df["is_control"], "series_label"].drop_duplicates().tolist()
        control_labels = source_df.loc[source_df["is_control"], "series_label"].drop_duplicates().tolist()

        if len(treatment_labels) != 1 or len(control_labels) == 0:
            continue

        treatment_label = treatment_labels[0]

        treat = source_df.loc[source_df["series_label"] == treatment_label].copy()
        if treat.empty:
            continue

        treat_base_date, treat_base_row = resolve_reference_row(
            treat,
            treat["base_date"].iloc[0],
            f"Relative summary treatment base for case='{case_name}', source_type='{source_type}', treatment='{treatment_label}'",
        )

        treat_base = float(treat_base_row["rebased_100"].iloc[0])

        for control_label in control_labels:
            ctrl = source_df.loc[source_df["series_label"] == control_label].copy()
            if ctrl.empty:
                continue

            ctrl_base_date, ctrl_base_row = resolve_reference_row(
                ctrl,
                ctrl["base_date"].iloc[0],
                f"Relative summary control base for case='{case_name}', source_type='{source_type}', control='{control_label}'",
            )

            ctrl_base = float(ctrl_base_row["rebased_100"].iloc[0])

            for horizon in horizons:
                target_month = event_month + MonthEnd(horizon)

                treat_target_row = treat.loc[treat["date"] == target_month]
                ctrl_target_row = ctrl.loc[ctrl["date"] == target_month]

                if treat_target_row.empty or ctrl_target_row.empty:
                    continue

                treat_target = float(treat_target_row["rebased_100"].iloc[0])
                ctrl_target = float(ctrl_target_row["rebased_100"].iloc[0])

                treat_pct = (treat_target / treat_base - 1.0) * 100.0
                ctrl_pct = (ctrl_target / ctrl_base - 1.0) * 100.0

                out_rows.append(
                    {
                        "case_name": case_name,
                        "source_type": source_type,
                        "treatment_series": treatment_label,
                        "control_series": control_label,
                        "base_month": treat_base_date.date(),
                        "event_month": event_month.date(),
                        "horizon_months": horizon,
                        "target_month": target_month.date(),
                        "treatment_pct_change_from_base": treat_pct,
                        "control_pct_change_from_base": ctrl_pct,
                        "relative_effect_pp": treat_pct - ctrl_pct,
                    }
                )

    return pd.DataFrame(out_rows)


def main():
    meta = pd.read_csv(
        META_FILE,
        parse_dates=["event_date", "base_date", "window_start", "window_end"],
    )

    required_cols = {
        "case_name",
        "series_id",
        "series_label",
        "source_type",
        "event_date",
        "base_date",
        "window_start",
        "window_end",
        "notes",
    }
    missing_cols = required_cols - set(meta.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in metadata: {sorted(missing_cols)}")

    meta["series_id"] = meta["series_id"].astype(str).str.strip()
    meta = meta[meta["series_id"] != ""].copy()

    start_year = int(meta["window_start"].min().year)
    end_year = int(meta["window_end"].max().year)
    series_ids = meta["series_id"].drop_duplicates().tolist()

    prices = fetch_bls_series(series_ids, start_year, end_year)

    requested = set(series_ids)
    returned = set(prices["series_id"].unique())
    missing_series = sorted(requested - returned)
    if missing_series:
        raise RuntimeError(f"Missing requested BLS series in case study pull: {missing_series}")

    df = prices.merge(meta, on="series_id", how="inner")
    df = df[(df["date"] >= df["window_start"]) & (df["date"] <= df["window_end"])].copy()
    df = df.sort_values(["case_name", "series_id", "date"]).reset_index(drop=True)

    rebased_parts = []
    for (case_name, series_id), g in df.groupby(["case_name", "series_id"]):
        base_date, base_row = resolve_reference_row(
            g,
            g["base_date"].iloc[0],
            f"Base date for case='{case_name}', series='{series_id}'",
        )

        base_level = float(base_row["level"].iloc[0])

        g = g.copy()
        g["resolved_base_date"] = base_date
        g["rebased_100"] = (g["level"] / base_level) * 100.0
        g["mom_pct"] = g["level"].pct_change() * 100.0
        g["yoy_pct"] = g["level"].pct_change(12) * 100.0
        rebased_parts.append(g)

    df = pd.concat(rebased_parts, ignore_index=True)

    OUT_CHART_DIR.mkdir(parents=True, exist_ok=True)
    OUT_TABLE_DIR.mkdir(parents=True, exist_ok=True)

    df.to_csv(OUT_TABLE_DIR / "product_case_studies_panel.csv", index=False)

    summary_tables = []
    relative_summary_tables = []

    for case_name, case_df in df.groupby("case_name"):
        case_df = case_df.sort_values(["series_id", "date"]).copy()
        event_date = case_df["event_date"].iloc[0]

        # Chart 1: all series rebased
        fig, ax = plt.subplots(figsize=(11, 6))
        for _, g in case_df.groupby("series_label"):
            ax.plot(g["date"], g["rebased_100"], label=g["series_label"].iloc[0])

        ax.axvline(event_date, linestyle="--", linewidth=0.9)
        ymax = ax.get_ylim()[1]
        ax.text(
            event_date,
            ymax,
            f"{case_name} event",
            rotation=90,
            va="top",
            ha="right",
            fontsize=8,
        )

        ax.set_title(f"{case_name}: Rebased Price Paths")
        ax.set_xlabel("Date")
        ax.set_ylabel("Index (base month = 100)")
        ax.legend()
        fig.tight_layout()
        fig.savefig(OUT_CHART_DIR / f"{sanitize_filename(case_name)}_rebased.png", dpi=200)
        plt.close(fig)

        # Chart 2: CPI treatment vs control
        cpi_case = case_df[case_df["source_type"] == "CPI"].copy()
        if len(cpi_case["series_label"].unique()) >= 2:
            fig, ax = plt.subplots(figsize=(11, 6))
            for _, g in cpi_case.groupby("series_label"):
                ax.plot(g["date"], g["rebased_100"], label=g["series_label"].iloc[0])

            ax.axvline(event_date, linestyle="--", linewidth=0.9)
            ymax = ax.get_ylim()[1]
            ax.text(
                event_date,
                ymax,
                f"{case_name} event",
                rotation=90,
                va="top",
                ha="right",
                fontsize=8,
            )

            ax.set_title(f"{case_name}: CPI Treatment vs Control")
            ax.set_xlabel("Date")
            ax.set_ylabel("Index (base month = 100)")
            ax.legend()
            fig.tight_layout()
            fig.savefig(
                OUT_CHART_DIR / f"{sanitize_filename(case_name)}_cpi_treatment_vs_control.png",
                dpi=200,
            )
            plt.close(fig)

        # Chart 3: YoY
        fig, ax = plt.subplots(figsize=(11, 6))
        for _, g in case_df.groupby("series_label"):
            ax.plot(g["date"], g["yoy_pct"], label=g["series_label"].iloc[0])

        ax.axvline(event_date, linestyle="--", linewidth=0.9)
        ymax = ax.get_ylim()[1]
        ax.text(
            event_date,
            ymax,
            f"{case_name} event",
            rotation=90,
            va="top",
            ha="right",
            fontsize=8,
        )

        ax.set_title(f"{case_name}: Year-over-Year Change")
        ax.set_xlabel("Date")
        ax.set_ylabel("YoY percent")
        ax.legend()
        fig.tight_layout()
        fig.savefig(OUT_CHART_DIR / f"{sanitize_filename(case_name)}_yoy.png", dpi=200)
        plt.close(fig)

        # Per-case summaries
        summary = build_case_summary(case_df, case_name, event_date)
        if not summary.empty:
            summary_tables.append(summary)

        relative_summary = build_relative_case_summary(case_df, case_name, event_date)
        if not relative_summary.empty:
            relative_summary_tables.append(relative_summary)
            relative_summary.to_csv(
                OUT_TABLE_DIR / f"{sanitize_filename(case_name)}_relative_summary.csv",
                index=False,
            )
            print("\nRelative summary:")
            print(relative_summary.to_string(index=False))

    if summary_tables:
        summary_all = pd.concat(summary_tables, ignore_index=True)
        summary_all = summary_all.sort_values(
            ["case_name", "source_type", "series_label", "horizon_months"]
        )
        summary_all.to_csv(OUT_TABLE_DIR / "product_case_studies_summary.csv", index=False)
        print("\nMain summary:")
        print(summary_all.to_string(index=False))
    else:
        print("No main summary rows produced.")

    if relative_summary_tables:
        relative_all = pd.concat(relative_summary_tables, ignore_index=True)
        relative_all = relative_all.sort_values(
            ["case_name", "source_type", "treatment_series", "control_series", "horizon_months"]
        )
        relative_all.to_csv(OUT_TABLE_DIR / "product_case_studies_relative_summary_all.csv", index=False)

    print(f"\nSaved case-study charts to {OUT_CHART_DIR}")
    print(f"Saved case-study tables to {OUT_TABLE_DIR}")


if __name__ == "__main__":
    main()