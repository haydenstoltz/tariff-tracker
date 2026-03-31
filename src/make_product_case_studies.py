import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests
from pandas.tseries.offsets import MonthEnd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_META_FILE = ROOT / "data" / "metadata" / "product_case_studies.csv"
DEFAULT_PRICES_FILE = ROOT / "data" / "processed" / "prices_clean.csv"
DEFAULT_OUT_CHART_DIR = ROOT / "outputs" / "charts" / "case_studies"
DEFAULT_OUT_TABLE_DIR = ROOT / "outputs" / "tables"
BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


def resolve_path(path_str: str, default_path: Path) -> Path:
    if not path_str.strip():
        return default_path
    path = Path(path_str)
    if not path.is_absolute():
        path = ROOT / path
    return path


def sanitize_filename(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def to_month_end(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.to_period("M").to_timestamp("M")


def to_month_period(ts: pd.Timestamp) -> pd.Period:
    return pd.Timestamp(ts).to_period("M")


def month_match_mask(series: pd.Series, ts: pd.Timestamp) -> pd.Series:
    series = pd.to_datetime(series, errors="coerce")
    target = to_month_period(ts)
    return series.dt.to_period("M") == target


def fetch_bls_series(series_ids: list[str], start_year: int, end_year: int) -> pd.DataFrame:
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }

    r = requests.post(BLS_URL, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()

    if data.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS request failed: {data}")

    rows = []
    for series in data["Results"]["series"]:
        sid = series["seriesID"]
        for item in series["data"]:
            period = item["period"]
            if not period.startswith("M") or period == "M13":
                continue

            try:
                value = float(item["value"])
            except (TypeError, ValueError):
                continue

            date = pd.to_datetime(
                {
                    "year": [int(item["year"])],
                    "month": [int(period[1:])],
                    "day": [1],
                }
            )[0] + MonthEnd(0)

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


def load_local_prices(prices_file: Path, series_ids: list[str]) -> pd.DataFrame:
    if not prices_file.exists():
        return pd.DataFrame(columns=["series_id", "date", "level"])

    local = pd.read_csv(prices_file, parse_dates=["date"])
    local["series_id"] = local["series_id"].astype(str).str.strip()

    if "level" not in local.columns:
        if "value" in local.columns:
            local = local.rename(columns={"value": "level"})
        else:
            raise ValueError(
                f"{prices_file} must contain either a 'level' column or a 'value' column"
            )

    local = local[local["series_id"].isin(series_ids)][["series_id", "date", "level"]].copy()
    local = local.drop_duplicates().sort_values(["series_id", "date"]).reset_index(drop=True)
    return local


def get_price_panel(series_ids: list[str], start_year: int, end_year: int, prices_file: Path) -> pd.DataFrame:
    requested = sorted(set(series_ids))

    local = load_local_prices(prices_file, requested)
    local_found = set(local["series_id"].unique()) if not local.empty else set()
    missing_series = sorted(set(requested) - local_found)

    if local_found:
        print(f"Loaded {len(local_found)} series from local prices file: {prices_file}")
    else:
        print(f"No requested series found in local prices file: {prices_file}")

    fetched = pd.DataFrame(columns=["series_id", "date", "level"])
    if missing_series:
        print(f"Fetching {len(missing_series)} missing series from BLS: {missing_series}")
        fetched = fetch_bls_series(missing_series, start_year, end_year)

    prices = pd.concat([local, fetched], ignore_index=True)
    if prices.empty:
        raise RuntimeError("No price observations available from local prices file or BLS.")

    prices["series_id"] = prices["series_id"].astype(str).str.strip()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["level"] = pd.to_numeric(prices["level"], errors="coerce")
    prices = prices.dropna(subset=["series_id", "date", "level"]).copy()
    prices = prices.drop_duplicates(subset=["series_id", "date"], keep="last")
    prices = prices.sort_values(["series_id", "date"]).reset_index(drop=True)
    return prices


def build_case_summary(case_df: pd.DataFrame, case_name: str, event_date: pd.Timestamp) -> pd.DataFrame:
    horizons = [3, 6, 12]
    out_rows = []

    event_month = to_month_end(event_date)

    for series_id, g in case_df.groupby("series_id"):
        g = g.sort_values("date").reset_index(drop=True)
        label = g["series_label"].iloc[0]
        source_type = g["source_type"].iloc[0]
        base_date = to_month_end(g["base_date"].iloc[0])

        event_row = g.loc[month_match_mask(g["date"], event_month)]
        base_row = g.loc[month_match_mask(g["date"], base_date)]

        if event_row.empty or base_row.empty:
            continue

        event_level = float(event_row["rebased_100"].iloc[0])
        base_level = float(base_row["rebased_100"].iloc[0])

        for horizon in horizons:
            target_month = event_month + MonthEnd(horizon)
            target_row = g.loc[month_match_mask(g["date"], target_month)]
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

    for source_type, source_df in case_df.groupby("source_type"):
        source_df = source_df.copy()
        source_df["role"] = source_df["role"].fillna("").astype(str).str.lower()

        treatment_labels = source_df.loc[source_df["role"] == "treatment", "series_label"].drop_duplicates().tolist()
        control_labels = source_df.loc[source_df["role"] == "control", "series_label"].drop_duplicates().tolist()

        if len(treatment_labels) != 1 or len(control_labels) == 0:
            continue

        treatment_label = treatment_labels[0]

        treat = source_df.loc[source_df["series_label"] == treatment_label].copy()
        if treat.empty:
            continue

        treat_base_date = to_month_end(treat["base_date"].iloc[0])
        treat_base_row = treat.loc[month_match_mask(treat["date"], treat_base_date)]
        if treat_base_row.empty:
            continue

        treat_base = float(treat_base_row["rebased_100"].iloc[0])

        for control_label in control_labels:
            ctrl = source_df.loc[source_df["series_label"] == control_label].copy()
            if ctrl.empty:
                continue

            ctrl_base_date = to_month_end(ctrl["base_date"].iloc[0])
            ctrl_base_row = ctrl.loc[month_match_mask(ctrl["date"], ctrl_base_date)]
            if ctrl_base_row.empty:
                continue

            ctrl_base = float(ctrl_base_row["rebased_100"].iloc[0])

            for horizon in horizons:
                target_month = event_month + MonthEnd(horizon)

                treat_target_row = treat.loc[month_match_mask(treat["date"], target_month)]
                ctrl_target_row = ctrl.loc[month_match_mask(ctrl["date"], target_month)]

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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--meta-file",
        default="",
        help="Metadata CSV path. Default: data/metadata/product_case_studies.csv",
    )
    parser.add_argument(
        "--prices-file",
        default="",
        help="Local prices CSV path. Default: data/processed/prices_clean.csv",
    )
    parser.add_argument(
        "--out-chart-dir",
        default="",
        help="Chart output directory. Default: outputs/charts/case_studies",
    )
    parser.add_argument(
        "--out-table-dir",
        default="",
        help="Table output directory. Default: outputs/tables",
    )
    args = parser.parse_args()

    meta_file = resolve_path(args.meta_file, DEFAULT_META_FILE)
    prices_file = resolve_path(args.prices_file, DEFAULT_PRICES_FILE)
    out_chart_dir = resolve_path(args.out_chart_dir, DEFAULT_OUT_CHART_DIR)
    out_table_dir = resolve_path(args.out_table_dir, DEFAULT_OUT_TABLE_DIR)

    meta = pd.read_csv(
        meta_file,
        parse_dates=["event_date", "base_date", "window_start", "window_end"],
    )

    required_cols = {
        "case_id",
        "case_name",
        "status",
        "series_id",
        "series_label",
        "source_type",
        "role",
        "event_date",
        "base_date",
        "window_start",
        "window_end",
        "policy_date_type",
        "tariff_authority",
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

    prices = get_price_panel(series_ids, start_year, end_year, prices_file)

    requested = set(series_ids)
    returned = set(prices["series_id"].unique())
    missing_series = sorted(requested - returned)
    if missing_series:
        raise RuntimeError(f"Missing requested series in assembled case-study pull: {missing_series}")

    df = prices.merge(meta, on="series_id", how="inner")
    df = df[(df["date"] >= df["window_start"]) & (df["date"] <= df["window_end"])].copy()
    df = df.sort_values(["case_name", "case_id", "series_id", "date"]).reset_index(drop=True)

    rebased_parts = []
    for (case_id, series_id), g in df.groupby(["case_id", "series_id"]):
        base_date = to_month_end(g["base_date"].iloc[0])
        base_row = g.loc[month_match_mask(g["date"], base_date)].copy()

        if base_row.empty:
            available_months = (
                g["date"]
                .dt.to_period("M")
                .astype(str)
                .drop_duplicates()
                .sort_values()
                .tolist()
            )
            raise ValueError(
                f"Base month {base_date.strftime('%Y-%m')} not found for case_id='{case_id}', "
                f"series='{series_id}'. Available months: {available_months[:6]} ... {available_months[-6:]}"
            )

        base_level = float(base_row["level"].iloc[0])

        g = g.copy()
        g["rebased_100"] = (g["level"] / base_level) * 100.0
        g["mom_pct"] = g["level"].pct_change() * 100.0
        g["yoy_pct"] = g["level"].pct_change(12) * 100.0
        rebased_parts.append(g)

    df = pd.concat(rebased_parts, ignore_index=True)

    out_chart_dir.mkdir(parents=True, exist_ok=True)
    out_table_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(out_table_dir / "product_case_studies_panel.csv", index=False)

    summary_tables = []
    relative_summary_tables = []

    for case_name, case_df in df.groupby("case_name"):
        case_df = case_df.sort_values(["series_id", "date"]).copy()
        event_date = case_df["event_date"].iloc[0]

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
        fig.savefig(out_chart_dir / f"{sanitize_filename(case_name)}_rebased.png", dpi=200)
        plt.close(fig)

        chart_case_rows = case_df[
            case_df["role"].astype(str).str.lower().isin(["treatment", "control"])
        ].copy()
        if len(chart_case_rows["series_label"].unique()) >= 2:
            fig, ax = plt.subplots(figsize=(11, 6))
            for _, g in chart_case_rows.groupby("series_label"):
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

            ax.set_title(f"{case_name}: Treatment vs Control")
            ax.set_xlabel("Date")
            ax.set_ylabel("Index (base month = 100)")
            ax.legend()
            fig.tight_layout()
            fig.savefig(
                out_chart_dir / f"{sanitize_filename(case_name)}_treatment_vs_control.png",
                dpi=200,
            )
            plt.close(fig)

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
        fig.savefig(out_chart_dir / f"{sanitize_filename(case_name)}_yoy.png", dpi=200)
        plt.close(fig)

        summary = build_case_summary(case_df, case_name, event_date)
        if not summary.empty:
            summary_tables.append(summary)

        relative_summary = build_relative_case_summary(case_df, case_name, event_date)
        if not relative_summary.empty:
            relative_summary_tables.append(relative_summary)
            relative_summary.to_csv(
                out_table_dir / f"{sanitize_filename(case_name)}_relative_summary.csv",
                index=False,
            )
            print("\nRelative summary:")
            print(relative_summary.to_string(index=False))

    if summary_tables:
        summary_all = pd.concat(summary_tables, ignore_index=True)
        summary_all = summary_all.sort_values(
            ["case_name", "source_type", "series_label", "horizon_months"]
        )
        summary_all.to_csv(out_table_dir / "product_case_studies_summary.csv", index=False)
        print("\nMain summary:")
        print(summary_all.to_string(index=False))
    else:
        print("No main summary rows produced.")

    if relative_summary_tables:
        relative_all = pd.concat(relative_summary_tables, ignore_index=True)
        relative_all = relative_all.sort_values(
            ["case_name", "source_type", "treatment_series", "control_series", "horizon_months"]
        )
        relative_all.to_csv(out_table_dir / "product_case_studies_relative_summary_all.csv", index=False)

    print(f"\nUsed metadata: {meta_file}")
    print(f"Used prices file: {prices_file}")
    print(f"Saved case-study charts to {out_chart_dir}")
    print(f"Saved case-study tables to {out_table_dir}")


if __name__ == "__main__":
    main()