from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests
from pandas.tseries.offsets import MonthEnd

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_FILE = ROOT / "data" / "metadata" / "washer_producer_control_candidates.csv"
OUT_DIR = ROOT / "outputs" / "tables"
BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

EVENT_DATE = pd.Timestamp("2018-02-07")
BASE_DATE = pd.Timestamp("2018-01-31")
TARGETS = {
    3: 0.57,
    6: 1.16,
    12: 1.86,
}


def to_month_end(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.to_period("M").to_timestamp("M")


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


def pct_from_base(g: pd.DataFrame, base_date: pd.Timestamp, target_date: pd.Timestamp) -> float | None:
    base_row = g.loc[g["date"] == base_date]
    target_row = g.loc[g["date"] == target_date]

    if base_row.empty or target_row.empty:
        return None

    base_level = float(base_row["level"].iloc[0])
    target_level = float(target_row["level"].iloc[0])

    return (target_level / base_level - 1.0) * 100.0


def main() -> None:
    candidates = pd.read_csv(CANDIDATE_FILE)
    required_cols = {"series_id", "series_label", "role", "notes"}
    missing = required_cols - set(candidates.columns)
    if missing:
        raise ValueError(f"Missing columns in washer_producer_control_candidates.csv: {sorted(missing)}")

    candidates["series_id"] = candidates["series_id"].astype(str).str.strip()
    candidates["role"] = candidates["role"].astype(str).str.strip()

    treatment_rows = candidates[candidates["role"] == "treatment"].copy()
    if len(treatment_rows) != 1:
        raise ValueError("washer_producer_control_candidates.csv must have exactly one treatment row")

    treatment_id = treatment_rows["series_id"].iloc[0]
    treatment_label = treatment_rows["series_label"].iloc[0]

    control_rows = candidates[candidates["role"] == "control_candidate"].copy()
    if control_rows.empty:
        raise ValueError("No control_candidate rows found")

    all_series = candidates.loc[candidates["role"].isin(["treatment", "control_candidate"]), "series_id"].drop_duplicates().tolist()

    prices = fetch_bls_series(all_series, start_year=2017, end_year=2019)
    returned = set(prices["series_id"].unique())

    missing_series = sorted(set(all_series) - returned)
    if missing_series:
        print("Warning: some candidate series were not returned by BLS:")
        for sid in missing_series:
            print(f"  - {sid}")

    event_month = to_month_end(EVENT_DATE)
    base_month = to_month_end(BASE_DATE)

    treat = prices[prices["series_id"] == treatment_id].copy()
    if treat.empty:
        raise RuntimeError(f"Treatment series not returned: {treatment_id}")

    results = []

    for _, row in control_rows.iterrows():
        control_id = row["series_id"]
        control_label = row["series_label"]
        notes = row["notes"]

        ctrl = prices[prices["series_id"] == control_id].copy()
        if ctrl.empty:
            results.append(
                {
                    "control_series_id": control_id,
                    "control_series_label": control_label,
                    "status": "missing_from_bls_pull",
                    "score_abs_error_sum": None,
                    "rel_3m_pp": None,
                    "rel_6m_pp": None,
                    "rel_12m_pp": None,
                    "notes": notes,
                }
            )
            continue

        out = {
            "control_series_id": control_id,
            "control_series_label": control_label,
            "status": "ok",
            "notes": notes,
        }

        score = 0.0
        all_present = True

        for horizon, target_rel in TARGETS.items():
            target_month = event_month + MonthEnd(horizon)

            treat_pct = pct_from_base(treat, base_month, target_month)
            ctrl_pct = pct_from_base(ctrl, base_month, target_month)

            if treat_pct is None or ctrl_pct is None:
                out[f"rel_{horizon}m_pp"] = None
                out[f"target_{horizon}m_pp"] = target_rel
                out[f"abs_error_{horizon}m"] = None
                all_present = False
                continue

            rel = treat_pct - ctrl_pct
            err = abs(rel - target_rel)

            out[f"rel_{horizon}m_pp"] = round(rel, 3)
            out[f"target_{horizon}m_pp"] = target_rel
            out[f"abs_error_{horizon}m"] = round(err, 3)
            score += err

        out["score_abs_error_sum"] = round(score, 3) if all_present else None
        out["status"] = "ok" if all_present else "incomplete_window"
        results.append(out)

    result_df = pd.DataFrame(results)

    sort_key = result_df["score_abs_error_sum"].fillna(999999)
    result_df = result_df.assign(_sort=sort_key).sort_values(["_sort", "control_series_label"]).drop(columns="_sort")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    outfile = OUT_DIR / "washer_producer_control_sweep.csv"
    result_df.to_csv(outfile, index=False)

    print("\nWasher producer control sweep:")
    print(result_df.to_string(index=False))
    print(f"\nWrote {outfile}")


if __name__ == "__main__":
    main()