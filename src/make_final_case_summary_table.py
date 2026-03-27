from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
INFILE = ROOT / "outputs" / "tables" / "product_case_studies_relative_summary_all.csv"
OUTFILE = ROOT / "outputs" / "tables" / "final_case_summary_table.csv"


def value_for_horizon(g: pd.DataFrame, horizon: int):
    hit = g.loc[g["horizon_months"] == horizon, "relative_effect_pp"]
    if hit.empty:
        return None
    return round(float(hit.iloc[0]), 3)


def main() -> None:
    df = pd.read_csv(INFILE, parse_dates=["target_month"])
    required = {"case_name", "horizon_months", "target_month", "relative_effect_pp"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in relative summary file: {sorted(missing)}")

    rows = []
    for case_name, g in df.sort_values(["case_name", "horizon_months"]).groupby("case_name"):
        g = g.copy()

        peak_idx = g["relative_effect_pp"].abs().idxmax()
        peak_row = g.loc[peak_idx]

        rows.append(
            {
                "case_name": case_name,
                "effect_3m_pp": value_for_horizon(g, 3),
                "effect_6m_pp": value_for_horizon(g, 6),
                "effect_12m_pp": value_for_horizon(g, 12),
                "pre_event_gap_std_pp": None,
                "peak_post_gap_pp": round(float(peak_row["relative_effect_pp"]), 3),
                "peak_post_gap_month": pd.to_datetime(peak_row["target_month"]).strftime("%Y-%m"),
                "placebo_n_3m": None,
                "placebo_p_abs_3m": None,
                "placebo_n_6m": None,
                "placebo_p_abs_6m": None,
            }
        )

    out = pd.DataFrame(rows).sort_values("case_name")
    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTFILE, index=False)
    print(out.to_string(index=False))
    print(f"\nWrote {OUTFILE}")


if __name__ == "__main__":
    main()
