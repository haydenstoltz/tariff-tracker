from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
META_DIR = ROOT / "data" / "metadata"
SPEC_DIR = ROOT / "docs" / "case_specs"

SITE_CASES = META_DIR / "site_cases.csv"
EVENT_CASE_MAP = META_DIR / "event_case_map.csv"
CASE_STAGE_MAP = META_DIR / "case_stage_map.csv"
PRODUCT_CASE_STUDIES = META_DIR / "product_case_studies.csv"


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def one_row(df: pd.DataFrame, label: str, case_id: str) -> pd.Series:
    if df.empty:
        raise ValueError(f"No {label} row found for case_id='{case_id}'")
    if len(df) > 1:
        raise ValueError(f"Expected one {label} row for case_id='{case_id}', found {len(df)}")
    return df.iloc[0]


def infer_product_rows(product_df: pd.DataFrame, site_row: pd.Series) -> pd.DataFrame:
    case_id = normalize_text(site_row["case_id"])
    case_name = normalize_text(site_row["case_name"])
    source_type = normalize_text(site_row["source_type"])
    treatment_label = normalize_text(site_row["treatment_label"])
    control_label = normalize_text(site_row["control_label"])

    by_case_id = product_df[product_df["case_id"].astype(str).str.strip() == case_id].copy()
    if not by_case_id.empty:
        return by_case_id

    by_case_name = product_df[product_df["case_name"].astype(str).str.strip() == case_name].copy()

    if source_type:
        by_case_name = by_case_name[
            by_case_name["source_type"].astype(str).str.strip() == source_type
        ].copy()

    if treatment_label or control_label:
        by_case_name = by_case_name[
            by_case_name["series_label"].astype(str).str.strip().isin([treatment_label, control_label])
            | by_case_name["role"].astype(str).str.strip().eq("proxy")
        ].copy()

    if by_case_name.empty:
        raise ValueError(
            f"Could not infer product_case_studies rows for site case_id='{case_id}' case_name='{case_name}'"
        )

    return by_case_name


def validate_product_rows(product_rows: pd.DataFrame, site_row: pd.Series) -> str:
    treatment_label = normalize_text(site_row["treatment_label"])
    control_label = normalize_text(site_row["control_label"])
    source_type = normalize_text(site_row["source_type"])

    roles = set(product_rows["role"].astype(str).str.strip())
    labels = set(product_rows["series_label"].astype(str).str.strip())
    source_types = set(product_rows["source_type"].astype(str).str.strip())
    case_ids = set(product_rows["case_id"].astype(str).str.strip())

    if source_type and source_type not in source_types:
        raise ValueError(
            f"Expected site source_type='{source_type}' in product_case_studies rows, found {sorted(source_types)}"
        )

    if treatment_label and treatment_label not in labels:
        raise ValueError(f"Treatment label '{treatment_label}' not found in product rows")

    if control_label and control_label not in labels:
        raise ValueError(f"Control label '{control_label}' not found in product rows")

    if "treatment" not in roles:
        raise ValueError("No treatment row found in product rows")

    if "control" not in roles:
        raise ValueError("No control row found in product rows")

    if len(case_ids) != 1:
        raise ValueError(f"Expected one product_case_id, found {sorted(case_ids)}")

    return next(iter(case_ids))


def build_series_rows(product_rows: pd.DataFrame) -> list[dict[str, str]]:
    ordered = product_rows.copy()
    ordered["_role_order"] = ordered["role"].map({"treatment": 0, "control": 1}).fillna(2)
    ordered = ordered.sort_values(["_role_order", "source_type", "series_label"]).drop(columns="_role_order")

    series = []
    for _, row in ordered.iterrows():
        series.append(
            {
                "status": normalize_text(row["status"]),
                "series_id": normalize_text(row["series_id"]),
                "series_label": normalize_text(row["series_label"]),
                "source_type": normalize_text(row["source_type"]),
                "role": normalize_text(row["role"]),
                "event_date": normalize_text(row["event_date"]),
                "base_date": normalize_text(row["base_date"]),
                "window_start": normalize_text(row["window_start"]),
                "window_end": normalize_text(row["window_end"]),
                "policy_date_type": normalize_text(row["policy_date_type"]),
                "tariff_authority": normalize_text(row["tariff_authority"]),
                "notes": normalize_text(row["notes"]),
            }
        )
    return series


def build_spec(
    site_row: pd.Series,
    event_map_row: pd.Series,
    case_stage_row: pd.Series,
    product_rows: pd.DataFrame,
    product_case_id: str,
) -> dict:
    return {
        "site_event_id": normalize_text(site_row["event_id"]),
        "event_map_id": normalize_text(event_map_row["event_id"]),
        "event_title": normalize_text(site_row["event_title"]),
        "authority": normalize_text(site_row["authority"]),
        "country": normalize_text(site_row["country"]),
        "announced_date": normalize_text(site_row["announced_date"]),
        "effective_date": normalize_text(site_row["effective_date"]),
        "event_date_type": normalize_text(site_row["event_date_type"]),
        "event_source_label": normalize_text(site_row["event_source_label"]),
        "event_source_url": normalize_text(site_row["event_source_url"]),
        "event_status": normalize_text(site_row["event_status"]),
        "site_status": normalize_text(site_row["site_status"]),
        "case_id": normalize_text(site_row["case_id"]),
        "product_case_id": normalize_text(product_case_id),
        "case_name": normalize_text(site_row["case_name"]),
        "site_source_type": normalize_text(site_row["source_type"]),
        "treatment_label": normalize_text(site_row["treatment_label"]),
        "control_label": normalize_text(site_row["control_label"]),
        "confidence_tier": normalize_text(site_row["confidence_tier"]),
        "rationale_short": normalize_text(site_row["rationale_short"]),
        "caveat": normalize_text(site_row["caveat"]),
        "robustness_note": normalize_text(site_row["robustness_note"]),
        "method_note": normalize_text(site_row["method_note"]),
        "display_order": normalize_text(event_map_row["display_order"]),
        "primary_case_flag": normalize_text(event_map_row["primary_case_flag"]),
        "event_case_map_notes": normalize_text(event_map_row.get("notes", "")),
        "case_stage": normalize_text(case_stage_row["case_stage"]),
        "stage_order": normalize_text(case_stage_row["stage_order"]),
        "estimate_kind": normalize_text(case_stage_row["estimate_kind"]),
        "case_stage_notes": normalize_text(case_stage_row.get("notes", "")),
        "series": build_series_rows(product_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("case_id", help="site-layer case_id from site_cases.csv")
    parser.add_argument(
        "--out",
        default="",
        help="Optional explicit output path. Default: docs/case_specs/<case_id>.json",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing spec file if present.",
    )
    args = parser.parse_args()

    case_id = normalize_text(args.case_id)
    if not case_id:
        raise ValueError("case_id must be non-empty")

    site_df = read_csv(SITE_CASES)
    event_map_df = read_csv(EVENT_CASE_MAP)
    case_stage_df = read_csv(CASE_STAGE_MAP)
    product_df = read_csv(PRODUCT_CASE_STUDIES)

    site_row = one_row(
        site_df[site_df["case_id"].astype(str).str.strip() == case_id],
        "site_cases",
        case_id,
    )

    event_map_row = one_row(
        event_map_df[event_map_df["case_id"].astype(str).str.strip() == case_id],
        "event_case_map",
        case_id,
    )

    case_stage_row = one_row(
        case_stage_df[case_stage_df["case_id"].astype(str).str.strip() == case_id],
        "case_stage_map",
        case_id,
    )

    product_rows = infer_product_rows(product_df, site_row)
    product_case_id = validate_product_rows(product_rows, site_row)

    spec = build_spec(
        site_row=site_row,
        event_map_row=event_map_row,
        case_stage_row=case_stage_row,
        product_rows=product_rows,
        product_case_id=product_case_id,
    )

    if normalize_text(args.out):
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = ROOT / out_path
    else:
        out_path = SPEC_DIR / f"{case_id}.json"

    if out_path.exists() and not args.force:
        raise FileExistsError(
            f"Refusing to overwrite existing spec: {out_path}. Re-run with --force if needed."
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(spec, f, indent=2)

    print(f"Wrote spec: {out_path}")
    print(f"site case_id: {case_id}")
    print(f"product_case_id: {product_case_id}")
    print(f"series rows: {len(spec['series'])}")


if __name__ == "__main__":
    main()