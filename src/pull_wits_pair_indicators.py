from __future__ import annotations

import argparse
import json
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_TARGETS_FILE = ROOT / "data" / "metadata" / "world" / "pair_pull_targets.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"

INDICATORS: dict[str, dict[str, str]] = {
    "tradestats-trade": {
        "MPRT-TRD-VL": "Import Trade Value (US$ Thousand)",
    },
    "tradestats-tariff": {
        "AHS-WGHTD-AVRG": "Effectively Applied Weighted Average Tariff (%)",
        "AHS-SMPL-AVRG": "Effectively Applied Simple Average Tariff (%)",
        "TRF-NMBR-AGGRMNT": "Number of Tariff Agreements",
    },
}

REQUIRED_TARGET_COLS = ["year", "reporter_id", "reporter_name", "partner_id", "partner_name", "enabled_flag"]

TOTAL_ONLY_DEFAULT = True


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


def strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def normalize_key(key: str) -> str:
    return strip_ns(str(key)).strip().upper()


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def build_url(dataset: str, reporter_id: str, partner_id: str, year: str, indicator_code: str) -> str:
    # WITS pair endpoints require product/all for these indicators.
    return (
        f"https://wits.worldbank.org/API/V1/SDMX/V21/datasource/{dataset}"
        f"/reporter/{reporter_id.lower()}"
        f"/year/{year}"
        f"/partner/{partner_id.lower()}"
        f"/product/all"
        f"/indicator/{indicator_code}"
    )


def is_total_product_code(indicator_code: str, product_code: str) -> bool:
    indicator = normalize_text(indicator_code).upper()
    code = normalize_text(product_code).upper()

    if not code:
        return True

    if indicator == "TRF-NMBR-AGGRMNT":
        return code in {"999999", "TOTAL", "ALL"}

    return code in {"TOTAL", "999999", "ALL"}


def parse_sdmx_xml(xml_text: str, totals_only: bool) -> list[dict[str, str]]:
    root = ET.fromstring(xml_text)
    rows: list[dict[str, str]] = []

    for series in root.iter():
        if strip_ns(series.tag) != "Series":
            continue

        series_attrs = {normalize_key(k): normalize_text(v) for k, v in series.attrib.items()}
        indicator_code = normalize_text(series_attrs.get("INDICATOR", ""))
        product_code = normalize_text(series_attrs.get("PRODUCTCODE", series_attrs.get("PRODUCT", "")))

        if totals_only and not is_total_product_code(indicator_code, product_code):
            continue

        obs_nodes = [node for node in series if strip_ns(node.tag) == "Obs"]
        if obs_nodes:
            for obs in obs_nodes:
                row = dict(series_attrs)
                row.update({normalize_key(k): normalize_text(v) for k, v in obs.attrib.items()})
                rows.append(row)
        elif series_attrs:
            rows.append(series_attrs)

    return rows


def fetch_one(
    session: requests.Session,
    dataset: str,
    reporter_id: str,
    partner_id: str,
    year: str,
    indicator_code: str,
    indicator_label: str,
    pause_seconds: float,
    totals_only: bool,
) -> list[dict[str, str]]:
    url = build_url(dataset, reporter_id, partner_id, year, indicator_code)

    try:
        response = session.get(url, timeout=120)
    except Exception as exc:
        return [
            {
                "year": year,
                "requested_year": year,
                "reporter_id": reporter_id,
                "partner_id": partner_id,
                "requested_partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": "",
                "obs_value": "",
                "series_count": "0",
                "pull_status": "request_error",
                "http_status": "",
                "error_message": str(exc),
                "source_url": url,
                "product_code": "",
                "partner_code_raw": "",
                "reporter_code_raw": "",
            }
        ]

    if pause_seconds > 0:
        time.sleep(pause_seconds)

    if response.status_code != 200:
        return [
            {
                "year": year,
                "requested_year": year,
                "reporter_id": reporter_id,
                "partner_id": partner_id,
                "requested_partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": "",
                "obs_value": "",
                "series_count": "0",
                "pull_status": "http_error",
                "http_status": str(response.status_code),
                "error_message": normalize_text(response.text)[:500],
                "source_url": url,
                "product_code": "",
                "partner_code_raw": "",
                "reporter_code_raw": "",
            }
        ]

    try:
        parsed_rows = parse_sdmx_xml(response.text, totals_only=totals_only)
    except Exception as exc:
        return [
            {
                "year": year,
                "requested_year": year,
                "reporter_id": reporter_id,
                "partner_id": partner_id,
                "requested_partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": "",
                "obs_value": "",
                "series_count": "0",
                "pull_status": "parse_error",
                "http_status": str(response.status_code),
                "error_message": str(exc),
                "source_url": url,
                "product_code": "",
                "partner_code_raw": "",
                "reporter_code_raw": "",
            }
        ]

    if not parsed_rows:
        return [
            {
                "year": year,
                "requested_year": year,
                "reporter_id": reporter_id,
                "partner_id": partner_id,
                "requested_partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": "",
                "obs_value": "",
                "series_count": "0",
                "pull_status": "empty_response",
                "http_status": str(response.status_code),
                "error_message": "",
                "source_url": url,
                "product_code": "",
                "partner_code_raw": "",
                "reporter_code_raw": "",
            }
        ]

    out: list[dict[str, str]] = []
    series_count = str(len(parsed_rows))

    for row in parsed_rows:
        reporter_code_raw = normalize_text(row.get("REPORTER", reporter_id)).upper()
        partner_code_raw = normalize_text(row.get("PARTNER", partner_id)).upper()
        resolved_year = normalize_text(row.get("TIME_PERIOD", year))
        product_code = normalize_text(row.get("PRODUCTCODE", row.get("PRODUCT", "")))

        out.append(
            {
                "year": resolved_year or year,
                "requested_year": year,
                "reporter_id": reporter_code_raw or reporter_id,
                "partner_id": partner_code_raw or partner_id,
                "requested_partner_id": partner_id,
                "dataset": dataset,
                "indicator_code": indicator_code,
                "indicator_label": indicator_label,
                "time_period": resolved_year or year,
                "obs_value": normalize_text(row.get("OBS_VALUE", "")),
                "series_count": series_count,
                "pull_status": "success",
                "http_status": str(response.status_code),
                "error_message": "",
                "source_url": url,
                "product_code": product_code,
                "partner_code_raw": partner_code_raw,
                "reporter_code_raw": reporter_code_raw,
            }
        )

    return out


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def parse_indicator_filter(indicators_str: str) -> dict[str, dict[str, str]]:
    chosen = {normalize_text(x).upper() for x in indicators_str.split(",") if normalize_text(x)}
    if not chosen:
        return INDICATORS

    filtered: dict[str, dict[str, str]] = {}
    for dataset, indicator_map in INDICATORS.items():
        keep = {code: label for code, label in indicator_map.items() if code in chosen}
        if keep:
            filtered[dataset] = keep

    if not filtered:
        raise ValueError(f"--indicators did not match any known indicator codes: {sorted(chosen)}")

    return filtered


def build_totals(raw_df: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    out_cols = [
        "year",
        "reporter_id",
        "reporter_name",
        "partner_id",
        "partner_name",
        "wits_import_trade_value_usd_thousands",
        "wits_import_trade_value_usd",
        "wits_bilateral_weighted_tariff_pct",
        "wits_bilateral_simple_tariff_pct",
        "wits_tariff_agreement_count",
        "wits_rta_in_force",
    ]

    if raw_df.empty:
        return pd.DataFrame(columns=out_cols)

    success = raw_df[raw_df["pull_status"] == "success"].copy()
    if success.empty:
        return pd.DataFrame(columns=out_cols)

    for col in ["year", "reporter_id", "partner_id", "indicator_code", "obs_value"]:
        success[col] = success[col].map(normalize_text)

    success["year"] = success["year"].map(normalize_text)
    success["reporter_id"] = success["reporter_id"].str.upper()
    success["partner_id"] = success["partner_id"].str.upper()
    success["obs_value_num"] = pd.to_numeric(success["obs_value"], errors="coerce")
    success = success.dropna(subset=["obs_value_num"]).copy()
    if success.empty:
        return pd.DataFrame(columns=out_cols)

    target_pairs = targets[["year", "reporter_id", "partner_id"]].copy()
    target_pairs["year"] = target_pairs["year"].map(normalize_text)
    target_pairs["reporter_id"] = target_pairs["reporter_id"].str.upper()
    target_pairs["partner_id"] = target_pairs["partner_id"].str.upper()
    target_pairs = target_pairs.drop_duplicates()

    success = success.merge(
        target_pairs,
        on=["year", "reporter_id", "partner_id"],
        how="inner",
        validate="many_to_many",
    )
    if success.empty:
        return pd.DataFrame(columns=out_cols)

    success = success.sort_values(
        ["year", "reporter_id", "partner_id", "indicator_code", "time_period"],
        kind="stable",
    ).drop_duplicates(
        subset=["year", "reporter_id", "partner_id", "indicator_code"],
        keep="last",
    )

    totals = (
        success.pivot_table(
            index=["year", "reporter_id", "partner_id"],
            columns="indicator_code",
            values="obs_value_num",
            aggfunc="mean",
        )
        .reset_index()
    )
    totals.columns.name = None

    totals = totals.rename(
        columns={
            "MPRT-TRD-VL": "wits_import_trade_value_usd_thousands",
            "AHS-WGHTD-AVRG": "wits_bilateral_weighted_tariff_pct",
            "AHS-SMPL-AVRG": "wits_bilateral_simple_tariff_pct",
            "TRF-NMBR-AGGRMNT": "wits_tariff_agreement_count",
        }
    )

    for col in [
        "wits_import_trade_value_usd_thousands",
        "wits_bilateral_weighted_tariff_pct",
        "wits_bilateral_simple_tariff_pct",
        "wits_tariff_agreement_count",
    ]:
        if col not in totals.columns:
            totals[col] = pd.NA

    totals["wits_import_trade_value_usd"] = totals["wits_import_trade_value_usd_thousands"] * 1000.0
    totals["wits_tariff_agreement_count"] = totals["wits_tariff_agreement_count"].round(0)
    totals["wits_rta_in_force"] = totals["wits_tariff_agreement_count"].map(
        lambda x: "" if pd.isna(x) else ("yes" if float(x) > 0 else "no")
    )

    labels = (
        targets[["year", "reporter_id", "reporter_name", "partner_id", "partner_name"]]
        .drop_duplicates()
        .copy()
    )
    labels["reporter_id"] = labels["reporter_id"].map(normalize_text).str.upper()
    labels["partner_id"] = labels["partner_id"].map(normalize_text).str.upper()
    labels["year"] = labels["year"].map(normalize_text)

    totals = totals.merge(
        labels,
        on=["year", "reporter_id", "partner_id"],
        how="left",
        validate="one_to_one",
    )

    totals = totals[
        [
            "year",
            "reporter_id",
            "reporter_name",
            "partner_id",
            "partner_name",
            "wits_import_trade_value_usd_thousands",
            "wits_import_trade_value_usd",
            "wits_bilateral_weighted_tariff_pct",
            "wits_bilateral_simple_tariff_pct",
            "wits_tariff_agreement_count",
            "wits_rta_in_force",
        ]
    ].sort_values(["year", "reporter_id", "partner_id"], kind="stable")

    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Pull WITS partner indicators and emit pair-level totals for bilateral trade, "
            "tariff rates, and agreement counts across the active worldwide target matrix."
        )
    )
    parser.add_argument("--targets-file", default="", help="Path to pair_pull_targets.csv")
    parser.add_argument("--out-dir", default="", help="Output directory")
    parser.add_argument("--year", default="", help="Optional year filter, e.g. 2023")
    parser.add_argument("--reporters", default="", help="Optional comma-separated reporter_ids subset")
    parser.add_argument(
        "--partner-mode",
        choices=["all", "pair"],
        default="all",
        help="Use partner/all requests per reporter (recommended) or one request per pair target",
    )
    parser.add_argument("--indicators", default="", help="Optional comma-separated indicator-code subset")
    parser.add_argument("--pause-seconds", type=float, default=0.1, help="Pause between requests")
    parser.add_argument(
        "--include-all-products",
        action="store_true",
        help="Keep all product slices in raw output (default keeps total/all-products only).",
    )
    parser.add_argument("--write-json", action="store_true", help="Also emit JSON outputs")
    args = parser.parse_args()

    targets_file = resolve_path(args.targets_file, DEFAULT_TARGETS_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)

    targets = pd.read_csv(targets_file, dtype=str, keep_default_na=False)
    for col in targets.columns:
        targets[col] = targets[col].map(normalize_text)

    require_columns(targets, REQUIRED_TARGET_COLS, targets_file.name)
    targets = targets[targets["enabled_flag"].str.lower() == "yes"].copy()
    if targets.empty:
        raise ValueError("No enabled pair pull targets found")

    if normalize_text(args.year):
        targets = targets[targets["year"] == normalize_text(args.year)].copy()
        if targets.empty:
            raise ValueError(f"No enabled pair targets found for year={args.year}")

    reporters_filter = {
        normalize_text(x).upper()
        for x in normalize_text(args.reporters).split(",")
        if normalize_text(x)
    }
    if reporters_filter:
        targets = targets[targets["reporter_id"].str.upper().isin(reporters_filter)].copy()
        if targets.empty:
            raise ValueError("No targets left after applying --reporters filter")

    target_years = sorted({normalize_text(y) for y in targets["year"].tolist() if normalize_text(y)})
    if not target_years:
        raise ValueError("No year values found in filtered pair targets")

    indicator_catalog = parse_indicator_filter(args.indicators)
    totals_only = not args.include_all_products if TOTAL_ONLY_DEFAULT else False

    request_jobs: list[dict[str, str]] = []
    if args.partner_mode == "all":
        reporter_years = (
            targets[["year", "reporter_id"]]
            .drop_duplicates()
            .sort_values(["year", "reporter_id"], kind="stable")
        )
        for _, row in reporter_years.iterrows():
            for dataset, indicator_map in indicator_catalog.items():
                for indicator_code, indicator_label in indicator_map.items():
                    request_jobs.append(
                        {
                            "year": normalize_text(row["year"]),
                            "reporter_id": normalize_text(row["reporter_id"]).upper(),
                            "partner_id": "ALL",
                            "dataset": dataset,
                            "indicator_code": indicator_code,
                            "indicator_label": indicator_label,
                        }
                    )
    else:
        pair_rows = (
            targets[["year", "reporter_id", "partner_id"]]
            .drop_duplicates()
            .sort_values(["year", "reporter_id", "partner_id"], kind="stable")
        )
        for _, row in pair_rows.iterrows():
            for dataset, indicator_map in indicator_catalog.items():
                for indicator_code, indicator_label in indicator_map.items():
                    request_jobs.append(
                        {
                            "year": normalize_text(row["year"]),
                            "reporter_id": normalize_text(row["reporter_id"]).upper(),
                            "partner_id": normalize_text(row["partner_id"]).upper(),
                            "dataset": dataset,
                            "indicator_code": indicator_code,
                            "indicator_label": indicator_label,
                        }
                    )

    if not request_jobs:
        raise ValueError("No WITS request jobs were generated")

    rows: list[dict[str, str]] = []
    session = requests.Session()
    session.headers.update({"User-Agent": "tariff-tracker-worldwide-pull/2.0"})

    for i, job in enumerate(request_jobs, start=1):
        print(
            f"[{i}/{len(request_jobs)}] Pulling {job['indicator_code']} "
            f"{job['reporter_id']}->{job['partner_id']} year={job['year']}"
        )
        rows.extend(
            fetch_one(
                session=session,
                dataset=job["dataset"],
                reporter_id=job["reporter_id"],
                partner_id=job["partner_id"],
                year=job["year"],
                indicator_code=job["indicator_code"],
                indicator_label=job["indicator_label"],
                pause_seconds=args.pause_seconds,
                totals_only=totals_only,
            )
        )

    out_dir.mkdir(parents=True, exist_ok=True)

    raw_df = pd.DataFrame(rows)
    raw_csv_path = out_dir / "wits_pair_indicators_raw.csv"
    raw_df.to_csv(raw_csv_path, index=False)

    totals_df = build_totals(raw_df, targets=targets)
    totals_csv_path = out_dir / "wits_pair_indicators_totals.csv"
    totals_df.to_csv(totals_csv_path, index=False)

    if args.write_json:
        write_json(out_dir / "wits_pair_indicators_raw.json", raw_df.to_dict(orient="records"))
        write_json(out_dir / "wits_pair_indicators_totals.json", totals_df.to_dict(orient="records"))

    success_count = int((raw_df["pull_status"] == "success").sum()) if not raw_df.empty else 0
    failure_count = int((raw_df["pull_status"] != "success").sum()) if not raw_df.empty else 0

    manifest = {
        "request_count": len(request_jobs),
        "target_pair_count": int(len(targets)),
        "target_reporter_count": int(targets["reporter_id"].nunique()),
        "target_years": target_years,
        "partner_mode": args.partner_mode,
        "totals_only": bool(totals_only),
        "raw_row_count": int(len(raw_df)),
        "success_row_count": success_count,
        "failure_row_count": failure_count,
        "totals_row_count": int(len(totals_df)),
        "totals_with_weighted_tariff": int(totals_df["wits_bilateral_weighted_tariff_pct"].notna().sum())
        if not totals_df.empty
        else 0,
        "totals_with_agreement_count": int(totals_df["wits_tariff_agreement_count"].notna().sum())
        if not totals_df.empty
        else 0,
    }
    write_json(out_dir / "wits_pair_indicators_manifest.json", manifest)

    print(f"Requests issued: {len(request_jobs)}")
    print(f"Raw rows: {len(raw_df)}")
    print(f"Success rows: {success_count}")
    print(f"Failure rows: {failure_count}")
    print(f"Totals rows: {len(totals_df)}")
    print(f"Wrote: {raw_csv_path}")
    print(f"Wrote: {totals_csv_path}")
    print(f"Wrote: {out_dir / 'wits_pair_indicators_manifest.json'}")


if __name__ == "__main__":
    main()
