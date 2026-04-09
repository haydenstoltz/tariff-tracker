from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_RAW_DIR = ROOT / "data" / "raw" / "worldwide" / "wto_ttd"
DEFAULT_CODE_MAP_FILE = ROOT / "data" / "metadata" / "world" / "wto_actor_code_map.csv"
DEFAULT_TARGETS_FILE = ROOT / "data" / "metadata" / "world" / "pair_pull_targets.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "worldwide"
DEFAULT_MANIFEST_FILE = ROOT / "data" / "raw" / "worldwide" / "wto_ttd" / "wto_ttd_ingest_manifest.json"

REQUIRED_IMPORTS_COLS = [
    "reporter_name",
    "reporter_code",
    "year",
    "classification",
    "classification_version",
    "product_code",
    "mtn_categories",
    "partner_code",
    "partner_name",
    "value",
]

REQUIRED_MFN_COLS = [
    "reporter_name",
    "reporter_code",
    "year",
    "classification",
    "classification_version",
    "duty_scheme_code",
    "duty_scheme_name",
    "product_code",
    "mtn_categories",
    "simple_average",
    "trade_weighted",
    "duty_free_share",
]

REQUIRED_CODE_MAP_COLS = [
    "actor_id",
    "wto_partner_code",
    "canonical_name",
]

REQUIRED_TARGET_COLS = [
    "year",
    "reporter_id",
    "partner_id",
    "enabled_flag",
]


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


def normalize_wto_code(value: object) -> str:
    text = normalize_text(value).upper()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return text
    return str(int(digits))


def require_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def list_year_files(raw_dir: Path, prefix: str) -> dict[str, Path]:
    out: dict[str, Path] = {}
    pattern = re.compile(rf"^{re.escape(prefix)}_(\d{{4}})\.csv$", re.IGNORECASE)

    for path in raw_dir.glob(f"{prefix}_*.csv"):
        match = pattern.match(path.name)
        if not match:
            continue
        out[match.group(1)] = path

    return out


def resolve_default_year_paths(raw_dir: Path, explicit_year: str = "") -> tuple[Path, Path, str]:
    imports_by_year = list_year_files(raw_dir, "imports_by_partner")
    mfn_by_year = list_year_files(raw_dir, "mfn_applied_total")

    common_years = sorted(set(imports_by_year) & set(mfn_by_year))
    if not common_years:
        raise FileNotFoundError(
            f"No common staged year files found in {raw_dir}. "
            "Expected imports_by_partner_<YEAR>.csv and mfn_applied_total_<YEAR>.csv"
        )

    if explicit_year:
        if explicit_year not in common_years:
            raise FileNotFoundError(
                f"Requested year {explicit_year} not available in staged raw directory. "
                f"Available years: {common_years}"
            )
        year = explicit_year
    else:
        year = max(common_years)

    return imports_by_year[year], mfn_by_year[year], year


def infer_single_year(df: pd.DataFrame, label: str) -> str:
    years = sorted({normalize_text(x) for x in df["year"].tolist() if normalize_text(x)})
    if not years:
        raise ValueError(f"No nonblank year values found in {label}")
    if len(years) != 1:
        raise ValueError(f"Expected exactly one year in {label}, found: {years}")
    year = years[0]
    if not year.isdigit() or len(year) != 4:
        raise ValueError(f"Invalid year value in {label}: {year}")
    return year


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--imports-file", default="", help="Path to WTO TTD imports export")
    parser.add_argument("--mfn-file", default="", help="Path to WTO TTD MFN applied duty export")
    parser.add_argument("--raw-dir", default="", help="Directory containing staged year-specific WTO raw files")
    parser.add_argument("--year", default="", help="Optional explicit staged raw year, e.g. 2023")
    parser.add_argument("--code-map-file", default="", help="Path to WTO actor code map")
    parser.add_argument("--targets-file", default="", help="Path to pair_pull_targets.csv")
    parser.add_argument("--out-dir", default="", help="Output directory")
    parser.add_argument("--manifest-file", default="", help="Path to ingest manifest JSON")
    parser.add_argument(
        "--allow-partial-imports",
        action="store_true",
        help="Allow missing target-pair import rows and keep only available import pairs",
    )
    args = parser.parse_args()

    raw_dir = resolve_path(args.raw_dir, DEFAULT_RAW_DIR)
    code_map_file = resolve_path(args.code_map_file, DEFAULT_CODE_MAP_FILE)
    targets_file = resolve_path(args.targets_file, DEFAULT_TARGETS_FILE)
    out_dir = resolve_path(args.out_dir, DEFAULT_OUT_DIR)
    manifest_file = resolve_path(args.manifest_file, DEFAULT_MANIFEST_FILE)

    if args.imports_file.strip() or args.mfn_file.strip():
        if not args.imports_file.strip() or not args.mfn_file.strip():
            raise ValueError("When using explicit raw files, both --imports-file and --mfn-file are required.")
        imports_file = resolve_path(args.imports_file, raw_dir / "imports_by_partner_latest.csv")
        mfn_file = resolve_path(args.mfn_file, raw_dir / "mfn_applied_total_latest.csv")
        selected_year = ""
    else:
        imports_file, mfn_file, selected_year = resolve_default_year_paths(raw_dir, explicit_year=args.year.strip())

    imports_df = pd.read_csv(imports_file, dtype=str, keep_default_na=False)
    mfn_df = pd.read_csv(mfn_file, dtype=str, keep_default_na=False)
    code_map_df = pd.read_csv(code_map_file, dtype=str, keep_default_na=False)
    targets_df = pd.read_csv(targets_file, dtype=str, keep_default_na=False)

    require_columns(imports_df, REQUIRED_IMPORTS_COLS, imports_file.name)
    require_columns(mfn_df, REQUIRED_MFN_COLS, mfn_file.name)
    require_columns(code_map_df, REQUIRED_CODE_MAP_COLS, code_map_file.name)
    require_columns(targets_df, REQUIRED_TARGET_COLS, targets_file.name)

    for df in [imports_df, mfn_df, code_map_df, targets_df]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    imports_year = infer_single_year(imports_df, imports_file.name)
    mfn_year = infer_single_year(mfn_df, mfn_file.name)

    if imports_year != mfn_year:
        raise ValueError(
            f"Imports year ({imports_year}) and MFN year ({mfn_year}) do not match."
        )

    if selected_year and selected_year != imports_year:
        raise ValueError(
            f"Requested staged year ({selected_year}) does not match raw file content year ({imports_year})."
        )

    selected_year = imports_year

    targets_df = targets_df[targets_df["enabled_flag"].str.lower() == "yes"].copy()
    if targets_df.empty:
        raise ValueError("No enabled target rows found in pair_pull_targets.csv")

    targets_df = targets_df[targets_df["year"] == selected_year].copy()
    if targets_df.empty:
        raise ValueError(
            f"No enabled target rows found in pair_pull_targets.csv for year {selected_year}"
        )

    code_map_df["wto_partner_code_norm"] = code_map_df["wto_partner_code"].map(normalize_wto_code)
    code_map_df = code_map_df.drop_duplicates(subset=["wto_partner_code_norm"]).copy()

    reporter_map = code_map_df.rename(
        columns={
            "actor_id": "reporter_id",
            "wto_partner_code": "reporter_code_map",
            "wto_partner_code_norm": "reporter_code_norm",
            "canonical_name": "reporter_canonical_name",
        }
    )
    partner_map = code_map_df.rename(
        columns={
            "actor_id": "partner_id",
            "wto_partner_code": "partner_code_map",
            "wto_partner_code_norm": "partner_code_norm",
            "canonical_name": "partner_canonical_name",
        }
    )

    target_reporter_codes = set(
        reporter_map.loc[
            reporter_map["reporter_id"].isin(targets_df["reporter_id"].unique()),
            "reporter_code_norm",
        ].tolist()
    )
    target_partner_codes = set(
        partner_map.loc[
            partner_map["partner_id"].isin(targets_df["partner_id"].unique()),
            "partner_code_norm",
        ].tolist()
    )

    imports_df["reporter_code_norm"] = imports_df["reporter_code"].map(normalize_wto_code)
    imports_df["partner_code_norm"] = imports_df["partner_code"].map(normalize_wto_code)

    imports_df = imports_df[
        (imports_df["year"] == selected_year)
        & (imports_df["product_code"].str.upper() == "TOTAL")
        & (imports_df["mtn_categories"].str.strip() == "All products")
        & (imports_df["reporter_code_norm"].isin(target_reporter_codes))
        & (imports_df["partner_code_norm"].isin(target_partner_codes))
    ].copy()

    imports_df = imports_df.merge(
        reporter_map[["reporter_id", "reporter_code_norm", "reporter_canonical_name"]],
        on="reporter_code_norm",
        how="left",
        validate="many_to_one",
    )
    imports_df = imports_df.merge(
        partner_map[["partner_id", "partner_code_norm", "partner_canonical_name"]],
        on="partner_code_norm",
        how="left",
        validate="many_to_one",
    )

    imports_df["reporter_id"] = imports_df["reporter_id"].fillna("").map(normalize_text)
    imports_df["partner_id"] = imports_df["partner_id"].fillna("").map(normalize_text)

    missing_reporters = sorted(imports_df.loc[imports_df["reporter_id"] == "", "reporter_code"].unique().tolist())
    missing_partners = sorted(imports_df.loc[imports_df["partner_id"] == "", "partner_code"].unique().tolist())
    if missing_reporters:
        raise ValueError(f"Unmapped reporter_code values in filtered imports export: {missing_reporters}")
    if missing_partners:
        raise ValueError(f"Unmapped partner_code values in filtered imports export: {missing_partners}")

    imports_df = imports_df.rename(
        columns={
            "reporter_name": "reporter_name_wto",
            "partner_name": "partner_name_wto",
            "value": "trade_value_usd",
        }
    )

    imports_df["pair_id"] = imports_df["reporter_id"] + "__" + imports_df["partner_id"]

    dupes = (
        imports_df.groupby(["year", "reporter_id", "partner_id"], dropna=False)
        .size()
        .reset_index(name="n")
        .query("n > 1")
        .copy()
    )
    if not dupes.empty:
        raise ValueError(
            "Filtered WTO imports export still has duplicate target pair rows:\n"
            + dupes.to_string(index=False)
        )

    imports_target_df = targets_df.merge(
        imports_df[
            [
                "year",
                "pair_id",
                "reporter_id",
                "partner_id",
                "reporter_name_wto",
                "partner_name_wto",
                "trade_value_usd",
                "classification",
                "classification_version",
                "product_code",
                "mtn_categories",
            ]
        ],
        on=["year", "reporter_id", "partner_id"],
        how="left",
        validate="one_to_one",
    )

    missing_import_pairs = imports_target_df.loc[
        imports_target_df["trade_value_usd"] == "",
        ["year", "reporter_id", "partner_id"],
    ].copy()
    missing_import_pair_count = int(len(missing_import_pairs))

    if not missing_import_pairs.empty:
        if args.allow_partial_imports:
            print(
                "Partial mode: keeping target rows with missing WTO imports values; "
                "downstream scoring may fill these from alternate sources."
            )
        else:
            raise ValueError(
                "Missing WTO imports rows for target pairs:\n"
                + missing_import_pairs.to_string(index=False)
            )
    else:
        missing_import_pair_count = 0

    mfn_df["reporter_code_norm"] = mfn_df["reporter_code"].map(normalize_wto_code)

    mfn_df = mfn_df[
        (mfn_df["year"] == selected_year)
        & (mfn_df["product_code"].str.upper() == "TOTAL")
        & (mfn_df["mtn_categories"].str.strip() == "All products")
    ].copy()

    mfn_df = mfn_df.merge(
        reporter_map[["reporter_id", "reporter_code_norm", "reporter_canonical_name"]],
        on="reporter_code_norm",
        how="left",
        validate="many_to_one",
    )

    mfn_df["reporter_id"] = mfn_df["reporter_id"].fillna("").map(normalize_text)

    missing_mfn_reporters = sorted(mfn_df.loc[mfn_df["reporter_id"] == "", "reporter_code"].unique().tolist())
    if missing_mfn_reporters:
        raise ValueError(f"Unmapped reporter_code values in MFN export: {missing_mfn_reporters}")

    mfn_df = mfn_df.rename(
        columns={
            "reporter_name": "reporter_name_wto",
            "simple_average": "simple_average_mfn_pct",
            "trade_weighted": "trade_weighted_mfn_pct",
            "duty_free_share": "duty_free_share_ratio",
        }
    )

    mfn_df = mfn_df[
        [
            "year",
            "reporter_id",
            "reporter_name_wto",
            "classification",
            "classification_version",
            "duty_scheme_code",
            "duty_scheme_name",
            "product_code",
            "mtn_categories",
            "simple_average_mfn_pct",
            "trade_weighted_mfn_pct",
            "duty_free_share_ratio",
        ]
    ].drop_duplicates(subset=["year", "reporter_id"])

    required_mfn_reporters = set(imports_target_df["reporter_id"].tolist())
    missing_mfn_target_reporters = sorted(required_mfn_reporters - set(mfn_df["reporter_id"].tolist()))
    missing_mfn_target_reporter_count = int(len(missing_mfn_target_reporters))

    if missing_mfn_target_reporters:
        if args.allow_partial_imports:
            print(
                "Partial mode: keeping reporters without MFN totals for selected year: "
                + ", ".join(missing_mfn_target_reporters)
            )
        else:
            raise ValueError(
                f"Missing reporter-level MFN rows for target reporters: {missing_mfn_target_reporters}"
            )
    else:
        missing_mfn_target_reporter_count = 0

    out_dir.mkdir(parents=True, exist_ok=True)

    imports_out = out_dir / "wto_imports_by_partner_targets.csv"
    mfn_out = out_dir / "wto_mfn_reporter_totals.csv"
    imports_json = out_dir / "wto_imports_by_partner_targets.json"
    mfn_json = out_dir / "wto_mfn_reporter_totals.json"

    imports_target_df.to_csv(imports_out, index=False)
    mfn_df.to_csv(mfn_out, index=False)
    write_json(imports_json, imports_target_df.to_dict(orient="records"))
    write_json(mfn_json, mfn_df.to_dict(orient="records"))

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "selected_year": selected_year,
        "imports_source_file": str(imports_file),
        "mfn_source_file": str(mfn_file),
        "imports_target_rows": int(len(imports_target_df)),
        "missing_import_pair_count": int(missing_import_pair_count),
        "reporter_mfn_rows": int(len(mfn_df)),
        "missing_mfn_target_reporter_count": int(missing_mfn_target_reporter_count),
        "target_reporter_count": int(targets_df["reporter_id"].nunique()),
        "target_pair_count": int(len(targets_df)),
        "allow_partial_imports": bool(args.allow_partial_imports),
    }
    write_json(manifest_file, manifest)

    print(f"Selected raw year: {selected_year}")
    print(f"Target import rows: {len(imports_target_df)}")
    print(f"Reporter MFN rows: {len(mfn_df)}")
    print(f"Wrote: {imports_out}")
    print(f"Wrote: {mfn_out}")
    print(f"Wrote: {imports_json}")
    print(f"Wrote: {mfn_json}")
    print(f"Wrote: {manifest_file}")


if __name__ == "__main__":
    main()
