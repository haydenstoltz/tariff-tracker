from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_TARGETS_FILE = ROOT / "data" / "metadata" / "world" / "pair_pull_targets.csv"
DEFAULT_CODE_MAP_FILE = ROOT / "data" / "metadata" / "world" / "wto_actor_code_map.csv"
DEFAULT_OUT_FILE = ROOT / "data" / "metadata" / "world" / "worldwide_import_batch_registry.csv"

REQUIRED_TARGET_COLS = [
    "year",
    "reporter_id",
    "reporter_iso3",
    "reporter_name",
    "enabled_flag",
]

REQUIRED_CODE_MAP_COLS = [
    "actor_id",
    "wto_partner_code",
    "canonical_name",
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


def require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")


def normalize_wto_code(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return text
    return f"{int(digits):03d}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build the canonical reporter-batch registry for bilateral imports acquisition "
            "from enabled worldwide pull targets."
        )
    )
    parser.add_argument("--targets-file", default="", help="Path to pair_pull_targets.csv")
    parser.add_argument("--code-map-file", default="", help="Path to wto_actor_code_map.csv")
    parser.add_argument("--out-file", default="", help="Path to worldwide_import_batch_registry.csv")
    args = parser.parse_args()

    targets_file = resolve_path(args.targets_file, DEFAULT_TARGETS_FILE)
    code_map_file = resolve_path(args.code_map_file, DEFAULT_CODE_MAP_FILE)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)

    targets = pd.read_csv(targets_file, dtype=str, keep_default_na=False)
    code_map = pd.read_csv(code_map_file, dtype=str, keep_default_na=False)

    for df in [targets, code_map]:
        for col in df.columns:
            df[col] = df[col].map(normalize_text)

    require_columns(targets, REQUIRED_TARGET_COLS, targets_file.name)
    require_columns(code_map, REQUIRED_CODE_MAP_COLS, code_map_file.name)

    enabled_targets = targets[targets["enabled_flag"].str.lower() == "yes"].copy()
    if enabled_targets.empty:
        raise ValueError("No enabled rows found in pair_pull_targets.csv")

    reporters = (
        enabled_targets[
            ["year", "reporter_id", "reporter_iso3", "reporter_name"]
        ]
        .drop_duplicates()
        .sort_values(["year", "reporter_id"], kind="stable")
        .reset_index(drop=True)
    )

    code_map = code_map.rename(columns={"actor_id": "reporter_id"})
    reporters = reporters.merge(
        code_map[["reporter_id", "wto_partner_code", "canonical_name"]],
        on="reporter_id",
        how="left",
        validate="many_to_one",
    )

    missing_codes = reporters[reporters["wto_partner_code"] == ""]
    if not missing_codes.empty:
        raise ValueError(
            "Reporter rows missing WTO actor codes:\n"
            + missing_codes[["year", "reporter_id", "reporter_name"]].to_string(index=False)
        )

    reporters["wto_reporter_code"] = reporters["wto_partner_code"].map(normalize_wto_code)
    reporters["expected_batch_filename"] = reporters.apply(
        lambda row: f"imports_{row['reporter_id']}_{row['year']}.csv",
        axis=1,
    )
    reporters["batch_id"] = reporters.apply(
        lambda row: f"{row['reporter_id']}_{row['year']}",
        axis=1,
    )
    reporters["source_family"] = "wto_ttd_bilateral_imports"
    reporters["acquisition_status"] = "pending"
    reporters["notes"] = "Acquire one reporter-batch bilateral imports CSV for this reporter/year"

    out = reporters[
        [
            "year",
            "batch_id",
            "reporter_id",
            "reporter_iso3",
            "reporter_name",
            "canonical_name",
            "wto_reporter_code",
            "expected_batch_filename",
            "source_family",
            "acquisition_status",
            "notes",
        ]
    ].copy()

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_file, index=False)

    print(f"Enabled reporter batches: {len(out)}")
    print(f"Wrote: {out_file}")


if __name__ == "__main__":
    main()