from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_QUEUE_FILE = ROOT / "outputs" / "worldwide" / "worldwide_import_acquisition_queue.csv"
DEFAULT_OUT_FILE = ROOT / "outputs" / "worldwide" / "worldwide_import_download_checklist.md"

REQUIRED_QUEUE_COLS = [
    "priority_rank",
    "year",
    "batch_id",
    "reporter_id",
    "reporter_iso3",
    "reporter_name",
    "canonical_name",
    "wto_reporter_code",
    "source_portal",
    "source_section",
    "flow",
    "product_scope",
    "format",
    "expected_batch_filename",
    "expected_batch_path",
    "download_status",
    "source_family",
    "notes",
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


def build_markdown(queue: pd.DataFrame) -> str:
    lines: list[str] = []
    lines.append("# Worldwide import download checklist")
    lines.append("")
    lines.append(f"Missing reporter batches: {len(queue)}")
    lines.append("")
    lines.append("Use WTO Data portal > INDICATORS > Imports by origin > TOTAL / All products > CSV.")
    lines.append("")
    lines.append("| Priority | Reporter | Year | WTO code | Expected filename | Status |")
    lines.append("|---:|---|---:|---:|---|---|")

    for _, row in queue.iterrows():
        lines.append(
            f"| {normalize_text(row['priority_rank'])} | "
            f"{normalize_text(row['reporter_id'])} — {normalize_text(row['reporter_name'])} | "
            f"{normalize_text(row['year'])} | "
            f"{normalize_text(row['wto_reporter_code'])} | "
            f"`{normalize_text(row['expected_batch_filename'])}` | "
            f"{normalize_text(row['download_status']) or 'pending'} |"
        )

    lines.append("")

    for _, row in queue.iterrows():
        lines.append(f"## {normalize_text(row['priority_rank'])}. {normalize_text(row['reporter_id'])} — {normalize_text(row['reporter_name'])}")
        lines.append("")
        lines.append(f"- Year: {normalize_text(row['year'])}")
        lines.append(f"- WTO reporter code: {normalize_text(row['wto_reporter_code'])}")
        lines.append(f"- Source portal: {normalize_text(row['source_portal'])}")
        lines.append(f"- Source section: {normalize_text(row['source_section'])}")
        lines.append(f"- Flow: {normalize_text(row['flow'])}")
        lines.append(f"- Product scope: {normalize_text(row['product_scope'])}")
        lines.append(f"- Format: {normalize_text(row['format'])}")
        lines.append(f"- Expected filename: `{normalize_text(row['expected_batch_filename'])}`")
        lines.append(f"- Expected path: `{normalize_text(row['expected_batch_path'])}`")
        lines.append(f"- Current status: {normalize_text(row['download_status']) or 'pending'}")
        note = normalize_text(row["notes"])
        if note:
            lines.append(f"- Notes: {note}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a human-readable Markdown checklist from the worldwide import acquisition queue."
    )
    parser.add_argument("--queue-file", default="", help="Path to worldwide_import_acquisition_queue.csv")
    parser.add_argument("--out-file", default="", help="Output Markdown checklist path")
    args = parser.parse_args()

    queue_file = resolve_path(args.queue_file, DEFAULT_QUEUE_FILE)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)

    queue = pd.read_csv(queue_file, dtype=str, keep_default_na=False)
    for col in queue.columns:
        queue[col] = queue[col].map(normalize_text)

    require_columns(queue, REQUIRED_QUEUE_COLS, queue_file.name)

    queue = queue.sort_values(["priority_rank", "reporter_id"], kind="stable").reset_index(drop=True)
    markdown = build_markdown(queue)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(markdown, encoding="utf-8")

    print(f"Checklist rows: {len(queue)}")
    print(f"Wrote: {out_file}")


if __name__ == "__main__":
    main()