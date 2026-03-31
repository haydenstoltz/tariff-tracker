from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_JSON = ROOT / "outputs" / "tariff_intel" / "raw_source_hits.json"
DEFAULT_OVERRIDES_CSV = ROOT / "data" / "metadata" / "tariff_feed_overrides.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "tariff_intel"
DEFAULT_SITE_DATA_DIR = ROOT / "site" / "data"

OUTPUT_FIELDS = [
    "feed_id",
    "normalized_title",
    "authority",
    "country_scope",
    "product_scope",
    "status_bucket",
    "incidence_priority",
    "event_type",
    "display_date",
    "latest_item_date",
    "source_family",
    "source_count",
    "source_labels",
    "primary_source_label",
    "primary_source_url",
    "matched_keywords",
    "raw_hit_count",
    "notes",
]

SOURCE_PRIORITY = {
    "White House": 4,
    "USTR": 3,
    "CBP": 2,
}

COUNTRY_HINTS = [
    "China",
    "Canada",
    "Mexico",
    "European Union",
    "United Kingdom",
    "Russia",
    "Japan",
    "India",
    "Brazil",
    "Global",
]

PRODUCT_HINTS = [
    "steel",
    "aluminum",
    "copper",
    "semiconductor",
    "semiconductors",
    "solar",
    "washers",
    "automobile",
    "autos",
    "auto parts",
    "lumber",
    "critical minerals",
    "shipbuilding",
]

DATE_PATTERNS = [
    ("%Y-%m-%d", re.compile(r"^\d{4}-\d{2}-\d{2}$")),
    ("%m/%d/%Y", re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")),
    ("%B %d, %Y", re.compile(
        r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}$",
        re.IGNORECASE,
    )),
]

STOPWORDS = {
    "the", "a", "an", "of", "for", "and", "to", "on", "in", "with", "by",
    "under", "from", "into", "at", "related", "regarding", "action", "actions",
    "presidential", "executive", "order", "orders", "proclamation", "proclamations",
    "update", "guidance", "notice", "press", "release", "releases", "trade", "imports",
}


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_text(value: object) -> str:
    return clean_text(value).lower()


def slugify(value: str) -> str:
    value = normalize_text(value)
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing raw source JSON: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_overrides(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing overrides CSV: {path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    required = {
        "match_pattern",
        "normalized_title",
        "authority",
        "country_scope",
        "product_scope",
        "status_bucket",
        "incidence_priority",
        "event_type",
        "notes_override",
    }
    for idx, row in enumerate(rows, start=1):
        missing = [field for field in required if field not in row]
        if missing:
            raise ValueError(f"Override row {idx} missing columns: {missing}")

    return rows


def parse_date_to_iso(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""

    for fmt, pattern in DATE_PATTERNS:
        if pattern.match(value):
            try:
                return datetime.strptime(value, fmt).date().isoformat()
            except ValueError:
                pass

    return ""


def display_date_or_blank(value: str) -> str:
    iso = parse_date_to_iso(value)
    return iso if iso else clean_text(value)


def keyword_list(value: str) -> list[str]:
    text = clean_text(value)
    if not text:
        return []
    return [part for part in text.split("|") if clean_text(part)]


def event_type_from_keywords(keywords: Iterable[str]) -> str:
    joined = " ".join(sorted(set(clean_text(k) for k in keywords if clean_text(k)))).lower()
    if "cbp" in joined or "customs" in joined or "de minimis" in joined:
        return "implementation_update"
    if "executive order" in joined or "proclamation" in joined:
        return "presidential_action"
    if any(token in joined for token in ["section 232", "section 301", "section 201", "section 122", "tariff", "import surcharge", "reciprocal tariff"]):
        return "tariff_action"
    return "trade_policy_item"


def infer_authority(title: str, keywords: Iterable[str], source_family: str) -> str:
    hay = f"{title} {' '.join(keywords)}".lower()
    if "section 232" in hay:
        return "Section 232"
    if "section 301" in hay:
        return "Section 301"
    if "section 201" in hay:
        return "Section 201"
    if "section 122" in hay:
        return "Section 122"
    if "reciprocal tariff" in hay:
        return "Reciprocal Tariffs"
    if "import surcharge" in hay:
        return "Import Surcharge"
    if "de minimis" in hay:
        return "Customs / De Minimis"
    if "executive order" in hay:
        return "Executive Order"
    if "proclamation" in hay:
        return "Presidential Proclamation"
    if source_family == "CBP":
        return "Customs / CBP"
    return "Trade Policy"


def infer_country_scope(title: str, snippet: str, authority: str) -> str:
    hay = f"{title} {snippet}"
    for hint in COUNTRY_HINTS:
        if hint.lower() in hay.lower():
            return hint
    if authority in {"Section 232", "Import Surcharge", "Reciprocal Tariffs", "Executive Order", "Presidential Proclamation"}:
        return "Global"
    return "Global"


def infer_product_scope(title: str, snippet: str) -> str:
    hay = f"{title} {snippet}".lower()
    hits = []
    for hint in PRODUCT_HINTS:
      if hint in hay:
        hits.append(hint)
    if not hits:
        return ""
    label = ", ".join(dict.fromkeys(hits))
    return label.title()


def infer_priority(authority: str, event_type: str, has_cbp: bool, has_wh: bool) -> str:
    if authority in {"Section 232", "Section 301", "Section 201", "Section 122", "Reciprocal Tariffs", "Import Surcharge"}:
        return "high"
    if event_type == "implementation_update" and has_cbp:
        return "high"
    if event_type == "presidential_action" and has_wh:
        return "medium"
    return "medium"


def infer_status_bucket(title: str, snippet: str) -> str:
    hay = f"{title} {snippet}".lower()
    if any(token in hay for token in ["terminated", "expired", "ended", "rescinded", "revoked"]):
        return "historical"
    if any(token in hay for token in ["effective", "takes effect", "goes into effect", "implementation", "guidance"]):
        return "current"
    return "current"


def token_signature(title: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", normalize_text(title))
    tokens = [t for t in tokens if t not in STOPWORDS and len(t) > 2]
    return " ".join(sorted(dict.fromkeys(tokens[:8])))


def override_match(overrides: list[dict[str, str]], title: str, keywords: Iterable[str], snippet: str) -> dict[str, str] | None:
    hay = f"{title} {' '.join(keywords)} {snippet}".lower()
    for row in overrides:
        pattern = normalize_text(row.get("match_pattern", ""))
        if pattern and pattern in hay:
            return row
    return None


def primary_source_row(rows: list[dict]) -> dict:
    def sort_key(row: dict):
        return (
            SOURCE_PRIORITY.get(clean_text(row.get("source_family")), 0),
            parse_date_to_iso(clean_text(row.get("item_date"))) or "",
            clean_text(row.get("item_title")),
        )
    return sorted(rows, key=sort_key, reverse=True)[0]


def grouped_feed_items(raw_rows: list[dict], overrides: list[dict[str, str]]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}

    for row in raw_rows:
        title = clean_text(row.get("item_title"))
        snippet = clean_text(row.get("snippet"))
        keywords = keyword_list(clean_text(row.get("keyword_matches")))
        override = override_match(overrides, title, keywords, snippet)

        if override:
            group_key = slugify(clean_text(override["normalized_title"]))
        else:
            authority = infer_authority(title, keywords, clean_text(row.get("source_family")))
            product_scope = infer_product_scope(title, snippet)
            signature = token_signature(title)
            group_key = slugify(f"{authority} {product_scope} {signature}".strip())

        grouped.setdefault(group_key, []).append(row)

    output: list[dict] = []

    for group_key, rows in grouped.items():
        top = primary_source_row(rows)
        top_title = clean_text(top.get("item_title"))
        top_snippet = clean_text(top.get("snippet"))
        merged_keywords = []
        for row in rows:
            merged_keywords.extend(keyword_list(clean_text(row.get("keyword_matches"))))
        merged_keywords = list(dict.fromkeys(merged_keywords))

        override = override_match(overrides, top_title, merged_keywords, top_snippet)

        if override:
            normalized_title = clean_text(override["normalized_title"])
            authority = clean_text(override["authority"])
            country_scope = clean_text(override["country_scope"])
            product_scope = clean_text(override["product_scope"])
            status_bucket = clean_text(override["status_bucket"])
            incidence_priority = clean_text(override["incidence_priority"])
            event_type = clean_text(override["event_type"])
            notes = clean_text(override["notes_override"]) or top_snippet
        else:
            authority = infer_authority(top_title, merged_keywords, clean_text(top.get("source_family")))
            country_scope = infer_country_scope(top_title, top_snippet, authority)
            product_scope = infer_product_scope(top_title, top_snippet)
            status_bucket = infer_status_bucket(top_title, top_snippet)
            has_cbp = any(clean_text(r.get("source_family")) == "CBP" for r in rows)
            has_wh = any(clean_text(r.get("source_family")) == "White House" for r in rows)
            event_type = event_type_from_keywords(merged_keywords)
            incidence_priority = infer_priority(authority, event_type, has_cbp, has_wh)
            normalized_title = top_title
            notes = top_snippet

        source_labels = list(dict.fromkeys(clean_text(r.get("source_label")) for r in rows if clean_text(r.get("source_label"))))
        latest_item_date = max((parse_date_to_iso(clean_text(r.get("item_date"))) for r in rows if clean_text(r.get("item_date"))), default="")
        display_date = latest_item_date or display_date_or_blank(clean_text(top.get("item_date")))

        output.append(
            {
                "feed_id": slugify(normalized_title) or group_key,
                "normalized_title": normalized_title,
                "authority": authority,
                "country_scope": country_scope,
                "product_scope": product_scope,
                "status_bucket": status_bucket,
                "incidence_priority": incidence_priority,
                "event_type": event_type,
                "display_date": display_date,
                "latest_item_date": latest_item_date,
                "source_family": clean_text(top.get("source_family")),
                "source_count": str(len({clean_text(r.get('item_url')) for r in rows if clean_text(r.get('item_url'))})),
                "source_labels": " | ".join(source_labels),
                "primary_source_label": clean_text(top.get("item_title")) or clean_text(top.get("source_label")),
                "primary_source_url": clean_text(top.get("item_url")),
                "matched_keywords": " | ".join(merged_keywords),
                "raw_hit_count": str(len(rows)),
                "notes": notes,
            }
        )

    def sort_key(row: dict):
        priority_rank = {"high": 3, "medium": 2, "low": 1}.get(normalize_text(row["incidence_priority"]), 0)
        status_rank = {
            "current": 3,
            "paused": 2,
            "other": 1,
            "historical": 0,
            "invalidated": -1,
        }.get(normalize_text(row["status_bucket"]), 0)
        return (
            -status_rank,
            -priority_rank,
            clean_text(row["latest_item_date"]),
            clean_text(row["normalized_title"]).lower(),
        )

    output.sort(key=sort_key)
    return output


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: clean_text(row.get(field, "")) for field in OUTPUT_FIELDS})


def write_json(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw-json",
        default="",
        help="Path to raw_source_hits.json. Default: outputs/tariff_intel/raw_source_hits.json",
    )
    parser.add_argument(
        "--overrides-csv",
        default="",
        help="Path to tariff_feed_overrides.csv. Default: data/metadata/tariff_feed_overrides.csv",
    )
    parser.add_argument(
        "--out-dir",
        default="",
        help="Output directory. Default: outputs/tariff_intel",
    )
    parser.add_argument(
        "--site-data-dir",
        default="",
        help="Site data directory. Default: site/data",
    )
    args = parser.parse_args()

    raw_json = Path(args.raw_json) if args.raw_json else DEFAULT_RAW_JSON
    if not raw_json.is_absolute():
        raw_json = ROOT / raw_json

    overrides_csv = Path(args.overrides_csv) if args.overrides_csv else DEFAULT_OVERRIDES_CSV
    if not overrides_csv.is_absolute():
        overrides_csv = ROOT / overrides_csv

    out_dir = Path(args.out_dir) if args.out_dir else DEFAULT_OUT_DIR
    if not out_dir.is_absolute():
        out_dir = ROOT / out_dir

    site_data_dir = Path(args.site_data_dir) if args.site_data_dir else DEFAULT_SITE_DATA_DIR
    if not site_data_dir.is_absolute():
        site_data_dir = ROOT / site_data_dir

    raw_rows = load_json(raw_json)
    if not isinstance(raw_rows, list):
        raise ValueError("raw_source_hits.json must contain a JSON list")

    overrides = load_overrides(overrides_csv)
    normalized_rows = grouped_feed_items(raw_rows, overrides)

    csv_path = out_dir / "normalized_feed_items.csv"
    json_path = out_dir / "normalized_feed_items.json"
    site_json_path = site_data_dir / "tariff_feed.json"

    write_csv(csv_path, normalized_rows)
    write_json(json_path, normalized_rows)
    write_json(site_json_path, normalized_rows)

    print(f"Raw rows loaded: {len(raw_rows)}")
    print(f"Normalized feed items: {len(normalized_rows)}")
    print(f"Wrote CSV: {csv_path}")
    print(f"Wrote JSON: {json_path}")
    print(f"Wrote site feed JSON: {site_json_path}")


if __name__ == "__main__":
    main()