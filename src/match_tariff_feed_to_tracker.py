from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_FEED_JSON = ROOT / "outputs" / "tariff_intel" / "normalized_feed_items.json"
DEFAULT_TARIFFS_JSON = ROOT / "site" / "data" / "tariffs.json"
DEFAULT_CASES_JSON = ROOT / "site" / "data" / "cases.json"
DEFAULT_OVERRIDES_CSV = ROOT / "data" / "metadata" / "tariff_feed_event_overrides.csv"
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
    "primary_source_label",
    "primary_source_url",
    "source_family",
    "source_count",
    "source_labels",
    "matched_keywords",
    "raw_hit_count",
    "notes",
    "matched_event_id",
    "matched_event_title",
    "matched_case_id",
    "matched_case_name",
    "matched_live_case_count",
    "matched_score",
    "match_basis",
]

STOPWORDS = {
    "the", "a", "an", "of", "for", "and", "to", "on", "in", "with", "by", "under",
    "from", "into", "at", "related", "regarding", "action", "actions", "presidential",
    "executive", "order", "orders", "proclamation", "proclamations", "update", "guidance",
    "notice", "press", "release", "releases", "trade", "imports", "import", "tariff",
    "tariffs", "duties", "duty",
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


def load_json_list(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing JSON file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, list):
        raise ValueError(f"Expected JSON list in {path}")
    return payload


def load_overrides(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing overrides CSV: {path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    required = {
        "feed_id",
        "event_id",
        "case_id_override",
        "priority_override",
        "status_override",
        "notes_override",
    }
    if rows:
        missing = required - set(rows[0].keys())
        if missing:
            raise ValueError(f"Override CSV missing required columns: {sorted(missing)}")

    out: dict[str, dict[str, str]] = {}
    for row in rows:
        feed_id = clean_text(row.get("feed_id"))
        if not feed_id:
            continue
        out[feed_id] = {k: clean_text(v) for k, v in row.items()}
    return out


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


def parse_date(value: str) -> date | None:
    value = clean_text(value)
    if not value:
        return None

    candidates = [
        value,
        value[:10],
    ]
    for candidate in candidates:
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y"):
            try:
                return datetime.strptime(candidate, fmt).date()
            except ValueError:
                pass
    return None


def is_global_scope(value: str) -> bool:
    v = normalize_text(value)
    if not v:
        return True
    if v in {"global", "world", "worldwide"}:
        return True
    if "global" in v or "multi-country" in v or "multiple countries" in v:
        return True
    if "european union" in v:
        return True
    if "," in v or ";" in v or "/" in v or " and " in v:
        return True
    return False


def tokenize(text: str) -> set[str]:
    tokens = re.findall(r"[a-z0-9]+", normalize_text(text))
    return {t for t in tokens if len(t) > 2 and t not in STOPWORDS}


def token_overlap_score(feed_item: dict, event: dict) -> int:
    feed_text = " ".join(
        [
            clean_text(feed_item.get("normalized_title")),
            clean_text(feed_item.get("product_scope")),
            clean_text(feed_item.get("notes")),
            clean_text(feed_item.get("matched_keywords")),
        ]
    )
    event_text = " ".join(
        [
            clean_text(event.get("title")),
            clean_text(event.get("product_scope")),
            clean_text(event.get("notes")),
            clean_text(event.get("rate_summary")),
            clean_text(event.get("authority")),
        ]
    )

    overlap = tokenize(feed_text) & tokenize(event_text)
    return min(30, len(overlap) * 5)


def authority_score(feed_authority: str, event_authority: str) -> int:
    f = normalize_text(feed_authority)
    e = normalize_text(event_authority)

    if not f or not e:
        return 0
    if f == e:
        return 35

    for section in ("section 232", "section 301", "section 201", "section 122"):
        if section in f and section in e:
            return 30

    if "reciprocal" in f and "reciprocal" in e:
        return 30
    if "surcharge" in f and "surcharge" in e:
        return 28
    if "customs" in f and ("customs" in e or "cbp" in e):
        return 18

    shared = tokenize(f) & tokenize(e)
    if shared:
        return min(18, len(shared) * 6)

    return 0


def country_score(feed_country: str, event_country: str) -> int:
    f = clean_text(feed_country)
    e = clean_text(event_country)

    if not f and not e:
        return 0

    if is_global_scope(f) and is_global_scope(e):
        return 8

    nf = normalize_text(f)
    ne = normalize_text(e)

    if nf == ne and nf:
        return 12

    if nf and ne and (nf in ne or ne in nf):
        return 8

    return 0


def date_score(feed_item: dict, event: dict) -> int:
    feed_dates = [
        parse_date(clean_text(feed_item.get("latest_item_date"))),
        parse_date(clean_text(feed_item.get("display_date"))),
    ]
    feed_dates = [d for d in feed_dates if d is not None]
    event_dates = [
        parse_date(clean_text(event.get("effective_date"))),
        parse_date(clean_text(event.get("announced_date"))),
    ]
    event_dates = [d for d in event_dates if d is not None]

    if not feed_dates or not event_dates:
        return 0

    best_days = min(abs((fd - ed).days) for fd in feed_dates for ed in event_dates)

    if best_days == 0:
        return 20
    if best_days <= 7:
        return 18
    if best_days <= 30:
        return 15
    if best_days <= 90:
        return 10
    if best_days <= 180:
        return 6
    if best_days <= 365:
        return 3
    return 0


def status_score(feed_status: str, event_status: str) -> int:
    if normalize_text(feed_status) == normalize_text(event_status):
        return 5
    return 0


def lead_case_by_event(cases: list[dict]) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = {}
    for case in cases:
        event_id = clean_text(case.get("event_id"))
        if not event_id:
            continue
        grouped.setdefault(event_id, []).append(case)

    def confidence_rank(value: str) -> int:
        v = normalize_text(value)
        if v == "high":
            return 3
        if v == "medium":
            return 2
        if v == "low":
            return 1
        return 0

    def primary_flag(value: str) -> int:
        return 1 if normalize_text(value) in {"yes", "true", "1"} else 0

    out: dict[str, dict] = {}
    for event_id, event_cases in grouped.items():
        chosen = sorted(
            event_cases,
            key=lambda c: (
                -primary_flag(clean_text(c.get("primary_case_flag"))),
                -confidence_rank(clean_text(c.get("confidence_tier"))),
                int(clean_text(c.get("display_order")) or "9999"),
                clean_text(c.get("case_name")).lower(),
            ),
        )[0]
        out[event_id] = chosen
    return out


def match_one_feed_item(feed_item: dict, tariffs: list[dict]) -> tuple[dict | None, int]:
    best_event = None
    best_score = -1

    for event in tariffs:
        score = 0
        score += authority_score(feed_item.get("authority", ""), event.get("authority", ""))
        score += country_score(feed_item.get("country_scope", ""), event.get("country_scope", "") or event.get("country", ""))
        score += token_overlap_score(feed_item, event)
        score += date_score(feed_item, event)
        score += status_score(feed_item.get("status_bucket", ""), event.get("status_bucket", ""))

        if normalize_text(feed_item.get("normalized_title")) == normalize_text(event.get("title")):
            score += 40

        if score > best_score:
            best_event = event
            best_score = score

    if best_score < 35:
        return None, best_score

    return best_event, best_score


def build_output_rows(
    feed_items: list[dict],
    tariffs: list[dict],
    cases: list[dict],
    overrides: dict[str, dict[str, str]],
) -> list[dict]:
    tariff_by_event = {clean_text(t["event_id"]): t for t in tariffs}
    case_by_id = {clean_text(c["case_id"]): c for c in cases}
    lead_case_map = lead_case_by_event(cases)

    output: list[dict] = []
    override_matches = 0
    auto_matches = 0

    for item in feed_items:
        feed_id = clean_text(item.get("feed_id"))
        override = overrides.get(feed_id)
        matched_event = None
        matched_score = ""
        match_basis = ""
        matched_case = None

        if override and clean_text(override.get("event_id")):
            matched_event = tariff_by_event.get(clean_text(override["event_id"]))
            if matched_event is None:
                raise ValueError(
                    f"Override for feed_id '{feed_id}' references unknown event_id '{override['event_id']}'"
                )
            matched_score = "override"
            match_basis = "override"
            override_matches += 1

            case_id_override = clean_text(override.get("case_id_override"))
            if case_id_override:
                matched_case = case_by_id.get(case_id_override)
                if matched_case is None:
                    raise ValueError(
                        f"Override for feed_id '{feed_id}' references unknown case_id '{case_id_override}'"
                    )
            else:
                matched_case = lead_case_map.get(clean_text(matched_event["event_id"]))
        else:
            matched_event, score = match_one_feed_item(item, tariffs)
            if matched_event is not None:
                matched_score = str(score)
                match_basis = "scored"
                matched_case = lead_case_map.get(clean_text(matched_event["event_id"]))
                auto_matches += 1

        priority = clean_text(item.get("incidence_priority"))
        status_bucket = clean_text(item.get("status_bucket"))
        notes = clean_text(item.get("notes"))

        if override:
            if clean_text(override.get("priority_override")):
                priority = clean_text(override["priority_override"])
            if clean_text(override.get("status_override")):
                status_bucket = clean_text(override["status_override"])
            if clean_text(override.get("notes_override")):
                notes = clean_text(override["notes_override"])

        output.append(
            {
                "feed_id": feed_id,
                "normalized_title": clean_text(item.get("normalized_title")),
                "authority": clean_text(item.get("authority")),
                "country_scope": clean_text(item.get("country_scope")),
                "product_scope": clean_text(item.get("product_scope")),
                "status_bucket": status_bucket,
                "incidence_priority": priority,
                "event_type": clean_text(item.get("event_type")),
                "display_date": clean_text(item.get("display_date")),
                "latest_item_date": clean_text(item.get("latest_item_date")),
                "primary_source_label": clean_text(item.get("primary_source_label")),
                "primary_source_url": clean_text(item.get("primary_source_url")),
                "source_family": clean_text(item.get("source_family")),
                "source_count": clean_text(item.get("source_count")),
                "source_labels": clean_text(item.get("source_labels")),
                "matched_keywords": clean_text(item.get("matched_keywords")),
                "raw_hit_count": clean_text(item.get("raw_hit_count")),
                "notes": notes,
                "matched_event_id": clean_text(matched_event.get("event_id")) if matched_event else "",
                "matched_event_title": clean_text(matched_event.get("title")) if matched_event else "",
                "matched_case_id": clean_text(matched_case.get("case_id")) if matched_case else "",
                "matched_case_name": clean_text(matched_case.get("case_name")) if matched_case else "",
                "matched_live_case_count": clean_text(matched_event.get("live_case_count")) if matched_event else "",
                "matched_score": clean_text(matched_score),
                "match_basis": match_basis,
            }
        )

    print(f"Override matches: {override_matches}")
    print(f"Scored matches: {auto_matches}")
    print(f"Unmatched feed items: {sum(1 for row in output if not clean_text(row['matched_event_id']))}")

    output.sort(
        key=lambda row: (
            clean_text(row["matched_event_id"]) == "",
            clean_text(row["status_bucket"]) != "current",
            clean_text(row["display_date"]) == "",
            clean_text(row["display_date"]),
            clean_text(row["normalized_title"]).lower(),
        )
    )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--feed-json",
        default="",
        help="Path to normalized_feed_items.json. Default: outputs/tariff_intel/normalized_feed_items.json",
    )
    parser.add_argument(
        "--tariffs-json",
        default="",
        help="Path to site/data/tariffs.json. Default: site/data/tariffs.json",
    )
    parser.add_argument(
        "--cases-json",
        default="",
        help="Path to site/data/cases.json. Default: site/data/cases.json",
    )
    parser.add_argument(
        "--overrides-csv",
        default="",
        help="Path to tariff_feed_event_overrides.csv. Default: data/metadata/tariff_feed_event_overrides.csv",
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

    feed_json = Path(args.feed_json) if args.feed_json else DEFAULT_FEED_JSON
    if not feed_json.is_absolute():
        feed_json = ROOT / feed_json

    tariffs_json = Path(args.tariffs_json) if args.tariffs_json else DEFAULT_TARIFFS_JSON
    if not tariffs_json.is_absolute():
        tariffs_json = ROOT / tariffs_json

    cases_json = Path(args.cases_json) if args.cases_json else DEFAULT_CASES_JSON
    if not cases_json.is_absolute():
        cases_json = ROOT / cases_json

    overrides_csv = Path(args.overrides_csv) if args.overrides_csv else DEFAULT_OVERRIDES_CSV
    if not overrides_csv.is_absolute():
        overrides_csv = ROOT / overrides_csv

    out_dir = Path(args.out_dir) if args.out_dir else DEFAULT_OUT_DIR
    if not out_dir.is_absolute():
        out_dir = ROOT / out_dir

    site_data_dir = Path(args.site_data_dir) if args.site_data_dir else DEFAULT_SITE_DATA_DIR
    if not site_data_dir.is_absolute():
        site_data_dir = ROOT / site_data_dir

    feed_items = load_json_list(feed_json)
    tariffs = load_json_list(tariffs_json)
    cases = load_json_list(cases_json)
    overrides = load_overrides(overrides_csv)

    rows = build_output_rows(feed_items, tariffs, cases, overrides)

    csv_path = out_dir / "matched_feed_items.csv"
    json_path = out_dir / "matched_feed_items.json"
    site_json_path = site_data_dir / "tariff_feed.json"

    write_csv(csv_path, rows)
    write_json(json_path, rows)
    write_json(site_json_path, rows)

    print(f"Wrote CSV: {csv_path}")
    print(f"Wrote JSON: {json_path}")
    print(f"Wrote site feed JSON: {site_json_path}")


if __name__ == "__main__":
    main()