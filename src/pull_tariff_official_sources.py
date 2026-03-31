from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests

try:
    from bs4 import BeautifulSoup
    from bs4.element import Tag
except ImportError as exc:
    raise ImportError(
        "This script requires BeautifulSoup4. Install it only if this import fails in your environment."
    ) from exc


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCES_FILE = ROOT / "data" / "metadata" / "tariff_feed_sources.csv"
DEFAULT_OUT_DIR = ROOT / "outputs" / "tariff_intel"

OUTPUT_FIELDS = [
    "retrieved_at_utc",
    "source_id",
    "source_family",
    "source_label",
    "parser_kind",
    "listing_url",
    "item_title",
    "item_url",
    "item_date",
    "item_type",
    "snippet",
    "keyword_matches",
]

KEYWORDS = [
    "tariff",
    "tariffs",
    "duty",
    "duties",
    "import surcharge",
    "reciprocal tariff",
    "reciprocal tariffs",
    "section 232",
    "section 301",
    "section 201",
    "section 122",
    "trade deal",
    "de minimis",
    "proclamation",
    "executive order",
    "customs",
    "cbp",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)

MONTH_NAME_REGEX = (
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
)
DATE_PATTERNS = [
    re.compile(rf"{MONTH_NAME_REGEX}\s+\d{{1,2}},\s+\d{{4}}", re.IGNORECASE),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b"),
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"\s+", " ", value).strip()
    return text


def normalize_for_match(value: str) -> str:
    return clean_text(value).lower()


def keyword_matches(value: str) -> list[str]:
    haystack = normalize_for_match(value)
    if not haystack:
        return []
    matches = [kw for kw in KEYWORDS if kw in haystack]
    return sorted(set(matches), key=matches.index)


def parse_visible_date(text: str) -> str:
    text = clean_text(text)
    if not text:
        return ""

    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(0)
    return ""


def absolute_url(base_url: str, href: str | None) -> str:
    href = clean_text(href)
    if not href:
        return ""
    if href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
        return ""
    return urljoin(base_url, href)


def same_domain_or_relative(base_url: str, candidate_url: str) -> bool:
    if not candidate_url:
        return False
    base_netloc = urlparse(base_url).netloc.lower()
    candidate_netloc = urlparse(candidate_url).netloc.lower()
    return candidate_netloc == "" or candidate_netloc == base_netloc or candidate_netloc.endswith("." + base_netloc)


def find_main_container(soup: BeautifulSoup) -> Tag:
    selectors = [
        "main",
        "[role='main']",
        "article",
        ".main-content",
        ".region-content",
        ".page-content",
        ".l-content",
        ".content",
        "body",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if isinstance(node, Tag):
            return node
    body = soup.body
    if isinstance(body, Tag):
        return body
    return soup


def fetch_html(url: str, timeout: int) -> str:
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    return response.text


def dedupe_rows(rows: Iterable[dict]) -> list[dict]:
    seen: set[tuple[str, str]] = set()
    output: list[dict] = []

    for row in rows:
        key = (clean_text(row["source_id"]), clean_text(row["item_url"]))
        if not key[1]:
            continue
        if key in seen:
            continue
        seen.add(key)
        output.append(row)

    return output


def build_row(
    *,
    source: dict,
    listing_url: str,
    item_title: str,
    item_url: str,
    item_date: str,
    item_type: str,
    snippet: str,
) -> dict:
    title = clean_text(item_title)
    snippet_clean = clean_text(snippet)
    title_matches = keyword_matches(title)

    return {
        "retrieved_at_utc": utc_now_iso(),
        "source_id": source["source_id"],
        "source_family": source["source_family"],
        "source_label": source["source_label"],
        "parser_kind": source["parser_kind"],
        "listing_url": listing_url,
        "item_title": title,
        "item_url": clean_text(item_url),
        "item_date": clean_text(item_date),
        "item_type": clean_text(item_type),
        "snippet": snippet_clean,
        "keyword_matches": "|".join(title_matches),
    }


def parse_link_index(source: dict, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    container = find_main_container(soup)
    listing_url = source["url"]

    rows: list[dict] = []
    seen_urls: set[str] = set()

    for anchor in container.find_all("a", href=True):
        href = absolute_url(listing_url, anchor.get("href"))
        if not href or href in seen_urls:
            continue

        title = clean_text(anchor.get_text(" ", strip=True))
        if len(title) < 4:
            continue

        if not same_domain_or_relative(listing_url, href):
            # keep outbound WH/FR links from USTR pages too
            allowed_domains = {
                "www.whitehouse.gov",
                "whitehouse.gov",
                "www.federalregister.gov",
                "federalregister.gov",
                "ustr.gov",
                "www.ustr.gov",
                "www.cbp.gov",
                "cbp.gov",
            }
            if urlparse(href).netloc.lower() not in allowed_domains:
                continue

        parent_text = clean_text(anchor.parent.get_text(" ", strip=True)) if isinstance(anchor.parent, Tag) else ""
        item_date = parse_visible_date(parent_text)

        rows.append(
            build_row(
                source=source,
                listing_url=listing_url,
                item_title=title,
                item_url=href,
                item_date=item_date,
                item_type="link",
                snippet="",
            )
        )
        seen_urls.add(href)

    return rows


def candidate_card_nodes(container: Tag) -> list[Tag]:
    selectors = [
        "article",
        "li",
        ".views-row",
        ".node",
        ".card",
        ".usa-card",
        ".post",
        ".wp-block-post",
        ".collection-result",
        ".view-content > div",
    ]

    found: list[Tag] = []
    seen_ids: set[int] = set()

    for selector in selectors:
        for node in container.select(selector):
            if not isinstance(node, Tag):
                continue
            obj_id = id(node)
            if obj_id in seen_ids:
                continue
            seen_ids.add(obj_id)
            found.append(node)

    return found


def extract_card_link(node: Tag, listing_url: str) -> tuple[str, str]:
    for heading_tag in ["h1", "h2", "h3", "h4"]:
        heading = node.find(heading_tag)
        if isinstance(heading, Tag):
            link = heading.find("a", href=True)
            if isinstance(link, Tag):
                return clean_text(link.get_text(" ", strip=True)), absolute_url(listing_url, link.get("href"))
            heading_text = clean_text(heading.get_text(" ", strip=True))
            if heading_text:
                any_link = node.find("a", href=True)
                if isinstance(any_link, Tag):
                    return heading_text, absolute_url(listing_url, any_link.get("href"))

    first_link = node.find("a", href=True)
    if isinstance(first_link, Tag):
        return clean_text(first_link.get_text(" ", strip=True)), absolute_url(listing_url, first_link.get("href"))

    return "", ""


def extract_card_snippet(node: Tag, title: str) -> str:
    for paragraph in node.find_all(["p", "div"]):
        text = clean_text(paragraph.get_text(" ", strip=True))
        if not text:
            continue
        if text == title:
            continue
        if len(text) < 20:
            continue
        return text[:400]
    return ""


def extract_card_date(node: Tag) -> str:
    time_tag = node.find("time")
    if isinstance(time_tag, Tag):
        datetime_attr = clean_text(time_tag.get("datetime"))
        if datetime_attr:
            return datetime_attr[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", datetime_attr) else datetime_attr
        time_text = clean_text(time_tag.get_text(" ", strip=True))
        if time_text:
            return time_text

    text = clean_text(node.get_text(" ", strip=True))
    return parse_visible_date(text)


def parse_card_list(source: dict, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    container = find_main_container(soup)
    listing_url = source["url"]

    rows: list[dict] = []
    seen_urls: set[str] = set()

    for node in candidate_card_nodes(container):
        title, item_url = extract_card_link(node, listing_url)
        if len(title) < 4 or not item_url or item_url in seen_urls:
            continue

        snippet = extract_card_snippet(node, title)
        item_date = extract_card_date(node)

        rows.append(
            build_row(
                source=source,
                listing_url=listing_url,
                item_title=title,
                item_url=item_url,
                item_date=item_date,
                item_type="card",
                snippet=snippet,
            )
        )
        seen_urls.add(item_url)

    return rows


def parse_source(source: dict, html: str) -> list[dict]:
    parser_kind = source["parser_kind"]
    if parser_kind == "link_index":
        return parse_link_index(source, html)
    if parser_kind == "card_list":
        return parse_card_list(source, html)
    raise ValueError(f"Unsupported parser_kind: {parser_kind}")


def read_sources(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing sources file: {path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    required = {"source_id", "source_family", "source_label", "url", "parser_kind", "active"}
    for idx, row in enumerate(rows, start=1):
        missing = [field for field in required if not clean_text(row.get(field, ""))]
        if missing:
            raise ValueError(f"tariff_feed_sources.csv row {idx} missing required values: {missing}")

    return [row for row in rows if clean_text(row.get("active", "")).lower() == "yes"]


def write_csv_file(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in OUTPUT_FIELDS})


def write_json_file(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sources-file",
        default="",
        help="Path to tariff_feed_sources.csv. Default: data/metadata/tariff_feed_sources.csv",
    )
    parser.add_argument(
        "--out-dir",
        default="",
        help="Output directory. Default: outputs/tariff_intel",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds. Default: 30",
    )
    args = parser.parse_args()

    sources_file = Path(args.sources_file) if args.sources_file else DEFAULT_SOURCES_FILE
    if not sources_file.is_absolute():
        sources_file = ROOT / sources_file

    out_dir = Path(args.out_dir) if args.out_dir else DEFAULT_OUT_DIR
    if not out_dir.is_absolute():
        out_dir = ROOT / out_dir

    sources = read_sources(sources_file)

    all_rows: list[dict] = []

    for source in sources:
        html = fetch_html(source["url"], timeout=args.timeout)
        candidates = parse_source(source, html)
        kept = [row for row in candidates if clean_text(row["keyword_matches"])]

        print(
            f"[{source['source_id']}] fetched {len(candidates)} candidate items, "
            f"kept {len(kept)} tariff-related hits"
        )

        all_rows.extend(kept)

    deduped = dedupe_rows(all_rows)
    deduped.sort(
        key=lambda row: (
            clean_text(row["source_family"]),
            clean_text(row["source_id"]),
            clean_text(row["item_date"]),
            clean_text(row["item_title"]).lower(),
        ),
        reverse=False,
    )

    csv_path = out_dir / "raw_source_hits.csv"
    json_path = out_dir / "raw_source_hits.json"

    write_csv_file(csv_path, deduped)
    write_json_file(json_path, deduped)

    print(f"\nWrote CSV: {csv_path}")
    print(f"Wrote JSON: {json_path}")
    print(f"Final kept rows after dedupe: {len(deduped)}")


if __name__ == "__main__":
    main()