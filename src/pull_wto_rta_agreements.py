from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]

DEFAULT_EXPORT_URL = "https://rtais.wto.org/UI/ExportAllRTAList.aspx"
DEFAULT_HOME_URL = "https://rtais.wto.org/UI/PublicMaintainRTAHome.aspx"
DEFAULT_DOWNLOAD_FILE = ROOT / "outputs" / "worldwide" / "rta_list_latest.xlsx"
DEFAULT_ACTOR_MAP_FILE = ROOT / "data" / "metadata" / "world" / "wto_actor_code_map.csv"
DEFAULT_OUT_FILE = ROOT / "site" / "data" / "world_trade_agreements.json"

XLSX_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

SIGNATORY_GROUP_MEMBERS: dict[str, list[str]] = {
    "asean free trade area afta": ["BRN", "KHM", "IDN", "LAO", "MYS", "MMR", "PHL", "SGP", "THA", "VNM"],
    "european free trade association efta": ["CHE", "ISL", "LIE", "NOR"],
    "european union": ["EU"],
}

SIGNATORY_ALIAS_IDS: dict[str, str] = {
    "korea republic of": "KOR",
    "lao peoples democratic republic": "LAO",
    "moldova republic of": "MDA",
    "venezuela bolivarian republic of": "VEN",
    "bolivia plurinational state of": "BOL",
    "russian federation": "RUS",
    "syrian arab republic": "SYR",
    "viet nam": "VNM",
    "taipei chinese": "TWN",
    "united states of america": "USA",
    "congo democratic republic of the": "COD",
}


def resolve_path(path_str: str, default: Path) -> Path:
    if not path_str.strip():
        return default
    candidate = Path(path_str)
    if not candidate.is_absolute():
        candidate = ROOT / candidate
    return candidate


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_name(value: object) -> str:
    text = normalize_text(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"[\(\)\[\],.'’/-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_last_updated_iso(home_html: str) -> str:
    match = re.search(r"Last updated on:\s*([^<.]+)", home_html, flags=re.IGNORECASE)
    if not match:
        return datetime.now(timezone.utc).date().isoformat()

    raw = normalize_text(match.group(1)).replace("\xa0", " ")
    raw = re.sub(r"\s+", " ", raw).strip().strip(".")
    for fmt in ("%A, %B %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return datetime.now(timezone.utc).date().isoformat()


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": "tariff-tracker-wto-rta-pull/1.0"})
    with urlopen(request, timeout=90) as response:  # nosec B310 (trusted URL + explicit timeout)
        return response.read().decode("utf-8", errors="replace")


def fetch_bytes(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "tariff-tracker-wto-rta-pull/1.0"})
    with urlopen(request, timeout=120) as response:  # nosec B310 (trusted URL + explicit timeout)
        return response.read()


def excel_col_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch.upper()) - 64)
    return idx - 1


def parse_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out: list[str] = []
    for si in root.findall(f"{XLSX_NS}si"):
        text = "".join(node.text or "" for node in si.iter(f"{XLSX_NS}t"))
        out.append(text)
    return out


def parse_sheet_rows(zf: zipfile.ZipFile, sheet_name: str, shared: list[str]) -> list[dict[str, str]]:
    root = ET.fromstring(zf.read(sheet_name))
    row_nodes = root.findall(f".//{XLSX_NS}sheetData/{XLSX_NS}row")
    if not row_nodes:
        return []

    rows_by_index: list[dict[int, str]] = []
    for row_node in row_nodes:
        row_map: dict[int, str] = {}
        for cell in row_node.findall(f"{XLSX_NS}c"):
            ref = normalize_text(cell.attrib.get("r"))
            col_idx = excel_col_index(ref) if ref else len(row_map)
            raw_type = normalize_text(cell.attrib.get("t"))
            value_node = cell.find(f"{XLSX_NS}v")
            value = normalize_text(value_node.text if value_node is not None else "")
            if raw_type == "s" and value:
                try:
                    value = shared[int(value)]
                except (ValueError, IndexError):
                    pass
            row_map[col_idx] = value
        rows_by_index.append(row_map)

    header_row = rows_by_index[0]
    max_col = max(header_row.keys()) if header_row else -1
    headers = [normalize_text(header_row.get(i, f"column_{i}")) for i in range(max_col + 1)]

    out: list[dict[str, str]] = []
    for row_map in rows_by_index[1:]:
        row = {headers[i]: normalize_text(row_map.get(i, "")) for i in range(len(headers))}
        out.append(row)
    return out


def excel_serial_to_iso(value: str) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""

    if re.fullmatch(r"\d+(\.\d+)?", raw):
        number = float(raw)
        date_value = datetime(1899, 12, 30) + timedelta(days=number)
        return date_value.date().isoformat()

    for fmt in ("%b %d %Y", "%B %d %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return raw


def status_bucket(status: str) -> str:
    text = normalize_name(status)
    if "early announcement" in text:
        return "early"
    if "inactive" in text:
        return "inactive"
    if "in force" in text:
        return "in_force"
    return "unknown"


def split_signatories(value: str) -> list[str]:
    parts = [normalize_text(p) for p in normalize_text(value).split(";")]
    return [p for p in parts if p]


def load_actor_name_map(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            actor_id = normalize_text(row.get("actor_id", "")).upper()
            canonical_name = normalize_text(row.get("canonical_name", ""))
            if not actor_id or not canonical_name:
                continue
            out[normalize_name(canonical_name)] = actor_id
    out.update(SIGNATORY_ALIAS_IDS)
    return out


def resolve_signatory_ids(signatories: list[str], actor_name_map: dict[str, str]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for signatory in signatories:
        normalized = normalize_name(signatory)
        if not normalized:
            continue

        group_members = SIGNATORY_GROUP_MEMBERS.get(normalized, [])
        if group_members:
            for member in group_members:
                if member not in seen:
                    seen.add(member)
                    ids.append(member)
            continue

        actor_id = actor_name_map.get(normalized, "")
        if actor_id and actor_id not in seen:
            seen.add(actor_id)
            ids.append(actor_id)
    return ids


def choose_entry_date(goods_date: str, services_date: str) -> str:
    candidates = [d for d in [excel_serial_to_iso(goods_date), excel_serial_to_iso(services_date)] if d]
    if not candidates:
        return ""
    return sorted(candidates)[0]


def build_agreement_rows(raw_rows: list[dict[str, str]], actor_name_map: dict[str, str], last_updated_iso: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in raw_rows:
        rta_id = normalize_text(row.get("RTA ID"))
        agreement_name = normalize_text(row.get("RTA Name"))
        if not rta_id and not agreement_name:
            continue

        current_signatories = split_signatories(row.get("Current signatories", ""))
        signatory_ids = resolve_signatory_ids(current_signatories, actor_name_map)
        status_text = normalize_text(row.get("Status"))
        data = {
            "rta_id": rta_id,
            "agreement_name": agreement_name,
            "coverage": normalize_text(row.get("Coverage")),
            "agreement_type": normalize_text(row.get("Type")),
            "notification": normalize_text(row.get("Notification")),
            "status": status_text,
            "status_bucket": status_bucket(status_text),
            "entry_into_force_goods_date": excel_serial_to_iso(row.get("Date of Entry into Force (G)", "")),
            "entry_into_force_services_date": excel_serial_to_iso(row.get("Date of Entry into Force (S)", "")),
            "entry_into_force_date": choose_entry_date(
                row.get("Date of Entry into Force (G)", ""),
                row.get("Date of Entry into Force (S)", ""),
            ),
            "rta_composition": normalize_text(row.get("RTA Composition")),
            "region": normalize_text(row.get("Region")),
            "cross_regional": normalize_name(row.get("Cross-regional", "")) == "yes",
            "current_signatory_count": len(current_signatories),
            "current_signatories": current_signatories,
            "signatory_ids": signatory_ids,
            "source_url": f"https://rtais.wto.org/UI/PublicShowRTAIDCard.aspx?rtaid={rta_id}" if rta_id else "",
            "last_updated_iso": last_updated_iso,
        }
        rows.append(data)

    rows.sort(key=lambda r: str(r.get("agreement_name", "")))
    rows.sort(key=lambda r: str(r.get("entry_into_force_date", "")), reverse=True)
    rows.sort(key=lambda r: 0 if r.get("status_bucket") == "in_force" else 1)
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pull latest WTO RTA export and build site/data/world_trade_agreements.json for Worldwide agreements coverage."
        )
    )
    parser.add_argument("--export-url", default=DEFAULT_EXPORT_URL, help="WTO RTA export URL")
    parser.add_argument("--home-url", default=DEFAULT_HOME_URL, help="WTO RTA homepage URL used for last-updated stamp")
    parser.add_argument("--download-file", default="", help="Path to save downloaded XLSX")
    parser.add_argument("--actor-map-file", default="", help="Path to wto_actor_code_map.csv")
    parser.add_argument("--out-file", default="", help="Path to JSON output file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    download_file = resolve_path(args.download_file, DEFAULT_DOWNLOAD_FILE)
    actor_map_file = resolve_path(args.actor_map_file, DEFAULT_ACTOR_MAP_FILE)
    out_file = resolve_path(args.out_file, DEFAULT_OUT_FILE)

    download_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    home_html = fetch_text(args.home_url)
    last_updated_iso = parse_last_updated_iso(home_html)

    xlsx_bytes = fetch_bytes(args.export_url)
    download_file.write_bytes(xlsx_bytes)

    with zipfile.ZipFile(download_file, "r") as zf:
        shared = parse_shared_strings(zf)
        raw_rows = parse_sheet_rows(zf, "xl/worksheets/sheet1.xml", shared)

    actor_name_map = load_actor_name_map(actor_map_file)
    agreements = build_agreement_rows(raw_rows, actor_name_map, last_updated_iso)

    out_file.write_text(json.dumps(agreements, ensure_ascii=False, indent=2), encoding="utf-8")

    in_force_count = sum(1 for row in agreements if row.get("status_bucket") == "in_force")
    mapped_count = sum(1 for row in agreements if row.get("signatory_ids"))
    print(f"Wrote {len(agreements)} agreements -> {out_file}")
    print(f"In-force agreements: {in_force_count}")
    print(f"Agreements with mapped signatory IDs: {mapped_count}")
    print(f"WTO reported last-updated date: {last_updated_iso}")


if __name__ == "__main__":
    main()
