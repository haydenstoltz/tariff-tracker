from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_PREVIEW_SITE_DATA_DIR = ROOT / "outputs" / "spec_site_preview" / "site" / "data"
DEFAULT_LIVE_SITE_DATA_DIR = ROOT / "site" / "data"
DEFAULT_BACKUP_ROOT = ROOT / "outputs" / "site_backups"


def resolve_path(path_str: str, default_path: Path) -> Path:
    if not path_str.strip():
        return default_path
    path = Path(path_str)
    if not path.is_absolute():
        path = ROOT / path
    return path


def require_path(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label}: {path}")


def validate_source(source_dir: Path) -> None:
    py = sys.executable
    cmd = [
        py,
        "src/validate_exported_site_data.py",
        "--site-data-dir",
        str(source_dir),
    ]
    print("\n>>>", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def backup_live_data(live_dir: Path, backup_root: Path) -> Path | None:
    if not live_dir.exists():
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = backup_root / f"site_data_{timestamp}"
    backup_dir.parent.mkdir(parents=True, exist_ok=True)

    shutil.copytree(live_dir, backup_dir)
    return backup_dir


def replace_live_data(source_dir: Path, live_dir: Path) -> None:
    if live_dir.exists():
        shutil.rmtree(live_dir)
    shutil.copytree(source_dir, live_dir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preview-site-data-dir",
        default="",
        help="Validated preview site data directory. Default: outputs/spec_site_preview/site/data",
    )
    parser.add_argument(
        "--live-site-data-dir",
        default="",
        help="Live site data directory to replace. Default: site/data",
    )
    parser.add_argument(
        "--backup-root",
        default="",
        help="Backup root directory. Default: outputs/site_backups",
    )
    args = parser.parse_args()

    source_dir = resolve_path(args.preview_site_data_dir, DEFAULT_PREVIEW_SITE_DATA_DIR)
    live_dir = resolve_path(args.live_site_data_dir, DEFAULT_LIVE_SITE_DATA_DIR)
    backup_root = resolve_path(args.backup_root, DEFAULT_BACKUP_ROOT)

    require_path(source_dir, "preview site data directory")
    require_path(source_dir / "tariffs.json", "preview tariffs.json")
    require_path(source_dir / "cases.json", "preview cases.json")
    require_path(source_dir / "summary.json", "preview summary.json")
    require_path(source_dir / "charts", "preview charts directory")
    require_path(source_dir / "csv", "preview csv directory")

    validate_source(source_dir)

    backup_dir = backup_live_data(live_dir, backup_root)
    replace_live_data(source_dir, live_dir)

    print("\nPromotion completed.")
    print(f"Source preview data: {source_dir}")
    print(f"Live site data: {live_dir}")
    if backup_dir is not None:
        print(f"Backup of previous live site data: {backup_dir}")
    else:
        print("No previous live site data directory existed, so no backup was created.")
    print("\nLocal verify command:")
    print("python -m http.server 8000 --directory site")
    print("\nThen verify at:")
    print("http://localhost:8000")