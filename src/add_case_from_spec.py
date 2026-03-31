from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEC_DIR = ROOT / "docs" / "case_specs"


def resolve_path(path_str: str, default_path: Path | None = None) -> Path:
    if path_str.strip():
        path = Path(path_str)
        if not path.is_absolute():
            path = ROOT / path
        return path
    if default_path is None:
        raise ValueError("A path value is required")
    return default_path


def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Safe spec-first helper. "
            "This script no longer writes live metadata directly. "
            "It validates a spec file by running the preview pipeline."
        )
    )
    parser.add_argument(
        "spec_path",
        help="Path to a case spec JSON file under docs/case_specs or an absolute path",
    )
    parser.add_argument(
        "--preview-dir",
        default="outputs/spec_preview",
        help="Preview metadata directory for the spec pipeline",
    )
    parser.add_argument(
        "--cache-file",
        default="data/processed/case_price_cache.csv",
        help="Reusable case price cache path",
    )
    parser.add_argument(
        "--build-dir",
        default="outputs/spec_preview_build",
        help="Scratch build output root",
    )
    parser.add_argument(
        "--preview-site-root",
        default="outputs/spec_site_preview/site",
        help="Runnable preview site root",
    )
    args = parser.parse_args()

    spec_path = resolve_path(args.spec_path)

    if not spec_path.exists():
        raise FileNotFoundError(f"Spec file not found: {spec_path}")
    if spec_path.suffix.lower() != ".json":
        raise ValueError(f"Spec file must be a JSON file: {spec_path}")

    try:
        spec_path.relative_to(DEFAULT_SPEC_DIR)
    except ValueError:
        print(
            "Warning: spec file is outside docs/case_specs. "
            "That is allowed for validation, but committed specs should live under docs/case_specs."
        )

    py = sys.executable

    print("Legacy direct-metadata materialization is retired.")
    print("This helper validates the spec by running the full spec preview pipeline.")
    print(f"Spec under test: {spec_path}")

    run(
        [
            py,
            "src/run_spec_preview_pipeline.py",
            "--preview-dir",
            args.preview_dir,
            "--cache-file",
            args.cache_file,
            "--build-dir",
            args.build_dir,
            "--preview-site-root",
            args.preview_site_root,
        ]
    )

    print("\nSpec validation run completed.")
    print("Local verify command:")
    print(f"python -m http.server 8000 --directory {args.preview_site_root}")
    print("\nThen verify at:")
    print("http://localhost:8000")


if __name__ == "__main__":
    main()