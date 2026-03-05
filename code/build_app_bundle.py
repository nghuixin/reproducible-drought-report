from __future__ import annotations

"""
Prepare small, app-friendly data files for the PyShiny Japan PRCP app.

This script copies pipeline outputs from the data/ directory into code/app_data/,
so that:

- The interactive app can read from a stable, versioned location
- A Shinylive export can bundle only the minimal data it needs

Expected inputs (produced by the Snakemake pipeline)
----------------------------------------------------
- data/metadata/japan_stations.csv
- data/manifests/japan_prcp_inventory.csv
- data/manifests/japan_prcp_manifest.meta.json
- data/monthly/japan_monthly_prcp.csv
- data/latest/japan_latest_prcp.csv

Outputs (overwritten)
---------------------
- code/app_data/japan_stations.csv
- code/app_data/japan_prcp_inventory.csv
- code/app_data/japan_prcp_manifest.meta.json
- code/app_data/japan_monthly_prcp.csv
- code/app_data/japan_latest_prcp.csv
"""

from pathlib import Path
import shutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
META_DIR = DATA_DIR / "metadata"
MANIFEST_DIR = DATA_DIR / "manifests"
MONTHLY_DIR = DATA_DIR / "monthly"
LATEST_DIR = DATA_DIR / "latest"

APP_DATA_DIR = PROJECT_ROOT / "code" / "app_data"


def copy_required(src: Path, dst: Path) -> None:
    """
    Copy a required file. Fail if missing or empty to keep builds reproducible.
    """
    if not src.exists():
        raise FileNotFoundError(f"Expected input file not found: {src}")
    if src.stat().st_size == 0:
        raise FileNotFoundError(f"Expected input file is empty: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
    print(f"Copied {src} -> {dst}")


def main() -> None:
    files = [
        (META_DIR / "japan_stations.csv", APP_DATA_DIR / "japan_stations.csv"),
        (MANIFEST_DIR / "japan_prcp_inventory.csv", APP_DATA_DIR / "japan_prcp_inventory.csv"),
        (MANIFEST_DIR / "japan_prcp_manifest.meta.json", APP_DATA_DIR / "japan_prcp_manifest.meta.json"),
        (MONTHLY_DIR / "japan_monthly_prcp.csv", APP_DATA_DIR / "japan_monthly_prcp.csv"),
        (LATEST_DIR / "japan_latest_prcp.csv", APP_DATA_DIR / "japan_latest_prcp.csv"),
    ]

    for src, dst in files:
        copy_required(src, dst)


if __name__ == "__main__":
    main()