from __future__ import annotations

"""
Prepare small, app-friendly data files for the PyShiny Japan PRCP app.

This script copies (or lightly reshapes) pipeline outputs from the data/
directory into code/app_data/, so that:

- The interactive app can read from a stable, versioned location
- A Shinylive export can bundle only the minimal data it needs

Current behavior
----------------
- Reads:
    data/metadata/japan_stations.csv
    data/manifests/japan_prcp_inventory.csv
    data/manifests/japan_prcp_manifest.meta.json
- Writes (overwrites):
    code/app_data/japan_stations.csv
    code/app_data/japan_prcp_inventory.csv
    code/app_data/japan_prcp_manifest.meta.json

Notes
-----
- At the moment this script simply copies the full CSVs/JSON into app_data.
  If the files become too large for client-side use (e.g. in Shinylive), you
  can add filtering/aggregation here (e.g., subset stations or years).
"""

from pathlib import Path
import shutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
META_DIR = DATA_DIR / "metadata"
MANIFEST_DIR = DATA_DIR / "manifests"
MONTHLY_DIR = DATA_DIR / "monthly"

APP_DATA_DIR = PROJECT_ROOT / "code" / "app_data"


def copy_if_exists(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"Expected input file not found: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
    print(f"Copied {src} -> {dst}")


def copy_optional(src: Path, dst: Path) -> None:
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        print(f"Copied {src} -> {dst}")
    else:
        print(f"Optional {src} not found; skipping (app will use per-station reads or show no z-scores).")


def main() -> None:
    stations_src = META_DIR / "japan_stations.csv"
    coverage_src = MANIFEST_DIR / "japan_prcp_inventory.csv"
    meta_src = MANIFEST_DIR / "japan_prcp_manifest.meta.json"

    stations_dst = APP_DATA_DIR / "japan_stations.csv"
    coverage_dst = APP_DATA_DIR / "japan_prcp_inventory.csv"
    meta_dst = APP_DATA_DIR / "japan_prcp_manifest.meta.json"

    copy_if_exists(stations_src, stations_dst)
    copy_if_exists(coverage_src, coverage_dst)
    copy_if_exists(meta_src, meta_dst)

    # Precomputed monthly PRCP (optional): makes z-score map instant instead of 202 file reads
    monthly_src = MONTHLY_DIR / "japan_monthly_prcp.csv"
    monthly_dst = APP_DATA_DIR / "japan_monthly_prcp.csv"
    copy_optional(monthly_src, monthly_dst)


if __name__ == "__main__":
    main()

