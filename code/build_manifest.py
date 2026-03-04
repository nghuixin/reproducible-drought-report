from __future__ import annotations

"""
Build Japan PRCP station manifest + coverage table from the GHCN-Daily inventory.

High-level purpose
- Convert the global fixed-width inventory file (ghcnd-inventory.txt; README §VII)
  into a Japan-only set of station IDs that have PRCP, plus a per-station PRCP
  coverage table (first/last year).
- This creates the *selection contract* for downstream steps (e.g., syncing
  /by_station/<ID>.csv.gz and powering a PyShiny PRCP viewer).

Primary input
- data/ghcnd-inventory.txt
  Fixed-width inventory describing element coverage per station.

Remote input (only if inventory missing/empty)
- https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt

Primary outputs (always regenerated)
1) data/manifests/japan_station_ids_prcp.txt
   Newline-delimited station IDs (unique, sorted) for Japan stations with PRCP.

2) data/manifests/japan_prcp_inventory.csv
   One row per station: station_id, lat, lon, firstyear_prcp, lastyear_prcp.

3) data/manifests/japan_prcp_manifest.meta.json
   Small provenance record (paths, station_count, sha256 of the station-id manifest).

What this step achieves
- Establishes a deterministic, reproducible list of PRCP-capable Japan stations
  derived from the authoritative GHCN-Daily inventory, without scanning
  ghcnd-stations.txt or downloading station-by-station data yet.
"""

from pathlib import Path
from typing import Iterable
import csv
import hashlib
import requests
from datetime import datetime, timezone
import json

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_DIR = DATA_DIR / "manifests"
MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

# INPUT: fixed-width inventory file (README §VII).
INVENTORY_PATH = DATA_DIR / "ghcnd-inventory.txt"

# OUTPUTS: Japan PRCP station selection + coverage table + tiny provenance metadata.
STATION_IDS_PATH = MANIFEST_DIR / "japan_station_ids_prcp.txt"
PRCP_COVERAGE_CSV_PATH = MANIFEST_DIR / "japan_prcp_inventory.csv"
RUN_METADATA_PATH = MANIFEST_DIR / "japan_prcp_manifest.meta.json"

# REMOTE: authoritative inventory endpoint (downloaded only if local file missing/empty).
GHCND_INVENTORY_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"

CONNECT_TIMEOUT = 30
READ_TIMEOUT = 120
CHUNK_SIZE = 1024 * 1024  # 1 MB


def ensure_inventory_exists() -> None:
    """
    Ensure the local inventory file exists and is non-empty.

    Inputs
    - INVENTORY_PATH: local target path for ghcnd-inventory.txt
    - GHCND_INVENTORY_URL: remote source used only if local file missing/empty

    Outputs
    - None (side effect: may write INVENTORY_PATH)

    What this achieves
    - Guarantees downstream parsing reads from a local inventory file.
    - Avoids unnecessary downloads on repeated runs by only fetching when missing/empty.
    """
    if INVENTORY_PATH.exists() and INVENTORY_PATH.stat().st_size > 0:
        print(f"Inventory exists: {INVENTORY_PATH}")
        return

    print(f"Downloading inventory -> {INVENTORY_PATH}")
    with requests.get(
        GHCND_INVENTORY_URL,
        stream=True,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    ) as resp:
        resp.raise_for_status()
        tmp_path = INVENTORY_PATH.with_suffix(INVENTORY_PATH.suffix + ".part")
        with tmp_path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
        tmp_path.replace(INVENTORY_PATH)

    print(f"Wrote inventory: {INVENTORY_PATH} ({INVENTORY_PATH.stat().st_size} bytes)")


def iter_japan_prcp_inventory_lines(lines: Iterable[str]) -> Iterable[tuple[str, float, float, int, int]]:
    """
    Stream-parse inventory lines and emit Japan+PRCP records.

    Inputs
    - lines: iterable of raw text lines from ghcnd-inventory.txt

    Outputs
    - yields tuples:
        (station_id, latitude, longitude, firstyear_prcp, lastyear_prcp)

    Selection criteria
    - station_id starts with "JA" (Japan stations per GHCN-Daily ID convention)
    - element == "PRCP" (precipitation)

    Fixed-width schema reference (README §VII)
    - ID        columns 1-11   -> line[0:11]
    - LAT       columns 13-20  -> line[12:20]
    - LON       columns 22-30  -> line[21:30]
    - ELEMENT   columns 32-35  -> line[31:35]
    - FIRSTYEAR columns 37-40  -> line[36:40]
    - LASTYEAR  columns 42-45  -> line[41:45]

    What this achieves
    - Provides a memory-efficient generator that filters the global inventory
      down to only the records needed to build the Japan PRCP manifest.
    """
    for line in lines:
        if len(line) < 45:
            continue

        station_id = line[0:11].strip()
        element = line[31:35].strip()

        if not (station_id.startswith("JA") and element == "PRCP"):
            continue

        try:
            lat = float(line[12:20].strip())
            lon = float(line[21:30].strip())
            firstyear = int(line[36:40].strip())
            lastyear = int(line[41:45].strip())
        except ValueError:
            continue

        yield (station_id, lat, lon, firstyear, lastyear)


def write_manifest_and_coverage() -> None:
    """
    Build and write the Japan PRCP station manifest and coverage table.

    Inputs
    - INVENTORY_PATH: local fixed-width inventory file.
      This function assumes ensure_inventory_exists() has already run.

    Outputs (overwritten atomically each run)
    - STATION_IDS_PATH: text file with unique, sorted station IDs (one per line).
    - PRCP_COVERAGE_CSV_PATH: CSV coverage table (one row per station).
    - RUN_METADATA_PATH: JSON metadata including SHA-256 of the station-id manifest.

    What this achieves
    - Produces the deterministic station set used by downstream download/sync steps
      and by the app for station selection and map/coverage display.
    """
    print("Generating Japan PRCP station manifest + coverage table...")

    # In theory the inventory has at most one PRCP row per station, but we deduplicate
    # defensively and (if duplicates occur) keep the widest [firstyear, lastyear] range.
    records_by_station: dict[str, tuple[float, float, int, int]] = {}

    with INVENTORY_PATH.open("r", encoding="utf-8") as infile:
        for station_id, lat, lon, firstyear, lastyear in iter_japan_prcp_inventory_lines(infile):
            if station_id not in records_by_station:
                records_by_station[station_id] = (lat, lon, firstyear, lastyear)
            else:
                old_lat, old_lon, old_first, old_last = records_by_station[station_id]
                new_first = min(old_first, firstyear)
                new_last = max(old_last, lastyear)
                records_by_station[station_id] = (old_lat, old_lon, new_first, new_last)

    station_ids_sorted = sorted(records_by_station.keys())

    # OUTPUT 1: station ID manifest (newline-delimited).
    tmp_ids = STATION_IDS_PATH.with_suffix(STATION_IDS_PATH.suffix + ".part")
    with tmp_ids.open("w", encoding="utf-8") as f:
        for sid in station_ids_sorted:
            f.write(f"{sid}\n")
    tmp_ids.replace(STATION_IDS_PATH)

    # OUTPUT 2: coverage table (lat/lon + first/last PRCP year).
    tmp_cov = PRCP_COVERAGE_CSV_PATH.with_suffix(PRCP_COVERAGE_CSV_PATH.suffix + ".part")
    with tmp_cov.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["station_id", "latitude", "longitude", "firstyear_prcp", "lastyear_prcp"])
        for sid in station_ids_sorted:
            lat, lon, firstyear, lastyear = records_by_station[sid]
            w.writerow([sid, f"{lat:.4f}", f"{lon:.4f}", firstyear, lastyear])
    tmp_cov.replace(PRCP_COVERAGE_CSV_PATH)

    # OUTPUT 3: small provenance metadata (deterministic hash of the station-id manifest).
    manifest_hash = hashlib.sha256(STATION_IDS_PATH.read_bytes()).hexdigest()


  # ISO 8601 UTC timestamp, no microseconds
    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    payload = {
        "run_timestamp_utc": now_utc,
        # what the app should display for this artifact
        "updated_as_of_utc": now_utc,
        "inventory_path": INVENTORY_PATH.as_posix(),
        "station_ids_path": STATION_IDS_PATH.as_posix(),
        "coverage_csv_path": PRCP_COVERAGE_CSV_PATH.as_posix(),
        "station_count": len(station_ids_sorted),
        "manifest_sha256": manifest_hash,
    }

    tmp_meta = RUN_METADATA_PATH.with_suffix(RUN_METADATA_PATH.suffix + ".part")
    tmp_meta.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp_meta.replace(RUN_METADATA_PATH)

 
  
    print(f"Stations with PRCP in Japan: {len(station_ids_sorted)}")
    print(f"Wrote manifest: {STATION_IDS_PATH}")
    print(f"Wrote coverage: {PRCP_COVERAGE_CSV_PATH}")
    print(f"Wrote meta:     {RUN_METADATA_PATH}")


def main() -> None:
    """
    Run the manifest build end-to-end.

    Inputs
    - Local inventory file (downloaded if missing/empty)

    Outputs
    - Writes the manifest, coverage table, and metadata files under data/manifests/

    What this achieves
    - Single entry point suitable for a scheduled pipeline step.
    """
    ensure_inventory_exists()
    write_manifest_and_coverage()


if __name__ == "__main__":
    main()