from __future__ import annotations

"""
Sync GHCN-Daily station metadata (ghcnd-stations.txt) and write a Japan-only CSV subset.

High-level purpose
- Maintain a local copy of the authoritative GHCN-Daily station metadata file
  (ghcnd-stations.txt; README §IV).
- Produce a Japan-only stations table for mapping and UI selection (e.g., PyShiny map).

Primary remote input (authoritative)
- https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt

Local inputs
- data/metadata/_stations_http_cache.json (optional; ETag/Last-Modified validators)

Primary outputs
1) data/metadata/ghcnd-stations.txt
   Local copy of the fixed-width stations metadata file.

2) data/metadata/japan_stations.csv
   Japan-only subset with fields suitable for plotting:
   station_id, name, latitude, longitude, elevation_m, gsn_flag, hcn_crn_flag, wmo_id

Incremental-sync metadata outputs
- data/metadata/_stations_http_cache.json
  Stores HTTP validators to enable conditional GET on future runs.

- data/metadata/_stations_sync_log.txt
  Append-only run log with UTC timestamps (status + output row count).

What this step achieves
- A stable, reproducible station metadata layer for your Japan PRCP app:
  you can join this with PRCP availability (inventory-derived) and/or with downloaded
  by_station station CSVs for interactive exploration.
"""

from pathlib import Path
from datetime import datetime, timezone
import csv
import json
import time

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

META_DIR = DATA_DIR / "metadata"
META_DIR.mkdir(parents=True, exist_ok=True)

# OUTPUT (local mirror): fixed-width stations metadata file (README §IV).
STATIONS_PATH = META_DIR / "ghcnd-stations.txt"

# OUTPUT (derived): Japan-only subset for mapping / UI.
JAPAN_STATIONS_CSV = META_DIR / "japan_stations.csv"

# OUTPUT (sync metadata): HTTP validators for conditional GET.
CACHE_PATH = META_DIR / "_stations_http_cache.json"

# OUTPUT (audit): append-only log of runs and failures.
LOG_PATH = META_DIR / "_stations_sync_log.txt"

# REMOTE: authoritative stations metadata file.
GHCND_STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"

CONNECT_TIMEOUT = 30
READ_TIMEOUT = 120
CHUNK_SIZE = 1024 * 1024  # 1 MB


def log_line(msg: str) -> None:
    """
    Append a timestamped message to the sync log.

    Inputs
    - msg: a single-line string describing status or errors

    Outputs
    - None (side effect: appends to LOG_PATH)

    What this achieves
    - An audit trail for provenance/debugging of station metadata sync runs.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"{ts}\t{msg}\n")


def load_cache() -> dict[str, str]:
    """
    Load HTTP validators used for conditional GET requests.

    Inputs
    - CACHE_PATH: JSON file with keys like {"etag": "...", "last_modified": "..."}

    Outputs
    - dict[str, str]: parsed validators (possibly empty if cache missing/unreadable)

    What this achieves
    - Enables bandwidth-friendly daily updates by allowing If-None-Match /
      If-Modified-Since requests.
    """
    if not CACHE_PATH.exists():
        return {}
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            out: dict[str, str] = {}
            for k, v in data.items():
                if isinstance(k, str) and isinstance(v, str):
                    out[k] = v
            return out
    except Exception:
        return {}
    return {}


def save_cache(cache: dict[str, str]) -> None:
    """
    Persist HTTP validators atomically.

    Inputs
    - cache: dict[str,str] containing at least "etag" and/or "last_modified"

    Outputs
    - None (side effect: writes CACHE_PATH)

    What this achieves
    - Makes conditional syncing reproducible across runs while avoiding partial writes.
    """
    tmp = CACHE_PATH.with_suffix(CACHE_PATH.suffix + ".part")
    tmp.write_text(json.dumps(cache, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(CACHE_PATH)


def sync_stations_txt(retries: int = 3) -> str:
    """
    Ensure STATIONS_PATH is up to date with the remote ghcnd-stations.txt.

    Inputs
    - retries: number of retry attempts for transient network/HTTP failures
    - CACHE_PATH (optional): provides ETag/Last-Modified validators if present

    Outputs
    - Returns a status string:
        "updated"   -> downloaded and replaced local file
        "unchanged" -> server returned 304 Not Modified (local file kept)
        "kept"      -> local file existed and no validators were available, so no HTTP call

    Side effects
    - May write STATIONS_PATH (on update)
    - May write CACHE_PATH (when response includes validators)
    - May log failures to LOG_PATH

    What this achieves
    - A daily-runnable, bandwidth-friendly station metadata sync step.
    """
    cache = load_cache()
    headers: dict[str, str] = {}

    if "etag" in cache:
        headers["If-None-Match"] = cache["etag"]
    if "last_modified" in cache:
        headers["If-Modified-Since"] = cache["last_modified"]

    # Policy: if file exists but we have no validators yet, keep it to avoid
    # redundant downloads. (You can change this policy if you want a one-time
    # forced refresh to populate validators.)
    if STATIONS_PATH.exists() and STATIONS_PATH.stat().st_size > 0 and not headers:
        return "kept"

    for attempt in range(1, retries + 1):
        try:
            with requests.get(
                GHCND_STATIONS_URL,
                stream=True,
                headers=headers,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            ) as resp:
                if resp.status_code == 304 and STATIONS_PATH.exists():
                    etag = resp.headers.get("ETag")
                    last_modified = resp.headers.get("Last-Modified")
                    if etag:
                        cache["etag"] = etag
                    if last_modified:
                        cache["last_modified"] = last_modified
                    save_cache(cache)
                    return "unchanged"

                resp.raise_for_status()

                tmp = STATIONS_PATH.with_suffix(STATIONS_PATH.suffix + ".part")
                with tmp.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                tmp.replace(STATIONS_PATH)

                etag = resp.headers.get("ETag")
                last_modified = resp.headers.get("Last-Modified")
                if etag:
                    cache["etag"] = etag
                if last_modified:
                    cache["last_modified"] = last_modified
                save_cache(cache)

                return "updated"

        except requests.RequestException as e:
            if attempt == retries:
                log_line(f"stations\tfailed\t{type(e).__name__}: {e}")
                raise
            time.sleep(2 ** (attempt - 1))

    return "kept"


def write_japan_subset() -> int:
    """
    Parse fixed-width ghcnd-stations.txt and write a Japan-only CSV subset.

    Inputs
    - STATIONS_PATH: local fixed-width stations file (must exist and be non-empty)

    Outputs
    - Returns: int row_count (number of Japan stations written)
    - Writes: JAPAN_STATIONS_CSV (overwritten atomically)

    Fixed-width schema reference (README §IV)
      ID         1-11  -> line[0:11]
      LAT       13-20  -> line[12:20]
      LON       22-30  -> line[21:30]
      ELEV      32-37  -> line[31:37]
      NAME      42-71  -> line[41:71]
      GSN       73-75  -> line[72:75]
      HCN/CRN   77-79  -> line[76:79]
      WMOID     81-85  -> line[80:85]

    What this achieves
    - Produces a clean, app-friendly stations table for Japan that can be
      used for mapping and station lookup without doing fixed-width parsing in the UI.
    """
    if not STATIONS_PATH.exists() or STATIONS_PATH.stat().st_size == 0:
        raise FileNotFoundError(f"Missing stations file at {STATIONS_PATH}")

    rows = 0
    tmp = JAPAN_STATIONS_CSV.with_suffix(JAPAN_STATIONS_CSV.suffix + ".part")
    with STATIONS_PATH.open("r", encoding="utf-8") as infile, tmp.open("w", encoding="utf-8", newline="") as out:
        w = csv.writer(out)
        w.writerow(["station_id", "name", "latitude", "longitude", "elevation_m", "gsn_flag", "hcn_crn_flag", "wmo_id"])

        for line in infile:
            if len(line) < 85:
                continue
            station_id = line[0:11].strip()
            if not station_id.startswith("JA"):
                continue

            try:
                lat = float(line[12:20].strip())
                lon = float(line[21:30].strip())
                elev_raw = line[31:37].strip()
                elev = float(elev_raw) if elev_raw and elev_raw != "-999.9" else None
            except ValueError:
                continue

            name = line[41:71].strip()
            gsn = line[72:75].strip()
            hcn_crn = line[76:79].strip()
            wmo = line[80:85].strip()

            w.writerow(
                [station_id, name, f"{lat:.4f}", f"{lon:.4f}", "" if elev is None else f"{elev:.1f}", gsn, hcn_crn, wmo]
            )
            rows += 1

    tmp.replace(JAPAN_STATIONS_CSV)
    return rows


def main() -> None:
    """
    Run the station metadata sync and Japan subset build end-to-end.

    Inputs
    - Remote stations file (queried conditionally using CACHE_PATH validators when available)

    Outputs
    - STATIONS_PATH (possibly updated)
    - JAPAN_STATIONS_CSV (always regenerated)
    - CACHE_PATH (possibly updated)
    - LOG_PATH (appended summary)

    What this achieves
    - A single command you can schedule (daily/weekly) to keep Japan station metadata current.
    """
    status = sync_stations_txt()
    rows = write_japan_subset()

    msg = f"stations_txt={status}\tjapan_rows={rows}\tpath={JAPAN_STATIONS_CSV}"
    print(msg)
    log_line(msg)


if __name__ == "__main__":
    main()