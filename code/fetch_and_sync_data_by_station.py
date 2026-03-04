from __future__ import annotations

"""
Sync Japan GHCN-Daily station CSVs from the official /by_station/ endpoint.

High-level purpose
- This step turns a Japan+PRCP station manifest into a local cache of per-station
  compressed CSV files that a PyShiny app (or downstream transforms) can read quickly.
- Station selection is delegated to the manifest builder (derived from
  ghcnd-inventory.txt per the GHCN-Daily README). This script only syncs files.

Primary input
- data/manifests/japan_station_ids_prcp.txt
  A newline-delimited list of station IDs (e.g., "JA...") for which PRCP coverage exists.

Remote input
- https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/<ID>.csv.gz
  One compressed CSV per station ID (not guaranteed to exist for every station).

Primary outputs
- data/by_station_japan/<STATION_ID>.csv.gz
  Local copies of the remote station CSVs (gzipped).

Incremental-sync metadata outputs
- data/by_station_japan/_http_cache.json
  Per-station HTTP validators (ETag / Last-Modified) used for conditional GETs.

- data/by_station_japan/_sync_log.txt
  Append-only log with UTC timestamps and a run summary (and failures).
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import time
from typing import Any

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# INPUT: manifest produced by your inventory/manifest builder step.
MANIFEST_PATH = DATA_DIR / "manifests" / "japan_station_ids_prcp.txt"

# OUTPUT: local mirror of /by_station/ for Japan stations (PRCP-focused manifest).
OUT_DIR = DATA_DIR / "by_station_japan"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# OUTPUT: local metadata to enable reproducible incremental syncs (conditional GET).
CACHE_PATH = OUT_DIR / "_http_cache.json"

# OUTPUT: audit trail for runs (what changed, what was missing, what failed).
LOG_PATH = OUT_DIR / "_sync_log.txt"

# REMOTE: authoritative by-station CSV location described in the GHCN-Daily layout.
BY_STATION_BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/"

CONNECT_TIMEOUT = 30
READ_TIMEOUT = 120
CHUNK_SIZE = 1024 * 1024  # 1 MB


def load_station_ids() -> list[str]:
    """
    Read Japan station IDs from the manifest file.

    Inputs
    - MANIFEST_PATH: newline-delimited text file, one station ID per line.
      Expected to include Japan stations with IDs beginning with "JA".

    Output
    - list[str]: sorted, de-duplicated station IDs (deterministic order).

    What this achieves
    - Establishes the exact set of station files we intend to sync locally,
      without re-deriving station membership from ghcnd-stations.txt.
    """
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"Missing manifest at {MANIFEST_PATH}. "
            "Run the manifest builder first (Japan PRCP inventory subset)."
        )

    station_ids: list[str] = []
    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            sid = line.strip()
            if sid and sid.startswith("JA"):
                station_ids.append(sid)

    station_ids = sorted(set(station_ids))
    print(f"Loaded {len(station_ids)} Japan station IDs from {MANIFEST_PATH.name}")
    return station_ids


def load_cache() -> dict[str, dict[str, str]]:
    """
    Load per-station HTTP validators from disk.

    Inputs
    - CACHE_PATH: JSON file written by save_cache() containing:
        { "<station_id>": {"etag": "...", "last_modified": "..."} , ... }

    Output
    - dict[str, dict[str, str]]: cache mapping station_id -> validator dict.
      Returns {} if the cache file does not exist or is unreadable.

    What this achieves
    - Enables conditional GET requests (If-None-Match / If-Modified-Since) so
      daily sync runs only download files that actually changed.
    """
    if not CACHE_PATH.exists():
        return {}
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            out: dict[str, dict[str, str]] = {}
            for k, v in data.items():
                if isinstance(k, str) and isinstance(v, dict):
                    vv: dict[str, str] = {}
                    for kk, vv_raw in v.items():
                        if isinstance(kk, str) and isinstance(vv_raw, str):
                            vv[kk] = vv_raw
                    out[k] = vv
            return out
    except Exception:
        pass
    return {}


def save_cache(cache: dict[str, dict[str, str]]) -> None:
    """
    Persist the HTTP validator cache atomically.

    Inputs
    - cache: dict mapping station_id -> {"etag": str, "last_modified": str}

    Output
    - None (side effect: writes CACHE_PATH)

    What this achieves
    - Makes sync behavior reproducible across runs by remembering server-provided
      validators, while avoiding partial writes via a .part -> replace pattern.
    """
    tmp = CACHE_PATH.with_suffix(CACHE_PATH.suffix + ".part")
    tmp.write_text(json.dumps(cache, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(CACHE_PATH)


def log_line(msg: str) -> None:
    """
    Append a single log line with a UTC timestamp.

    Inputs
    - msg: free-form message (typically summary or per-station failure details)

    Output
    - None (side effect: appends to LOG_PATH)

    What this achieves
    - Provides an audit trail of sync runs for debugging and provenance.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"{timestamp}\t{msg}\n")


def download_station_csv_gz(
    station_id: str,
    cache: dict[str, dict[str, str]],
    retries: int = 3,
) -> str:
    """
    Ensure the local <station_id>.csv.gz matches the remote /by_station/ file.

    Inputs
    - station_id: GHCN-Daily station identifier (expected "JA..." for Japan here).
    - cache: mutable dict for HTTP validators; updated in-place when headers are present.
    - retries: number of HTTP retry attempts for transient failures.

    Outputs
    - str status code:
        "kept"      -> local file existed and we had no validators to justify an HTTP check
        "updated"   -> downloaded and replaced local file
        "unchanged" -> server returned 304 Not Modified; local file kept
        "missing"   -> server returned 404; station has no /by_station/ file
        "failed"    -> non-404 HTTP/network error after all retries

    What this achieves
    - Implements incremental syncing:
        * conditional GET when validators exist
        * atomic writes for downloads (.part then replace)
        * skips stations that do not exist in /by_station/
    """
    url = f"{BY_STATION_BASE}{station_id}.csv.gz"
    out_path = OUT_DIR / f"{station_id}.csv.gz"

    validators = cache.get(station_id, {})
    headers: dict[str, str] = {}
    if "etag" in validators:
        headers["If-None-Match"] = validators["etag"]
    if "last_modified" in validators:
        headers["If-Modified-Since"] = validators["last_modified"]

    # Optimization: if we already have the file and no validators, avoid an HTTP call.
    # (Flip this policy if you want to always revalidate with the server.)
    if out_path.exists() and out_path.stat().st_size > 0 and not headers:
        return "kept"

    for attempt in range(1, retries + 1):
        try:
            with requests.get(
                url,
                stream=True,
                headers=headers,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            ) as resp:
                if resp.status_code == 404:
                    return "missing"

                if resp.status_code == 304:
                    etag = resp.headers.get("ETag")
                    last_modified = resp.headers.get("Last-Modified")
                    if etag or last_modified:
                        cache.setdefault(station_id, {})
                        if etag:
                            cache[station_id]["etag"] = etag
                        if last_modified:
                            cache[station_id]["last_modified"] = last_modified
                    return "unchanged"

                resp.raise_for_status()

                tmp_path = out_path.with_suffix(out_path.suffix + ".part")
                with tmp_path.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                tmp_path.replace(out_path)

                etag = resp.headers.get("ETag")
                last_modified = resp.headers.get("Last-Modified")
                cache.setdefault(station_id, {})
                if etag:
                    cache[station_id]["etag"] = etag
                if last_modified:
                    cache[station_id]["last_modified"] = last_modified

                return "updated"

        except requests.RequestException as e:
            if attempt == retries:
                log_line(f"{station_id}\tfailed\t{type(e).__name__}: {e}")
                return "failed"
            backoff = 2 ** (attempt - 1)
            time.sleep(backoff)

    return "failed"


def main() -> None:
    """
    Orchestrate a full sync run for all station IDs in the manifest.

    Inputs
    - MANIFEST_PATH (station IDs)
    - CACHE_PATH (optional; validators from previous runs)

    Outputs
    - Writes/updates station files in OUT_DIR
    - Writes CACHE_PATH (validator cache)
    - Appends a run summary to LOG_PATH
    - Prints a concise console summary

    What this achieves
    - A deterministic, resumable bulk sync step that can be run daily (or on-demand)
      to keep your local Japan PRCP station corpus up to date for the app.
    """
    station_ids = load_station_ids()
    cache = load_cache()

    counts: dict[str, int] = {"kept": 0, "updated": 0, "unchanged": 0, "missing": 0, "failed": 0}

    for sid in station_ids:
        status = download_station_csv_gz(sid, cache=cache)
        counts[status] = counts.get(status, 0) + 1

        if status in {"updated", "missing", "failed"}:
            print(f"{sid}: {status}")

    save_cache(cache)

    summary = (
        f"Done. total={len(station_ids)} "
        f"updated={counts['updated']} unchanged={counts['unchanged']} kept={counts['kept']} "
        f"missing={counts['missing']} failed={counts['failed']} "
        f"out_dir={OUT_DIR}"
    )
    print(summary)
    log_line(summary)


if __name__ == "__main__":
    main()