"""
Precompute monthly PRCP sums per station × year for the PyShiny app.

Reads all data/by_station_japan/<ID>.csv.gz (same format as the app's
load_station_prcp), aggregates to one row per (station_id, year, month)
with prcp_sum_mm. Writes a single CSV so the app can do z-scores with
in-memory lookups instead of 202 file reads per interaction.

Output: station_id, year, month, prcp_sum_mm
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BY_STATION_DIR = PROJECT_ROOT / "data" / "by_station_japan"
OUT_DIR = PROJECT_ROOT / "data" / "monthly"
OUT_PATH = OUT_DIR / "japan_monthly_prcp.csv"

COLUMNS = [
    "ID", "DATE", "ELEMENT", "DATA_VALUE",
    "M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME",
]
DTYPE = {
    "ID": "string", "DATE": "string", "ELEMENT": "string", "DATA_VALUE": "Int64",
    "M_FLAG": "string", "Q_FLAG": "string", "S_FLAG": "string", "OBS_TIME": "string",
}


def read_one_station(path: Path) -> pd.DataFrame | None:
    if not path.suffix == ".gz" or path.name.startswith("_"):
        return None
    try:
        df = pd.read_csv(
            path,
            compression="gzip",
            header=None,
            names=COLUMNS,
            dtype=DTYPE,
            low_memory=False,
        )
    except Exception:
        return None
    df = df[df["ELEMENT"] == "PRCP"].copy()
    df = df[df["DATA_VALUE"] != -9999]
    df["PRCP_MM"] = df["DATA_VALUE"] / 10.0
    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d")
    df["year"] = df["DATE"].dt.year
    df["month"] = df["DATE"].dt.month
    station_id = path.stem.replace(".csv", "")
    df["station_id"] = station_id
    return df


def main() -> None:
    BY_STATION_DIR.mkdir(parents=True, exist_ok=True)
    paths = sorted(BY_STATION_DIR.glob("*.csv.gz"))
    if not paths:
        raise FileNotFoundError(
            f"No *.csv.gz files in {BY_STATION_DIR}. Run japan_by_station first."
        )

    chunks = []
    for path in paths:
        one = read_one_station(path)
        if one is not None and not one.empty:
            chunks.append(one)

    if not chunks:
        raise ValueError("No PRCP rows found in any station file.")

    full = pd.concat(chunks, ignore_index=True)
    monthly = (
        full.groupby(["station_id", "year", "month"], as_index=False)["PRCP_MM"]
        .sum()
        .rename(columns={"PRCP_MM": "prcp_sum_mm"})
    )
    monthly = monthly.sort_values(["station_id", "year", "month"])
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    monthly.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} ({len(monthly)} rows, {monthly['station_id'].nunique()} stations)")


if __name__ == "__main__":
    main()
