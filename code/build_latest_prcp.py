"""
Build latest PRCP per station for the app (no raw .csv.gz reads at runtime).

Reads all data/by_station_japan/<ID>.csv.gz, finds the most recent PRCP
observation per station (with a 1-day buffer: latest date in the file).
Writes data/latest/japan_latest_prcp.csv so the app can show "Latest
precipitation" when the user selects a location without reading gz files.

Output CSV: station_id, latest_date, prcp_mm
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BY_STATION_DIR = PROJECT_ROOT / "data" / "by_station_japan"
OUT_DIR = PROJECT_ROOT / "data" / "latest"
OUT_PATH = OUT_DIR / "japan_latest_prcp.csv"

COLUMNS = [
    "ID", "DATE", "ELEMENT", "DATA_VALUE",
    "M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME",
]
DTYPE = {
    "ID": "string", "DATE": "string", "ELEMENT": "string", "DATA_VALUE": "Int64",
    "M_FLAG": "string", "Q_FLAG": "string", "S_FLAG": "string", "OBS_TIME": "string",
}


def read_one_station_latest(path: Path) -> pd.DataFrame | None:
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
    station_id = path.stem.replace(".csv", "")
    # Latest observation (1-day buffer: just the max date in the file)
    idx = df["DATE"].idxmax()
    row = df.loc[idx]
    return pd.DataFrame([{
        "station_id": station_id,
        "latest_date": row["DATE"].strftime("%Y-%m-%d"),
        "prcp_mm": float(row["PRCP_MM"]),
    }])


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    paths = sorted(BY_STATION_DIR.glob("*.csv.gz"))
    if not paths:
        raise FileNotFoundError(
            f"No *.csv.gz files in {BY_STATION_DIR}. Run japan_by_station first."
        )

    rows = []
    for path in paths:
        one = read_one_station_latest(path)
        if one is not None and not one.empty:
            rows.append(one)

    if not rows:
        raise ValueError("No PRCP rows found in any station file.")

    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values("station_id")
    out.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} ({len(out)} stations)")


if __name__ == "__main__":
    main()
