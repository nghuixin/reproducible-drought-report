from __future__ import annotations
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
from shiny import App, reactive, render, ui
from shinywidgets import output_widget, render_widget
import calendar
import json
import numpy as np
from datetime import date, datetime, timezone

 


# ----------------------------------------------------------------------
# Paths and data loading
# ----------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
META_DIR = DATA_DIR / "metadata"
MANIFEST_DIR = DATA_DIR / "manifests"
BY_STATION_DIR = DATA_DIR / "by_station_japan"

# When exporting to Shinylive or packaging the app, we prefer reading from
# code/app_data/, which is populated by prepare_app_data.py. For local
# development, we fall back to the data/ outputs if app_data is missing.
APP_DATA_DIR = Path(__file__).parent / "app_data"

APP_META_PATH = APP_DATA_DIR / "japan_prcp_manifest.meta.json"
APP_STATIONS_PATH = APP_DATA_DIR / "japan_stations.csv"
APP_COVERAGE_PATH = APP_DATA_DIR / "japan_prcp_inventory.csv"
APP_MONTHLY_PATH = APP_DATA_DIR / "japan_monthly_prcp.csv"

METADATA_PATH = APP_META_PATH if APP_META_PATH.exists() else MANIFEST_DIR / "japan_prcp_manifest.meta.json"
STATIONS_PATH = APP_STATIONS_PATH if APP_STATIONS_PATH.exists() else META_DIR / "japan_stations.csv"
COVERAGE_PATH = APP_COVERAGE_PATH if APP_COVERAGE_PATH.exists() else MANIFEST_DIR / "japan_prcp_inventory.csv"
MONTHLY_PATH = APP_MONTHLY_PATH if APP_MONTHLY_PATH.exists() else PROJECT_ROOT / "data" / "monthly" / "japan_monthly_prcp.csv"
LOCAL_TZ  = timezone.utc

 



def read_updated_as_of(metadata_path: Path) -> str:
    if not metadata_path.exists():
        return "Unknown"

    meta = json.loads(metadata_path.read_text(encoding="utf-8"))
    ts = meta.get("updated_as_of_utc") or meta.get("run_timestamp_utc")
    if not ts:
        return "Unknown"

    dt_utc = datetime.fromisoformat(ts)
    dt_local = dt_utc.astimezone(LOCAL_TZ)
    return dt_local.strftime("%Y-%m-%d")

# Load station metadata (required)
if not STATIONS_PATH.exists():
    raise FileNotFoundError(
        f"Expected station metadata at {STATIONS_PATH}. "
        "Run sync_station_metadata.py first."
    )

stations_df = pd.read_csv(STATIONS_PATH)
 
stations_df.columns = [c.strip().lower() for c in stations_df.columns]
stations_df = stations_df.rename(columns={"id": "station_id"})
# Ensure we have numeric lat/lon
stations_df["latitude"] = pd.to_numeric(stations_df["latitude"], errors="coerce")
stations_df["longitude"] = pd.to_numeric(stations_df["longitude"], errors="coerce")

# Build a label for the dropdown
def make_label(row: pd.Series) -> str:
    name = str(row.get("name", "")).strip()
    sid = str(row["station_id"]).strip()
    if name:
        return f"{name} ({sid})"
    return sid

stations_df["label"] = stations_df.apply(make_label, axis=1)

# Load PRCP coverage if available (optional)
if COVERAGE_PATH.exists():
    coverage_df = pd.read_csv(COVERAGE_PATH)
    coverage_df = coverage_df.set_index("station_id")
else:
    coverage_df = None

# Convenience: mapping from station_id -> metadata row
stations_by_id = stations_df.set_index("station_id")

# Precomputed monthly PRCP (station_id, year, month, prcp_sum_mm). If present, z-score map uses this
# and avoids 202 file reads per "Update map"; otherwise we fall back to per-station file reads.
monthly_prcp_df: pd.DataFrame | None = None
if MONTHLY_PATH.exists():
    monthly_prcp_df = pd.read_csv(MONTHLY_PATH)
    monthly_prcp_df["station_id"] = monthly_prcp_df["station_id"].astype(str)


def load_station_prcp(station_id: str):
    path = PROJECT_ROOT / "data" / "by_station_japan" / f"{station_id}.csv.gz"

    df = pd.read_csv(
        path,
        compression="gzip",
        header=None,  # IMPORTANT: no header in file
        names=[
            "ID",
            "DATE",
            "ELEMENT",
            "DATA_VALUE",
            "M_FLAG",
            "Q_FLAG",
            "S_FLAG",
            "OBS_TIME",
        ],
        dtype={
            "ID": "string",
            "DATE": "string",
            "ELEMENT": "string",
            "DATA_VALUE": "Int64",
            "M_FLAG": "string",
            "Q_FLAG": "string",
            "S_FLAG": "string",
            "OBS_TIME": "string",
        },
        low_memory=False,
    )

    # Keep only PRCP
    df = df[df["ELEMENT"] == "PRCP"].copy()

    # Remove missing sentinel
    df = df[df["DATA_VALUE"] != -9999]

    # Convert units (tenths of mm → mm)
    df["PRCP_MM"] = df["DATA_VALUE"] / 10.0

    # Parse date and add year/month for month_precip_by_year
    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d")
    df["YEAR"] = df["DATE"].dt.year
    df["MONTH"] = df["DATE"].dt.month
    df["DAY"] = df["DATE"].dt.day

    return df

updated_as_of = read_updated_as_of(METADATA_PATH)

 # ----------------------------------------------------------------------
# UI
# ----------------------------------------------------------------------

stations_sorted = stations_df.sort_values("label", kind="stable")

first_station = (
    stations_sorted["station_id"].iloc[0] if len(stations_sorted) > 0 else None
)

app_ui = ui.page_fluid(
    ui.panel_title(
        f"Precipitation Index in Japan 降水量指数 - Updated 更新 {updated_as_of}"
    ),
    ui.layout_sidebar(
        ui.sidebar(
            ui.h4("Station 気象庁"),
            ui.input_selectize(
                "station_id",
                "Location and ID 地点とID",
                choices=dict(zip(stations_sorted["station_id"], stations_sorted["label"])),
                selected=first_station,
                options={"placeholder": "Search by name or station ID..."},
            ),
            ui.h5("Precipitation 降水量指数 (mm)"),
           ui.input_numeric("selected_year", "Year 年", value=2000, min=1970, max=date.today().year),
           ui.input_select(
                "selected_month",
                "Month 月",
                choices={
                    "1": "Jan", "2": "Feb", "3": "Mar", "4": "Apr", "5": "May", "6": "Jun",
                    "7": "Jul", "8": "Aug", "9": "Sep", "10": "Oct", "11": "Nov", "12": "Dec" },
                selected=str(4),
            ),
            ui.input_action_button("update_map", "Update map"),  
            # If you want the old sidebar outputs back, uncomment:
            # ui.output_ui("prcp_on_day"),
            # ui.output_ui("zscore_recent_window"),
        ),
        ui.div(
            ui.h4(""),
            output_widget("station_map"),
        ),
    ),
)
# ----------------------------------------------------------------------
# Server
# ----------------------------------------------------------------------
def month_window(year: int, month: int, buffer_days: int = 3):
    """
    Returns (start, end) timestamps for the chosen month/year,
    with end buffered by buffer_days.
    """
    start = pd.Timestamp(year=year, month=month, day=1)
    last_day = calendar.monthrange(year, month)[1]
    end = pd.Timestamp(year=year, month=month, day=last_day) - pd.Timedelta(days=buffer_days)

    # Guard in case buffer would push end before start (only possible if buffer huge)
    if end < start:
        end = start

    return start.normalize(), end.normalize()


def month_precip_by_year(df: pd.DataFrame, month: int, min_year: int = 1970) -> pd.DataFrame:
    """
    For one station: precipitation totals for that month for each year >= min_year.
    Requires df has YEAR, MONTH, PRCP_MM (we already precompute those in your cached loader).
    """
    if df.empty:
        return pd.DataFrame(columns=["YEAR", "PRCP_SUM"])

    d = df[(df["YEAR"] >= min_year) & (df["MONTH"] == month)]
    out = d.groupby("YEAR", as_index=False).agg(PRCP_SUM=("PRCP_MM", "sum"))
    return out

 

def zscore_for_year(window_by_year: pd.DataFrame, target_year: int, min_hist_years: int = 25) -> Optional[float]:
    if window_by_year.empty:
        return None

    x_row = window_by_year[window_by_year["YEAR"] == target_year]
    if x_row.empty:
        return None

    hist = window_by_year[window_by_year["YEAR"] < target_year]["PRCP_SUM"].dropna()
    if hist.shape[0] < min_hist_years:
        return None

    mu = float(hist.mean())
    sigma = float(hist.std(ddof=1))
    if not np.isfinite(sigma) or sigma == 0.0:
        return None

    x = float(x_row["PRCP_SUM"].iloc[0])
    z = (x - mu) / sigma
    return float(np.clip(z, -2, 2))





def server(input, output, session):
    committed_year = reactive.Value(2000)
    committed_month = reactive.Value(4)

    @reactive.Effect
    @reactive.event(input.update_map)
    def _commit_month_year():
        y = int(input.selected_year())
        m = int(input.selected_month())
        y = min(y, date.today().year)
        committed_year.set(y)
        committed_month.set(m)

    @reactive.Calc
    def selected_station_id() -> Optional[str]:
        sid = input.station_id()
        if sid in stations_by_id.index:
            return sid
        return None

    @reactive.Calc
    def selected_station_row() -> Optional[pd.Series]:
        sid = selected_station_id()
        if sid is None:
            return None
        return stations_by_id.loc[sid]

    @reactive.Calc
    def selected_coverage() -> Optional[dict]:
        sid = selected_station_id()
        if sid is None or coverage_df is None:
            return None
        if sid not in coverage_df.index:
            return None
        row = coverage_df.loc[sid]
        return {
            "firstyear_prcp": int(row["firstyear_prcp"]),
            "lastyear_prcp": int(row["lastyear_prcp"]),
        }

    @reactive.Calc
    def stations_with_zscores() -> pd.DataFrame:
        year = committed_year.get()
        month = committed_month.get()

        out = stations_df.copy()
        out["z_score"] = np.nan

        if monthly_prcp_df is not None:
            # Use precomputed monthly table: in-memory only, instant.
            for sid in out["station_id"].astype(str).tolist():
                station_monthly = monthly_prcp_df[monthly_prcp_df["station_id"] == sid]
                if station_monthly.empty:
                    continue
                month_rows = station_monthly[
                    (station_monthly["month"] == month) & (station_monthly["year"] >= 1945)
                ]
                if month_rows.empty:
                    continue
                per_year = month_rows[["year", "prcp_sum_mm"]].copy()
                per_year = per_year.rename(columns={"year": "YEAR", "prcp_sum_mm": "PRCP_SUM"})
                z = zscore_for_year(per_year, target_year=year, min_hist_years=25)
                if z is not None:
                    out.loc[out["station_id"] == sid, "z_score"] = z
            return out

        # Fallback: read each station file (slow, 202 reads per update)
        for sid in out["station_id"].astype(str).tolist():
            try:
                df = load_station_prcp(sid)
            except FileNotFoundError:
                continue
            per_year = month_precip_by_year(df, month=month, min_year=1)
            z = zscore_for_year(per_year, target_year=year, min_hist_years=25)
            if z is not None:
                out.loc[out["station_id"] == sid, "z_score"] = z

        return out

    @output
    @render.ui
    def station_info():
        row = selected_station_row()
        if row is None:
            return ui.div("No station selected.")

        sid = row["station_id"]
        name = row.get("name", "")
        lat = row.get("latitude", float("nan"))
        lon = row.get("longitude", float("nan"))
        elev = row.get("elevation_m", "")

        cov = selected_coverage()
        if cov is not None:
            cov_text = f"{cov['firstyear_prcp']}–{cov['lastyear_prcp']}"
        else:
            cov_text = "unknown"

        return ui.div(
            ui.tags.b(f"{name} ({sid})"),
            ui.br(),
            f"Latitude: {lat}  |  Longitude: {lon}",
            ui.br(),
            f"Elevation (m): {elev if elev != '' else 'N/A'}",
            ui.br(),
            f"PRCP coverage years: {cov_text}",
        )

    @output
    @render_widget
    def station_map():
        """
        Plotly + OpenStreetMap map of all Japan stations. Dots are colored by
        z-score for the committed year/month (red = drier, blue = wetter).
        Selected station is highlighted with a red ring.
        """
        sid = selected_station_id()
        z_df = stations_with_zscores()

        # Color by z-score: red = dry (low), blue = wet (high). NaN → gray.
        has_z = z_df["z_score"].notna()
        with_z = z_df[has_z]
        no_z = z_df[~has_z]

        if with_z.empty:
            # No z-scores at all: single gray scatter
            fig = px.scatter_mapbox(
                z_df,
                lat="latitude",
                lon="longitude",
                hover_name="name",
                hover_data={"station_id": True, "latitude": False, "longitude": False},
                zoom=3.5,
                center={"lat": 35.0, "lon": 135.0},
            )
            fig.update_traces(marker=dict(size=7, color="lightgray"))
        else:
            fig = px.scatter_mapbox(
                with_z,
                lat="latitude",
                lon="longitude",
                color="z_score",
                color_continuous_scale="RdBu_r",
                range_color=(-2.0, 2.0),
                hover_name="name",
                hover_data={
                    "station_id": True,
                    "z_score": ":.2f",
                    "latitude": False,
                    "longitude": False,
                },
                zoom=3.5,
                center={"lat": 35.0, "lon": 135.0},
            )
            fig.update_traces(
                customdata=with_z[["name", "station_id", "z_score"]],
                hovertemplate=(
                    "Station: %{customdata[0]}<br>ID: %{customdata[1]}<br>"
                    "Z-score: %{customdata[2]:.2f}<extra></extra>"
                ),
                marker=dict(size=7),
            )
            # Gray dots for stations with no z-score
            if not no_z.empty:
                fig.add_scattermapbox(
                    lat=no_z["latitude"],
                    lon=no_z["longitude"],
                    mode="markers",
                    marker=dict(size=7, color="lightgray", symbol="circle"),
                    hoverinfo="skip",
                    showlegend=False,
                )

        layout_kw = dict(
            mapbox_style="open-street-map",
            margin=dict(l=10, r=10, t=0, b=0),
            height=450,
            hoverlabel=dict(
                bgcolor="rgba(0,0,0,0.75)",
                font_color="white",
                font=dict(family="Noto Serif Condensed Regular", size=12),
            ),
        )
        if has_z.any():
            layout_kw["coloraxis_colorbar"] = dict(title="Z-score (month PRCP)")
        fig.update_layout(**layout_kw)

        # Highlight selected station on top
        if sid is not None and sid in stations_by_id.index:
            row = stations_by_id.loc[sid]
            fig.add_scattermapbox(
                lat=[row["latitude"]],
                lon=[row["longitude"]],
                mode="markers",
                hoverinfo="skip",
                showlegend=False,
            )

        return fig


app = App(app_ui, server)