from __future__ import annotations
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
from shiny import App, reactive, render, ui
from shinywidgets import output_widget, render_widget
import json
from datetime import datetime, timezone

 


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

METADATA_PATH = APP_META_PATH if APP_META_PATH.exists() else MANIFEST_DIR / "japan_prcp_manifest.meta.json"
STATIONS_PATH = APP_STATIONS_PATH if APP_STATIONS_PATH.exists() else META_DIR / "japan_stations.csv"
COVERAGE_PATH = APP_COVERAGE_PATH if APP_COVERAGE_PATH.exists() else MANIFEST_DIR / "japan_prcp_inventory.csv"
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


def load_station_prcp(station_id: str):
    path = f"data/by_station_japan/{station_id}.csv.gz"

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

    # Parse date
    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d")

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
    ui.panel_title(f"Precipitation Index in Japan 降水量指数 - Updated 更新 {updated_as_of}"),
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
            ui.input_date(
                "selected_day",
                "Date 日付",
                value=date(2000, 4, 1),
                min=date(1945, 1, 1),
                max=date.today(),
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

def server(input, output, session):
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
        Plotly + OpenStreetMap map of all Japan stations, with the selected
        station highlighted in red.
        """
        sid = selected_station_id()
        fig = px.scatter_mapbox(
            stations_df,
            lat="latitude",
            lon="longitude",
            hover_name="name",
            # Only show station_id in hover; hide lat/lon.
            hover_data={
                "station_id": True,
                "latitude": False,
                "longitude": False,
            },
            zoom=3.5,
            center={"lat": 35.0, "lon": 135.0},
        )
        fig.update_layout(
            mapbox_style="open-street-map",
            margin=dict(l=10, r=10, t=0, b=0),
            height=450,
            hoverlabel=dict(
            bgcolor="rgba(0,0,0,0.75)",  # hover box background
            font_color="white",
            font=dict(family="Noto Serif Condensed Regular", size=12),      # text color
            #font_size=12
        ),
        )

        fig.update_traces(
        customdata=stations_df[["name", "station_id"]],
        hovertemplate="Station: %{customdata[0]}<br>ID: %{customdata[1]}<extra></extra>",
        marker=dict(
        size=7,
        color="darkblue"   # <-- change station dot color here
    )
        )

 

        # Highlight selected station
        if sid is not None and sid in stations_by_id.index:
            row = stations_by_id.loc[sid]
            fig.add_scattermapbox(
                lat=[row["latitude"]],
                lon=[row["longitude"]],
                mode="markers",
                marker=dict(size=12, color="red"),
                hoverinfo="skip",
                showlegend=False,
            )

        # NOTE: Map-click → station selection is NOT wired yet.
        # Doing that cleanly requires hooking Plotly click events (FigureWidget)
        # through shinywidgets and updating input.station_id from a callback.
        return fig

    @output
    @render.plot
    def prcp_timeseries():
        sid = selected_station_id()
        if sid is None:
            import matplotlib.pyplot as plt

            fig, ax = plt.subplots(figsize=(8, 3))
            ax.text(
                0.5,
                0.5,
                "No station selected.",
                ha="center",
                va="center",
            )
            ax.set_axis_off()
            fig.tight_layout()
            return fig

        df = load_station_prcp(sid)

        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(8, 3))
        if df.empty:
            ax.text(
                0.5,
                0.5,
                f"No PRCP data for {sid}.",
                ha="center",
                va="center",
            )
            ax.set_axis_off()
        else:
            ax.plot(df["DATE"], df["PRCP_MM"], linewidth=0.8)
            ax.set_xlabel("Date")
            ax.set_ylabel("Precipitation (mm)")
            ax.set_title(f"Daily precipitation (mm) — {sid}")
        fig.tight_layout()
        return fig


app = App(app_ui, server)