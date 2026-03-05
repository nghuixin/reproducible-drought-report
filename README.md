# reproducible-drought-report

A reproducible pipeline and **PyShiny** app.

- **Data pipeline**: Snakemake rules that:
  - Build a Japan PRCP station manifest from the global GHCN-Daily inventory.
  - Sync station metadata and write a Japan-only stations table.
  - (Optionally) sync per-station daily CSVs from NOAA.
  - Prepare small `code/app_data/*.csv` files for the app/Shinylive bundle.
- **App**: A Shiny for Python app in `code/app.py` that:
  - Loads Japan station metadata + coverage.
  - Shows stations on an OpenStreetMap (Plotly) basemap.
  - Displays a daily PRCP time series for the selected station.
- **Automation**: GitHub Actions (CI + Deploy) run the pipeline and export a **Shinylive** build to GitHub Pages.

---

## Rebuild the Shiny app and test it (local)

From the project root, with **Conda** and the project env available:

### 1. Environment (one-time or after changing deps)

```bash
conda activate drought-report
pip install -r requirements.txt
```

### 2. Build app data (manifest + metadata + app_data)

```bash
snakemake app_data -j 1
```

This runs `japan_manifest`, `japan_metadata`, and `app_data` (writes `code/app_data/*.csv` and `*.json`). It does **not** download all `by_station_japan` files (that would be `snakemake -j 1`).

### 3. Run the Shiny app (server mode)

```bash
python -m shiny run --reload code/app.py
```

Open the URL in the terminal (e.g. `http://127.0.0.1:8000`). Use the station dropdown and date picker; the map and time series should update. Stop with **Ctrl+C**.


### 4. Rebuild the Shinylive static site (optional)

To reproduce what GitHub Actions deploys to Pages:

```bash
snakemake app_data -j 1
python -m shinylive export code build
```

Then open `build/index.html` in a browser, or serve the `build/` folder locally (e.g. `python -m http.server 8080 --directory build` and go to `http://localhost:8080`).

### 5. Quick checks

- **App imports:**  
  `python -c "from code.app import app"`
- **App data files exist:**  
  `test -f code/app_data/japan_stations.csv && test -f code/app_data/japan_prcp_inventory.csv` (Unix) or equivalent in PowerShell.

