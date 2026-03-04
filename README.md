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
- **Automation**: GitHub Actions workflow that runs the pipeline and exports a **Shinylive** build for deployment to GitHub Pages.
 
