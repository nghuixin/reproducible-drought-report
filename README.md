# reproducible-drought-report

A reproducible **Japan PRCP** (precipitation) pipeline and **PyShiny** app: Snakemake builds the data, and the app shows station metadata on an OpenStreetMap map 

---

# A — Manual setup 

Use this when you want to run everything on your machine, experiment with the code, and learn the pipeline step by step.

## A.1 Prerequisites

- **Conda** (Miniconda or Anaconda). Install from [conda.io](https://docs.conda.io/en/latest/miniconda.html) if needed.
- A terminal (PowerShell, Command Prompt, or Git Bash on Windows; Terminal on Mac/Linux).

## A.2 One-time setup

From the project root:

```bash
# Create and activate the Conda environment
conda env create -f environment.yml
conda activate drought-report

# Install remaining dependencies (Snakemake, shinywidgets, etc.)
pip install -r requirements.txt
```

Your prompt should show `(drought-report)`.

**Later, after editing `environment.yml` or `requirements.txt`:**

```bash
conda activate drought-report
conda env update -f environment.yml --prune
pip install -r requirements.txt
```

## A.3 Run the data pipeline (manual)

The pipeline has three stages. You can run them via **Snakemake** (recommended) or by calling the Python scripts yourself (good for learning).

**Option 1 — Snakemake (one command for all outputs):**

```bash
conda activate drought-report
snakemake -j 1
```

This runs, in order:

1. **japan_manifest** — downloads `ghcnd-inventory.txt` if needed, then builds `data/manifests/japan_station_ids_prcp.txt`, `japan_prcp_inventory.csv`, and `japan_prcp_manifest.meta.json`.
2. **japan_metadata** — downloads `ghcnd-stations.txt`, then writes `data/metadata/japan_stations.csv`.
3. **japan_by_station** — downloads each station’s daily CSV from NOAA into `data/by_station_japan/<STATION_ID>.csv.gz` (can be slow; many files).

To run only the first two steps (manifest + metadata, no per-station downloads):

```bash
snakemake japan_manifest japan_metadata -j 1
```

**Option 2 — Instead of using Snakemake, run the Python scripts manually:**

```bash
conda activate drought-report

python code/build_manifest.py
python code/sync_station_metadata.py
python code/fetch_and_sync_data_by_station.py   # optional; downloads many files
```

## A.4 Run the PyShiny app

After the pipeline has produced at least `data/metadata/japan_stations.csv` (from `japan_manifest` + `japan_metadata`), start the app:

```bash
conda activate drought-report
python -m shiny run --reload code/app.py
```

- Open the URL shown in the terminal (e.g. **http://127.0.0.1:8000**).
- Use the sidebar to pick a station; the map (Plotly + OpenStreetMap) and the daily PRCP time series update.
- Stop with **Ctrl+C**.

**Note:** The app needs `data/metadata/japan_stations.csv` at startup. If you haven’t run the pipeline, run at least `snakemake japan_manifest japan_metadata -j 1` first.

## A.5 Project layout (reference)

```
├── .github/workflows/ci.yml   # GitHub Actions CI (see Part B)
├── code/
│   ├── app.py                 # PyShiny app (station map + PRCP time series)
│   ├── build_manifest.py      # Japan PRCP station list from ghcnd-inventory
│   ├── sync_station_metadata.py  # ghcnd-stations.txt → japan_stations.csv
│   └── fetch_and_sync_data_by_station.py  # Per-station daily CSVs
├── data/                      # Pipeline outputs (manifest, metadata, by_station_japan/)
├── Snakefile                  # Snakemake pipeline definition
├── environment.yml            # Conda env (Python, pandas, shiny, plotly)
├── requirements.txt           # pip deps (snakemake, shinywidgets, etc.)
└── README.md
```

---

# B — Automated reproducible pipeline (GitHub Actions)

Use this when you want the pipeline to run automatically on every push or pull request to `main`, with no manual steps.

## B.1 What runs automatically

When you push to `main` or open a pull request targeting `main`, GitHub Actions runs the workflow in `.github/workflows/ci.yml`. It:

1. **Checkout** the repository.
2. **Set up Python 3.11** and install dependencies from `requirements.txt`.
3. **Run the pipeline** with Snakemake: `snakemake japan_manifest japan_metadata -j 1`.  
   Only manifest and metadata are built (no `japan_by_station`), so CI stays fast and avoids downloading many station files.
4. **Smoke test** the app: `python -c "from code.app import app"` to ensure the app module loads after the data is in place.

If any step fails, the workflow fails and you see a red X on the commit or PR.

## B.2 How to enable it

1. **Push the repo to GitHub** (including the `.github/workflows/ci.yml` file).
2. **Default branch:** the workflow is configured for the `main` branch. If your default branch is different (e.g. `master`), either rename it to `main` or edit the `on.push.branches` and `on.pull_request.branches` in `ci.yml` to match.
3. **No secrets required:** the workflow only installs dependencies, runs Snakemake, and imports the app. It does not need API keys or tokens.

After that, every push to `main` and every PR into `main` will trigger the workflow. You can see runs under the **Actions** tab of the repository.

## B.3 Optional: status badge

To show CI status in your README, add this (replace `OWNER` and `REPO` with your GitHub user/org and repo name):

```markdown
![CI](https://github.com/OWNER/REPO/actions/workflows/ci.yml/badge.svg)
```

## B.4 Re-running or debugging

- **Re-run a failed workflow:** Open the run in the Actions tab and click **Re-run all jobs**.
- **Run only on demand (optional):** To add a “Run workflow” button, you can add `workflow_dispatch:` under `on:` in `ci.yml`; then you can trigger the same workflow manually from the Actions tab.

---

# Quick reference

| Goal | Command (manual) |
|------|-------------------|
| Create env | `conda env create -f environment.yml` |
| Activate env | `conda activate drought-report` |
| Install pip deps | `pip install -r requirements.txt` |
| Run full pipeline | `snakemake -j 1` |
| Run manifest + metadata only | `snakemake japan_manifest japan_metadata -j 1` |
| Run app | `python -m shiny run --reload code/app.py` |

**Automated:** Push to `main` (or open a PR) — CI runs `snakemake japan_manifest japan_metadata` and the app smoke test (see Part B).
