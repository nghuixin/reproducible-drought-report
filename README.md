# reproducible-drought-report

A reproducible drought report pipeline and **PyShiny** app. All steps can be run with Python from the project root.

---

## 1. Prerequisites

- **Conda** (Miniconda or Anaconda). Install from [conda.io](https://docs.conda.io/en/latest/miniconda.html) if needed.
- A terminal (PowerShell, Command Prompt, or Git Bash on Windows; Terminal on Mac/Linux).

---

## 2. One-time setup (Conda)

Open a terminal, go to the project folder, then create and activate the Conda environment from `environment.yml`.

```bash
# Go to the project folder (adjust the path to where your project lives)
cd "c:\Users\hxng\OneDrive - UC San Diego\tool-dev\reproducible-drought-report"

# Create the Conda env (includes Python 3.10+ and all dependencies)
conda env create -f environment.yml

# Activate it (same on Windows, Mac, and Linux)
conda activate drought-report
```

After activation, your prompt shows `(drought-report)` (or the env name from `environment.yml`).

**Updating the env later** (e.g. after changing `environment.yml`):

```bash
conda activate drought-report
conda env update -f environment.yml --prune
```

---

## 3. Running the PyShiny app (quickest way to see the app)

You can run the app **without** running any data pipeline first. The app will use a small built-in dummy dataset so it still works.

```bash
# From the project root, with the Conda env activated (conda activate drought-report):
python -m shiny run --reload code/app.py
```

- Your browser should open to something like **http://127.0.0.1:8000** (or the URL printed in the terminal).
- **`--reload`** means the app restarts when you change `code/app.py` (handy while learning).
- To stop: press **Ctrl+C** in the terminal.

That’s all you need to start exploring the PyShiny UI (sidebar filters, plot, table).

---

## 4. Running the data pipeline (optional)

If you want the app and the “synthetic” pipeline to use **files on disk** (e.g. `data/drought_metadata.csv`) instead of the in-memory dummy data, run the pipeline steps in order (with `conda activate drought-report` first).

**Option A – Run each script by hand (good for learning):**

```bash
# 1) Download raw data (writes data/raw_drought_download.csv)
#    Note: download_data.py is set up to fetch from a URL; if that URL is
#    still a placeholder, it will fail. See code/download_data.py to set a real URL,
#    or temporarily use the GHCN “light” pipeline below instead.
python code/download_data.py

# 2) Index, parse, and transform into drought_metadata.csv
python code/index_archive.py
python code/parse_data.py
python code/transform_data.py
```

After this, `data/drought_metadata.csv` exists and the app will load it automatically next time you run it.

**Option B – Use the Makefile (if you have `make` installed, e.g. via Git Bash or WSL):**

```bash
make data      # runs all four steps above (use Conda env: conda activate drought-report first)
make app       # runs the PyShiny app
```

*(`make install` installs via pip into the current Python; with Conda you use `conda env create -f environment.yml` instead.)*

**Option C – GHCN/NOAA pipeline (different data source):**

This uses NOAA GHCN-Daily data and produces `visuals/world_drought.png` and `index.html`. Some steps download large files or use placeholder logic (see comments in the scripts).

```bash
# If you have make:
make ghcnd_pipeline

# Or run the Python scripts in order:
python code/get_inventory.py
python code/get_station_data.py
python code/summarize_dly_files.py
python code/get_regions_years.py
python code/plot_drought_by_region.py
python code/render_index.py
```

(Omitting `get_all_archive.py` and `get_all_filenames.py` avoids the ~3.7 GB download; the rest can still run with placeholder/small data where noted.)

---

## 5. Summary cheat sheet

| Goal                         | Command |
|-----------------------------|--------|
| Create Conda env            | `conda env create -f environment.yml` |
| Activate Conda env           | `conda activate drought-report` |
| Run the PyShiny app         | `python -m shiny run --reload code/app.py` |
| Run synthetic data pipeline | `python code/download_data.py` then `index_archive.py` → `parse_data.py` → `transform_data.py` (or `make data`) |
| Generate static plot       | `python code/make_visualizations.py` (after `make data` or the four steps above) |
| Run full GHCN pipeline      | `make ghcnd_pipeline` (or run the `code/get_*.py`, `summarize_dly_files.py`, etc., in order) |

---

## 6. Project layout (reference)

```
├── .github/workflows/ci.yml   # CI runs pipeline + checks
├── code/
│   ├── app.py                 # PyShiny app (run this to start the app)
│   ├── download_data.py       # Download raw data from URL → data/raw_drought_download.csv
│   ├── index_archive.py       # Index raw data
│   ├── parse_data.py          # Parse into clean table
│   ├── transform_data.py      # Build data/drought_metadata.csv
│   ├── make_visualizations.py # Build visuals/drought_timeseries.png
│   └── get_*.py, summarize_*, plot_*, render_*  # GHCN pipeline scripts
├── data/                      # Downloaded and derived data (often in .gitignore)
├── visuals/                   # Generated figures
├── environment.yml            # Conda env definition (recommended)
├── requirements.txt          # pip fallback / CI
├── Makefile
├── index.html                 # Generated by render_index.py (GHCN pipeline)
└── README.md                  # This file
```

For a first run: **`conda env create -f environment.yml` → `conda activate drought-report` → `python -m shiny run --reload code/app.py`** and open the URL in your browser.
