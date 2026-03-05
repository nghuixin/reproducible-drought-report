 
from pathlib import Path

DATA = Path("data")
CODE = Path("code")
APP_DATA = CODE / "app_data"


rule all:
    input:
        # manifest outputs
        DATA / "ghcnd-inventory.txt",
        DATA / "manifests" / "japan_station_ids_prcp.txt",
        DATA / "manifests" / "japan_prcp_inventory.csv",
        DATA / "manifests" / "japan_prcp_manifest.meta.json",   
        DATA / "monthly" / "japan_monthly_prcp.csv",

        # station metadata outputs
        DATA / "metadata" / "ghcnd-stations.txt",
        DATA / "metadata" / "japan_stations.csv",

        # app data copies for the PyShiny/Shinylive app
        APP_DATA / "japan_stations.csv",
        APP_DATA / "japan_prcp_inventory.csv",
        APP_DATA / "japan_prcp_manifest.meta.json",
        APP_DATA / "japan_monthly_prcp.csv",
        APP_DATA / "japan_latest_prcp.csv",

        # by_station sync marker (stable target)
        DATA / "by_station_japan" / "_sync.done",

        # latest PRCP per station (for app "latest precipitation" feature)
        DATA / "latest" / "japan_latest_prcp.csv",


rule japan_manifest:
    output:
        inventory   = DATA / "ghcnd-inventory.txt",
        station_ids = DATA / "manifests" / "japan_station_ids_prcp.txt",
        coverage    = DATA / "manifests" / "japan_prcp_inventory.csv",
        meta        = DATA / "manifests" / "japan_prcp_manifest.meta.json",
    shell:
        "python {CODE}/build_manifest.py"


rule japan_metadata:
    output:
        stations_txt = DATA / "metadata" / "ghcnd-stations.txt",
        japan_csv    = DATA / "metadata" / "japan_stations.csv",
    shell:
        "python {CODE}/sync_station_metadata.py"


rule app_data:
    input:
        metadata_csv = DATA / "metadata" / "japan_stations.csv",
        coverage_csv = DATA / "manifests" / "japan_prcp_inventory.csv",
        manifest_meta = DATA / "manifests" / "japan_prcp_manifest.meta.json",
        monthly_csv = DATA / "monthly" / "japan_monthly_prcp.csv",
        latest_csv = DATA / "latest" / "japan_latest_prcp.csv",
    output:
        APP_DATA / "japan_stations.csv",
        APP_DATA / "japan_prcp_inventory.csv",
        APP_DATA / "japan_prcp_manifest.meta.json",
        APP_DATA / "japan_monthly_prcp.csv",
        APP_DATA / "japan_latest_prcp.csv",
    shell:
        "python {CODE}/build_app_bundle.py"


rule japan_by_station:
    input:
        station_ids = DATA / "manifests" / "japan_station_ids_prcp.txt",
    output:
        done = DATA / "by_station_japan" / "_sync.done",
    shell:
        r"""
        python {CODE}/fetch_and_sync_data_by_station.py && \
        python -c "from pathlib import Path; p = Path(r'{output.done}'); p.parent.mkdir(parents=True, exist_ok=True); p.write_text('ok\n')"
        """


rule japan_monthly_prcp:
    """Precompute station × year × month PRCP sums so the app avoids 202 file reads per map update."""
    input:
        done = DATA / "by_station_japan" / "_sync.done",
    output:
        DATA / "monthly" / "japan_monthly_prcp.csv",
    shell:
        "python {CODE}/build_monthly_prcp.py"


rule japan_latest_prcp:
    """Latest PRCP date and value per station (from by-station gz); for app 'latest precipitation'."""
    input:
        done = DATA / "by_station_japan" / "_sync.done",
    output:
        DATA / "latest" / "japan_latest_prcp.csv",
    shell:
        "python {CODE}/build_latest_prcp.py"