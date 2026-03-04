 
from pathlib import Path

DATA = Path("data")
CODE = Path("code")

rule all:
    input:
        # manifest outputs
        DATA / "ghcnd-inventory.txt",
        DATA / "manifests" / "japan_station_ids_prcp.txt",
        DATA / "manifests" / "japan_prcp_inventory.csv",
        DATA / "manifests" / "japan_prcp_manifest.meta.json",

        # station metadata outputs
        DATA / "metadata" / "ghcnd-stations.txt",
        DATA / "metadata" / "japan_stations.csv",

        # by_station sync marker (stable target)
        DATA / "by_station_japan" / "_sync.done",

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