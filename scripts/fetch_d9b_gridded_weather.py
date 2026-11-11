"""D9-B: download NASA POWER daily weather for every grid cell over Ukraine's wheat oblasts.

Run on a machine with internet access (standard library only, no installs):

    python scripts/fetch_d9b_gridded_weather.py

- Reads the 232 cells in configs/d9b_grid_cells.json.
- Saves each cell's raw response to data/raw/nasa_power_grid/cells/<cell_id>.json.
  Already-downloaded cells are skipped, so you can stop (Ctrl+C) and re-run any time.
- When all cells are present, writes one long file
  data/raw/nasa_power_grid/nasa_power_grid_daily_2001_2022.csv
  (cell_id, date, prectotcorr_mm, t2m_max_c, t2m_min_c; about 1.9 million rows)
  plus a manifest with every request URL and SHA-256.

Expect roughly 20-30 seconds per cell, so 1.5-2 hours in total. The raw folder
is ignored by Git; the Spark step (scripts/run_d9b_grid_weather.py) reads the
long CSV.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
USER_AGENT = "FoodShield-DA231o/0.3 (course research project)"


def request_url(cell: dict, nasa: dict) -> str:
    params = {
        "parameters": ",".join(nasa["parameters"]), "community": nasa["community"],
        "longitude": str(cell["longitude"]), "latitude": str(cell["latitude"]),
        "start": nasa["start"].replace("-", ""), "end": nasa["end"].replace("-", ""),
        "format": "JSON", "time-standard": nasa["time_standard"],
    }
    return "https://power.larc.nasa.gov/api/temporal/daily/point?" + urllib.parse.urlencode(params)


def fetch(url: str, attempts: int = 4) -> bytes:
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": USER_AGENT}), timeout=300) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError) as error:
            if attempt == attempts:
                raise
            wait = 15 * attempt
            print(f"    retry {attempt}/{attempts - 1} in {wait}s ({error})", flush=True)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cells", type=Path, default=ROOT / "configs" / "d9b_grid_cells.json")
    parser.add_argument("--out", type=Path, default=ROOT / "data" / "raw" / "nasa_power_grid")
    parser.add_argument("--pause", type=float, default=1.0, help="Seconds to wait between requests (be polite to NASA).")
    args = parser.parse_args()
    config = json.loads(args.cells.read_text(encoding="utf-8"))
    cells, nasa = config["cells"], config["nasa_power"]
    cell_dir = args.out / "cells"
    cell_dir.mkdir(parents=True, exist_ok=True)

    for n, cell in enumerate(cells, 1):
        target = cell_dir / f"{cell['cell_id']}.json"
        if target.exists() and target.stat().st_size > 1000:
            continue
        url = request_url(cell, nasa)
        print(f"[{n}/{len(cells)}] {cell['cell_id']} {cell['oblast']} ({cell['latitude']}, {cell['longitude']})", flush=True)
        body = fetch(url)
        json.loads(body)  # fail early on a broken response
        tmp = target.with_name(target.name + ".tmp")
        tmp.write_bytes(body)
        tmp.replace(target)
        done = sum(1 for c in cells if (cell_dir / f"{c['cell_id']}.json").exists())
        print(f"    saved; {done}/{len(cells)} cells done", flush=True)
        time.sleep(args.pause)

    missing = [c["cell_id"] for c in cells if not (cell_dir / f"{c['cell_id']}.json").exists()]
    if missing:
        print(f"{len(missing)} cells still missing; re-run the script.")
        return 1

    long_csv = args.out / "nasa_power_grid_daily_2001_2022.csv"
    manifest = []
    rows = 0
    with long_csv.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["cell_id", "date", "prectotcorr_mm", "t2m_max_c", "t2m_min_c"])
        for cell in cells:
            path = cell_dir / f"{cell['cell_id']}.json"
            doc = json.loads(path.read_text(encoding="utf-8"))
            p = doc["properties"]["parameter"]
            for day in sorted(p["PRECTOTCORR"]):
                vals = [p[k].get(day) for k in ("PRECTOTCORR", "T2M_MAX", "T2M_MIN")]
                writer.writerow([cell["cell_id"], f"{day[:4]}-{day[4:6]}-{day[6:]}",
                                 *["" if v is None or v == -999 else v for v in vals]])
                rows += 1
            manifest.append({"cell_id": cell["cell_id"], "request_url": request_url(cell, nasa),
                             "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    (args.out / "nasa_power_grid_daily_2001_2022.manifest.json").write_text(json.dumps({
        "built_utc": datetime.now(timezone.utc).isoformat(), "cells": len(cells), "rows": rows,
        "long_csv": long_csv.name, "long_csv_sha256": hashlib.sha256(long_csv.read_bytes()).hexdigest(),
        "source": "NASA POWER Daily API (AG community), see request_url per cell", "cells_detail": manifest,
    }, indent=1) + "\n", encoding="utf-8")
    print(f"Done: {rows:,} daily rows from {len(cells)} cells -> {long_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
