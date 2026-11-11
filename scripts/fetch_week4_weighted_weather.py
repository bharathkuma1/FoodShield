"""Build wheat-area-weighted monthly NASA POWER weather for the Ukraine pilot.

Replaces the single 49N/32E screening point with one point per wheat-producing
oblast (24 oblasts), weighted by official SSSU wheat harvested area. Monthly
aggregation follows the Week 2 rules exactly: precipitation is the sum of daily
PRECTOTCORR, temperatures are means of daily T2M_MAX/T2M_MIN, and the NASA
-999 fill value is excluded.

Usage (needs internet access to power.larc.nasa.gov and, if the area extract is
missing, stat.gov.ua):

    python scripts/fetch_week4_weighted_weather.py

Raw daily responses are cached under the configured raw directory (ignored by
Git, like other bulk acquisitions). The derived monthly CSV and its metadata
sidecar are the shared outputs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
USER_AGENT = "FoodShield-DA231o/0.2 (course research project)"
SSSU_AREA_URL = (
    "https://stat.gov.ua/sdmx/workspaces/default:integration/registry/sdmx/3.0/data/dataflow/SSSU/"
    "DF_AREA_HARVESTS_CROP_YIELD_M/3.0.1/AREA_HARVEST_AGRICULTURAL_CROP.*.C_0040.ALL_AGRICULTURAL_HOLDINGS.*"
    "?dimensionAtObservation=TIME_PERIOD"
)
NATIONAL = "UA00000000000000000"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def fetch(url: str, accept: str | None = None, timeout: int = 180) -> bytes:
    headers = {"User-Agent": USER_AGENT}
    if accept:
        headers["Accept"] = accept
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=timeout) as response:
        return response.read()


def ensure_area_extract(path: Path, first_year: int = 2015, last_year: int = 2021) -> None:
    """Write the final-season wheat harvested area by oblast if it is missing."""
    if path.exists():
        return
    raw = fetch(SSSU_AREA_URL, "application/vnd.sdmx.data+csv;version=2.0.0;labels=id;timeFormat=normalized;keys=both")
    best: dict[tuple[str, int], tuple[int, str, str]] = {}
    for row in csv.DictReader(raw.decode("utf-8-sig").splitlines()):
        year, month = row["TIME_PERIOD"].split("-M")
        if not first_year <= int(year) <= last_year:
            continue
        key = (row["REGION"], int(year))
        if key not in best or int(month) > best[key][0]:
            best[key] = (int(month), row["OBS_VALUE"], row.get("OBS_FLAG", ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as out:
        out.write("region_katottg,year,final_month,harvested_area_thousand_ha,obs_flag\n")
        for (region, year), (month, value, flag) in sorted(best.items()):
            out.write(f"{region},{year},{month},{value},{flag}\n")


def area_weights(path: Path, oblasts: list[dict], years: list[int]) -> dict[str, float]:
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    means = {}
    for oblast in oblasts:
        values = [float(r["harvested_area_thousand_ha"]) for r in rows
                  if r["region_katottg"] == oblast["katottg"] and int(r["year"]) in years]
        if len(values) != len(years):
            raise ValueError(f"Missing area years for {oblast['name']}: {len(values)} of {len(years)}")
        means[oblast["katottg"]] = sum(values) / len(values)
    national = [float(r["harvested_area_thousand_ha"]) for r in rows
                if r["region_katottg"] == NATIONAL and int(r["year"]) in years]
    total = sum(means.values())
    if national and abs(total - sum(national) / len(national)) > 0.5:
        raise ValueError("Oblast wheat areas do not add up to the national SSSU total")
    # Rounded to 8 decimals so the committed CSV is reproducible bit-for-bit.
    return {key: round(value / total, 8) for key, value in means.items()}


def point_monthly(document: dict) -> dict[str, dict[str, float]]:
    """Daily -> monthly with the Week 2 aggregation rules, excluding -999."""
    parameters = document["properties"]["parameter"]
    acc: dict[str, dict[str, list[float]]] = defaultdict(lambda: {"p": [], "x": [], "n": []})
    for code, key in (("PRECTOTCORR", "p"), ("T2M_MAX", "x"), ("T2M_MIN", "n")):
        for day, value in parameters[code].items():
            if value is None or value == -999:
                continue
            acc[f"{day[:4]}-{day[4:6]}"][key].append(float(value))
    monthly = {}
    for month, values in acc.items():
        monthly[month] = {
            "precipitation_sum_mm": sum(values["p"]) if values["p"] else None,
            "t2m_max_mean_c": sum(values["x"]) / len(values["x"]) if values["x"] else None,
            "t2m_min_mean_c": sum(values["n"]) / len(values["n"]) if values["n"] else None,
            "days": len(values["p"]),
        }
    return monthly


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "week4_weather_oblasts.json")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    nasa = config["nasa_power"]
    oblasts = config["oblasts"]
    area_csv = ROOT / config["area_weights"]["source_csv"]
    ensure_area_extract(area_csv)
    weights = area_weights(area_csv, oblasts, config["area_weights"]["weight_years"])

    raw_dir = ROOT / config["raw_output_dir"]
    raw_dir.mkdir(parents=True, exist_ok=True)
    per_point = {}
    provenance = []
    for oblast in oblasts:
        params = {
            "parameters": ",".join(nasa["parameters"]), "community": nasa["community"],
            "longitude": str(oblast["longitude"]), "latitude": str(oblast["latitude"]),
            "start": nasa["start"].replace("-", ""), "end": nasa["end"].replace("-", ""),
            "format": "JSON", "time-standard": nasa["time_standard"],
        }
        url = "https://power.larc.nasa.gov/api/temporal/daily/point?" + urllib.parse.urlencode(params)
        target = raw_dir / f"nasa_power_{oblast['katottg']}_daily.json"
        if not target.exists():
            target.write_bytes(fetch(url))
        document = json.loads(target.read_text(encoding="utf-8"))
        per_point[oblast["katottg"]] = point_monthly(document)
        provenance.append({
            "katottg": oblast["katottg"], "name": oblast["name"], "weight": weights[oblast["katottg"]],
            "requested_lat_lon": [oblast["latitude"], oblast["longitude"]],
            "nasa_grid_lon_lat_elev": document["geometry"]["coordinates"],
            "request_url": url, "raw_file": target.relative_to(ROOT).as_posix(), "raw_sha256": sha256_file(target),
        })

    months = sorted(set().union(*[set(m) for m in per_point.values()]))
    output = ROOT / config["monthly_output_csv"]
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = ["precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c"]
    with output.open("w", encoding="utf-8", newline="") as out:
        out.write("month,precipitation_sum_mm,t2m_max_mean_c,t2m_min_mean_c,precipitation_days_present,weight_covered,points_present\n")
        for month in months:
            sums = {f: 0.0 for f in fields}
            covered = {f: 0.0 for f in fields}
            days, points = 0.0, 0
            for key, monthly in per_point.items():
                record = monthly.get(month)
                if not record:
                    continue
                points += 1
                days += weights[key] * record["days"]
                for f in fields:
                    if record[f] is not None:
                        sums[f] += weights[key] * record[f]
                        covered[f] += weights[key]
            values = [sums[f] / covered[f] if covered[f] else None for f in fields]
            out.write(",".join([month] + [f"{v:.4f}" if v is not None else "" for v in values]
                               + [f"{days / covered['precipitation_sum_mm']:.2f}" if covered["precipitation_sum_mm"] else "",
                                  f"{covered['precipitation_sum_mm']:.6f}", str(points)]) + "\n")

    sidecar = output.with_name(output.name + ".metadata.json")
    sidecar.write_text(json.dumps({
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "method": "Per oblast point: monthly sum of daily PRECTOTCORR, mean of daily T2M_MAX and T2M_MIN, -999 excluded (Week 2 rules). National value = weighted mean across oblast points with fixed SSSU wheat-area weights; weights renormalised over points with data.",
        "weights": config["area_weights"],
        "spatial_basis": "sssu_wheat_area_weighted_24_oblast_points",
        "output_file": output.relative_to(ROOT).as_posix(),
        "output_sha256": sha256_file(output),
        "area_source_sha256": sha256_file(area_csv),
        "points": provenance,
        "limits": [
            "One NASA POWER grid cell per oblast, not every wheat field; within-oblast variation is not captured.",
            "Weights are fixed 2015-2020 shares; 2010-2014 crop geography is assumed similar.",
            "Pre-2022 SSSU areas exclude temporarily occupied Crimea, Sevastopol and parts of Donetsk/Luhansk.",
        ],
        "citations": [
            "NASA Langley Research Center POWER Project, Daily API (AG community).",
            "State Statistics Service of Ukraine (SSSU), Areas, gross harvests and crop yield (monthly), DF_AREA_HARVESTS_CROP_YIELD_M via SDMX API v3.",
            "geoBoundaries gbOpen UKR ADM1 (Runfola et al. 2020) for oblast centroids.",
        ],
    }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {output.relative_to(ROOT)} ({len(months)} months, {len(per_point)} oblast points)")


if __name__ == "__main__":
    main()
