"""Replay records for the Week 6 Kafka demonstration (decision D12).

Three record types, all for Ukraine wheat, each with the UTC time at which the
observation would have been known (``available_at``):

| record_type     | Source                                              | available_at                         |
|-----------------|-----------------------------------------------------|--------------------------------------|
| weather_daily   | NASA POWER daily, one of 232 grid cells (D9-B)       | the next day, 00:00 UTC              |
| price_monthly   | FAOSTAT farm-gate price deflated by SSSU CPI (real) | first day of the next month, 00:00   |
| annual          | FAOSTAT QCL/TCL Ukraine wheat value for data year Y | 1 January of Y + lag (D6), 00:00     |

The weather and price rules are replay assumptions; the annual rule is the
project's D6 availability lag. Records are replayed in ``(available_at,
record_id)`` order, which also puts annual < price < weather at equal times.

Kafka key: the grid cell for weather, and ``UKR:wheat`` for price and annual
records, so every series keeps its order inside one partition and the annual
value for a month always precedes that month's price.

The envelope is flat so Spark can parse it with an explicit schema; absent
values are JSON null.
"""

from __future__ import annotations

import csv
import heapq
import json
from array import array
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterator

SCHEMA_VERSION = 1
FEATURE_SET_VERSION = "week5_corrected_v1"
COUNTRY, COMMODITY = "UKR", "wheat"
NON_WEATHER_KEY = f"{COUNTRY}:{COMMODITY}"

# Same element -> field mapping as foodshield.features.week5.ANNUAL_SIGNALS.
ANNUAL_FIELDS = {
    "QCL": {"Production": "production_t", "Yield": "yield_kg_ha", "Area harvested": "area_harvested_ha"},
    "TCL": {"Import quantity": "import_quantity_t", "Export quantity": "export_quantity_t",
            "Import value": "import_value_1000_usd", "Export value": "export_value_1000_usd"},
}

FIELDS = ["schema_version", "record_id", "record_type", "country_code", "commodity_code", "source", "series_id",
          "observation_date", "available_at", "feature_set_version",
          "prectotcorr_mm", "t2m_max_c", "t2m_min_c", "price_real_lcu_per_tonne", "annual_field", "data_year",
          "value", "unit"]


def _ts(d: date) -> str:
    return f"{d.isoformat()}T00:00:00Z"


def _base(record_id: str, record_type: str, source: str, series_id: str, observation_date: str,
          available: date) -> dict[str, Any]:
    record = dict.fromkeys(FIELDS)
    record.update(schema_version=SCHEMA_VERSION, record_id=record_id, record_type=record_type, country_code=COUNTRY,
                  commodity_code=COMMODITY, source=source, series_id=series_id, observation_date=observation_date,
                  available_at=_ts(available), feature_set_version=FEATURE_SET_VERSION)
    return record


def _num(text: str) -> float | None:
    return None if text in ("", None) else float(text)


class GridWeather:
    """The D9-B daily grid CSV held compactly (232 cells x 8,035 days x 3 floats, about 45 MB)."""

    def __init__(self, path: Path):
        self.cells: list[str] = []
        self.first: date | None = None
        values: dict[str, tuple[array, array, array]] = {}
        nan = float("nan")
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.reader(stream)
            if next(reader) != ["cell_id", "date", "prectotcorr_mm", "t2m_max_c", "t2m_min_c"]:
                raise ValueError(f"Unexpected header in {path}")
            for cell, day, p, tx, tn in reader:
                if cell not in values:
                    values[cell] = (array("d"), array("d"), array("d"))
                    self.cells.append(cell)
                    if self.first is None:
                        self.first = date.fromisoformat(day)
                arrays = values[cell]
                if date.fromisoformat(day) != self.first + timedelta(days=len(arrays[0])):
                    raise ValueError(f"Grid CSV is not a complete daily series for {cell} at {day}")
                for a, v in zip(arrays, (p, tx, tn)):
                    a.append(nan if v == "" else float(v))
        self.cells.sort()
        self.values = values
        self.days = len(next(iter(values.values()))[0])
        if any(len(v[0]) != self.days for v in values.values()):
            raise ValueError("Grid cells have different day counts")

    def records(self, lag_days: int = 1) -> Iterator[tuple[str, str, dict[str, Any]]]:
        for i in range(self.days):
            d = self.first + timedelta(days=i)
            available = d + timedelta(days=lag_days)
            for cell in self.cells:
                p, tx, tn = (a[i] for a in self.values[cell])
                rid = f"wx:{cell}:{d.isoformat()}"
                r = _base(rid, "weather_daily", "NASA POWER daily AG (D9-B grid)", cell, d.isoformat(), available)
                r.update(prectotcorr_mm=None if p != p else p, t2m_max_c=None if tx != tx else tx,
                         t2m_min_c=None if tn != tn else tn)
                yield r["available_at"], rid, r


def price_records(path: Path) -> list[tuple[str, str, dict[str, Any]]]:
    out = []
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            y, m = int(row["year"]), int(row["month"])
            available = date(y + (m == 12), 1 if m == 12 else m + 1, 1)
            month = f"{y:04d}-{m:02d}"
            rid = f"px:{NON_WEATHER_KEY}:{month}"
            r = _base(rid, "price_monthly", "FAOSTAT PP farm-gate price / SSSU CPI (real, Dec 2000 LCU)",
                      NON_WEATHER_KEY, f"{month}-01", available)
            r.update(price_real_lcu_per_tonne=float(row["real_price_dec_2000_lcu_per_tonne"]), unit="LCU/tonne (Dec 2000)")
            out.append((r["available_at"], rid, r))
    return sorted(out)


def annual_records(qcl_csv: Path, tcl_csv: Path, lag_years: int) -> list[tuple[str, str, dict[str, Any]]]:
    out = []
    for domain, path in (("QCL", qcl_csv), ("TCL", tcl_csv)):
        with path.open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                field = ANNUAL_FIELDS[domain].get(row["Element"])
                if field is None or row["Value"] in ("", None):
                    continue
                year = int(row["Year"])
                rid = f"an:{domain}:{field}:{year}"
                r = _base(rid, "annual", f"FAOSTAT {domain} (Ukraine wheat)", NON_WEATHER_KEY, f"{year}-01-01",
                          date(year + lag_years, 1, 1))
                r.update(annual_field=field, data_year=year, value=float(row["Value"]), unit=row["Unit"])
                out.append((r["available_at"], rid, r))
    return sorted(out)


def replay_records(grid: GridWeather, price_csv: Path, qcl_csv: Path, tcl_csv: Path, annual_lag_years: int,
                   until: str | None = None) -> Iterator[dict[str, Any]]:
    """All records in replay order. ``until`` (an available_at prefix such as '2023-01-01') bounds the end."""
    last = _ts(grid.first + timedelta(days=grid.days))  # weather for the last day becomes available here
    limit = f"{until}T00:00:00Z" if until else last
    streams = [grid.records(), iter(price_records(price_csv)), iter(annual_records(qcl_csv, tcl_csv, annual_lag_years))]
    for available, _, record in heapq.merge(*streams, key=lambda t: (t[0], t[1])):
        if available > limit:
            return
        yield record


def kafka_key(record: dict[str, Any]) -> str:
    return record["series_id"] if record["record_type"] == "weather_daily" else NON_WEATHER_KEY


def to_json(record: dict[str, Any]) -> bytes:
    return json.dumps(record, separators=(",", ":"), allow_nan=False).encode("utf-8")
