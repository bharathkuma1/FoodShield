"""Online wheat-stress monitor used by the Week 6 Spark Structured Streaming job.

Spark does the heavy work on each micro-batch (parsing, de-duplication and the
daily -> cell-month partial sums of 1.86 million weather rows). This class keeps
the small running state on the driver:

- open cell-months: running sums, day counts and a day bitmask per grid cell;
- the national wheat-weighted monthly weather series (D9-B weights);
- the monthly real price series and the annual production/trade values.

When a month has complete weather for all 232 cells and its price, the monitor
computes the corrected M0-M4 features exactly as the batch pipeline defines
them (decision D10) and scores the frozen M0 and M4 models (coefficients,
intercepts and validation cutoffs from the one-time test run). It does not
refit anything; the replay adds no test evidence.

The state is plain JSON so a restarted consumer resumes where it stopped;
``last_batch_id`` makes a re-delivered micro-batch a no-op.
"""

from __future__ import annotations

import calendar
import json
import math
import os
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Iterable

from foodshield.evaluation.week4 import month_key, month_number, price_features
from foodshield.streaming.winsafe import write_text_atomic

WEATHER = ("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c")
CLIMATOLOGY_YEARS, CLIMATOLOGY_MIN_YEARS = 10, 5  # same as foodshield.features.week5


def load_frozen_models(report_path: Path, variant: str, annual_lag: int, stages: Iterable[str]) -> dict[str, dict]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    run = next(r for r in report["runs"] if r["variant"] == variant and r["annual_lag"] == annual_lag)
    out = {}
    for stage in stages:
        s = run["stages"][stage]
        out[stage] = {"coefficients": s["coefficients"], "intercept": s["intercept"],
                      "cutoff": s["cutoff_from_validation"],
                      "version": f"final_test {report['generated_utc'][:10]} {variant} lag{annual_lag} {stage}"}
    return out


def load_cell_weights(cells_config: Path) -> dict[str, float]:
    return {c["cell_id"]: float(c["weight"]) for c in json.loads(cells_config.read_text(encoding="utf-8"))["cells"]}


class OnlineMonitor:
    def __init__(self, cell_weights: dict[str, float], models: dict[str, dict], annual_lag: int):
        self.weights = cell_weights
        self.models = models
        self.annual_lag = annual_lag
        self.open: dict[str, dict[str, list]] = {}       # month -> cell -> [sums..., counts..., days, mask, max_ts]
        self.cells_done: dict[str, dict[str, list]] = {}  # month -> cell -> [p, tx, tn]
        self.weather: dict[str, list[float | None]] = {}  # month -> national [p, tx, tn]
        self.input_ts: dict[str, float] = {}               # month -> latest Kafka timestamp among its inputs
        self.prices: dict[str, float] = {}
        self.annual: dict[str, float] = {}                 # "field|year" -> value
        self.scored: list[str] = []
        self.last_batch_id = -1
        self.counters = {"weather_rows": 0, "price_rows": 0, "annual_rows": 0, "day_overlaps": 0}

    # ---- inputs -------------------------------------------------------------------------------
    def add_weather_partials(self, rows: Iterable[tuple]) -> None:
        """rows: (cell_id, month, sum_p, sum_tx, sum_tn, n_p, n_tx, n_tn, n_days, day_mask, max_ts_seconds)."""
        for cell, month, sp, stx, stn, n_p, ntx, ntn, n_days, mask, ts in rows:
            self.counters["weather_rows"] += int(n_days)
            acc = self.open.setdefault(month, {}).setdefault(cell, [0.0, 0.0, 0.0, 0, 0, 0, 0, 0, 0.0])
            if acc[7] & int(mask):  # a day already counted: only possible for a duplicate beyond the watermark
                self.counters["day_overlaps"] += 1
                continue
            for i, v in enumerate((sp, stx, stn)):
                acc[i] += v or 0.0
            for i, v in enumerate((n_p, ntx, ntn)):
                acc[3 + i] += int(v)
            acc[6] += int(n_days)
            acc[7] |= int(mask)
            acc[8] = max(acc[8], float(ts))
            year, mon = map(int, month.split("-"))
            if acc[6] == calendar.monthrange(year, mon)[1]:
                # Week 2 / D9-B monthly rules: precipitation sum, mean of daily max/min temperature.
                p = acc[0] if acc[3] else None
                tx = acc[1] / acc[4] if acc[4] else None
                tn = acc[2] / acc[5] if acc[5] else None
                self.cells_done.setdefault(month, {})[cell] = [p, tx, tn]
                self.input_ts[month] = max(self.input_ts.get(month, 0.0), acc[8])
                del self.open[month][cell]
                if not self.open[month]:
                    del self.open[month]
                if len(self.cells_done[month]) == len(self.weights):
                    self._close_weather_month(month)

    def _close_weather_month(self, month: str) -> None:
        cells = self.cells_done.pop(month)
        national = []
        for i in range(3):
            pairs = [(v[i], self.weights[c]) for c, v in cells.items() if v[i] is not None]
            weight = sum(w for _, w in pairs)
            national.append(sum(v * w for v, w in pairs) / weight if weight > 0 else None)
        self.weather[month] = national

    def add_price(self, month: str, real_price: float, ts: float) -> None:
        self.counters["price_rows"] += 1
        self.prices[month] = real_price
        self.input_ts[month] = max(self.input_ts.get(month, 0.0), ts)

    def add_annual(self, field: str, year: int, value: float) -> None:
        self.counters["annual_rows"] += 1
        self.annual[f"{field}|{year}"] = value

    # ---- features and scoring -----------------------------------------------------------------
    def _clim_z(self, month: str, i: int) -> float | None:
        n = month_number(month)
        value = self.weather[month][i]
        prior = [self.weather[k][i] for k in (month_key(n - 12 * j) for j in range(1, CLIMATOLOGY_YEARS + 1))
                 if k in self.weather and self.weather[k][i] is not None]
        if value is None or len(prior) < CLIMATOLOGY_MIN_YEARS:
            return None
        sd = stdev(prior)
        return (value - mean(prior)) / sd if sd > 0 else None

    def _yoy(self, field: str, year: int) -> float | None:
        a, b = self.annual.get(f"{field}|{year}"), self.annual.get(f"{field}|{year - 1}")
        if a is None or b is None or abs(a) + abs(b) == 0:
            return None
        return (a - b) / ((abs(a) + abs(b)) / 2.0)

    def features(self, month: str) -> dict[str, float | None]:
        mom, z = price_features(self.prices, month)
        out = {"price_real_mom_change": mom, "price_real_anomaly_z_12m": z}
        for i, name in enumerate(WEATHER):
            out[f"{name}_clim_z"] = self._clim_z(month, i)
        feature_year = int(month[:4]) - self.annual_lag  # latest data year available by the month (D6)
        for field in ("production_t", "yield_kg_ha", "area_harvested_ha", "import_quantity_t", "export_quantity_t",
                      "import_value_1000_usd", "export_value_1000_usd"):
            out[f"{field}_yoy_sym_change"] = self._yoy(field, feature_year)
        precip = out["precipitation_sum_mm_clim_z"]
        out["m4_price_precip_clim_interaction"] = z * precip if z is not None and precip is not None else None
        return out

    def ready_months(self) -> list[str]:
        done = set(self.scored)
        return sorted(m for m in self.weather if m in self.prices and m not in done)

    def score_ready(self) -> list[dict[str, Any]]:
        alerts = []
        for month in self.ready_months():
            feats = self.features(month)
            for stage, model in self.models.items():
                names = list(model["coefficients"])
                if any(feats[f] is None for f in names):
                    continue
                contributions = {f: model["coefficients"][f] * feats[f] for f in names}
                eta = model["intercept"] + sum(contributions.values())
                score = 1.0 / (1.0 + math.exp(-eta))
                top = sorted(contributions.items(), key=lambda kv: -abs(kv[1]))[:3]
                alerts.append({
                    "schema_version": 1, "record_id": f"alert:{stage}:{month}", "country_code": "UKR",
                    "commodity_code": "wheat", "observation_month": month, "model_stage": stage,
                    "model_version": model["version"], "score": score, "cutoff": model["cutoff"],
                    "alert": score >= model["cutoff"],
                    "reason_codes": [f"{f}:{'+' if c >= 0 else '-'}{abs(c):.3f}" for f, c in top],
                    "features": {f: feats[f] for f in names},
                    "inputs_complete_kafka_ts": self.input_ts.get(month),
                })
            self.scored.append(month)
        return alerts

    # ---- persistence --------------------------------------------------------------------------
    def to_json(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in ("open", "cells_done", "weather", "input_ts", "prices", "annual",
                                               "scored", "last_batch_id", "counters")}

    def save(self, path: Path) -> None:
        write_text_atomic(path, json.dumps(self.to_json()))

    def load(self, path: Path) -> bool:
        if not path.exists():
            return False
        for k, v in json.loads(path.read_text(encoding="utf-8")).items():
            setattr(self, k, v)
        return True


def replay_offline(records: Iterable[dict[str, Any]], monitor: OnlineMonitor) -> list[dict[str, Any]]:
    """Feed records straight into the monitor, one day of weather at a time (no Kafka, no Spark).

    Used as the reference: streamed alerts must match these month for month.
    """
    alerts: list[dict[str, Any]] = []
    pending: dict[tuple[str, str], list] = {}
    current = None

    def flush() -> None:
        monitor.add_weather_partials(
            (cell, month, *acc) for (cell, month), acc in sorted(pending.items()))
        pending.clear()
        alerts.extend(monitor.score_ready())

    for r in records:
        if r["available_at"] != current:
            flush()
            current = r["available_at"]
        if r["record_type"] == "weather_daily":
            month = r["observation_date"][:7]
            acc = pending.setdefault((r["series_id"], month), [0.0, 0.0, 0.0, 0, 0, 0, 0, 0, 0.0])
            for i, k in enumerate(("prectotcorr_mm", "t2m_max_c", "t2m_min_c")):
                if r[k] is not None:
                    acc[i] += r[k]
                    acc[3 + i] += 1
            acc[6] += 1
            acc[7] |= 1 << int(r["observation_date"][8:10])
        elif r["record_type"] == "price_monthly":
            monitor.add_price(r["observation_date"][:7], r["price_real_lcu_per_tonne"], 0.0)
        else:
            monitor.add_annual(r["annual_field"], int(r["data_year"]), r["value"])
    flush()
    return alerts
