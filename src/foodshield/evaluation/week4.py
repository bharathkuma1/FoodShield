"""Week 4 evaluation helpers shared by the rolling-origin backtest and the synthetic showcase.

Everything here is plain Python over small monthly series (at most a few hundred
rows), so results can be checked by hand. Model fitting and scoring stay in
Spark MLlib in the calling scripts.
"""

from __future__ import annotations

import csv
import math
from pathlib import Path
from statistics import mean, stdev


def month_number(key: str) -> int:
    year, month = map(int, key.split("-")[:2])
    return year * 12 + month - 1


def month_key(number: int) -> str:
    year, month0 = divmod(number, 12)
    return f"{year:04d}-{month0 + 1:02d}"


def read_real_prices(path: Path) -> dict[str, float]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    prices = {f"{r['year']}-{int(r['month']):02d}": float(r["real_price_dec_2000_lcu_per_tonne"]) for r in rows}
    if len(prices) != len(rows):
        raise ValueError("Real-price input must have unique calendar months")
    return prices


def read_eligible(path: Path, column: str = "M4_features_complete") -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return sorted(r["month"] for r in csv.DictReader(stream) if r[column] == "1")


def future_rise_label(prices: dict[str, float], origin: str, horizon: int, rise_pct: float,
                      last_allowed: str | None = None) -> int | None:
    """1 if max real price in t+1..t+h is at least rise_pct above the price at t.

    Returns None when any future month is missing or lies after ``last_allowed``
    (used to keep test-period prices out of pre-test evaluations).
    """
    start = month_number(origin)
    keys = [month_key(start + offset) for offset in range(1, horizon + 1)]
    if last_allowed is not None and month_number(keys[-1]) > month_number(last_allowed):
        return None
    if origin not in prices or any(k not in prices for k in keys):
        return None
    return int(max(prices[k] for k in keys) / prices[origin] - 1.0 >= rise_pct / 100.0)


def first_stress_month(prices: dict[str, float], origin: str, horizon: int, rise_pct: float) -> str | None:
    """First month in t+1..t+h at which the real price is at least rise_pct above the price at t."""
    start = month_number(origin)
    for offset in range(1, horizon + 1):
        key = month_key(start + offset)
        if key in prices and prices[key] / prices[origin] - 1.0 >= rise_pct / 100.0:
            return key
    return None


def price_features(prices: dict[str, float], month: str) -> tuple[float | None, float | None]:
    """M0 features with the Week 3 definitions.

    price_real_mom_change = P_t / P_(t-1) - 1
    price_real_anomaly_z_12m = (P_t - mean(P_(t-12..t-1))) / sample SD(P_(t-12..t-1)),
    null until 12 prior calendar months exist and the SD is positive.
    """
    n = month_number(month)
    prior = month_key(n - 1)
    mom = prices[month] / prices[prior] - 1.0 if month in prices and prior in prices else None
    window = [prices.get(month_key(n - k)) for k in range(1, 13)]
    if month not in prices or any(v is None for v in window):
        return mom, None
    sd = stdev(window)
    z = (prices[month] - mean(window)) / sd if sd > 0 else None
    return mom, z


def confusion(scores: list[float], labels: list[int], threshold: float) -> dict[str, float | int]:
    tp = sum(1 for s, y in zip(scores, labels) if s >= threshold and y == 1)
    fp = sum(1 for s, y in zip(scores, labels) if s >= threshold and y == 0)
    fn = sum(1 for s, y in zip(scores, labels) if s < threshold and y == 1)
    tn = sum(1 for s, y in zip(scores, labels) if s < threshold and y == 0)
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    f1 = (2 * precision * recall / (precision + recall)
          if precision is not None and recall is not None and precision + recall else (0.0 if tp + fn else None))
    fpr = fp / (fp + tn) if fp + tn else None
    return {"threshold": threshold, "n": len(labels), "positives": tp + fn, "alerts": tp + fp,
            "true_positive": tp, "false_positive": fp, "false_negative": fn, "true_negative": tn,
            "precision": precision, "recall": recall, "f1": f1, "false_positive_rate": fpr}


def best_f1_threshold(scores: list[float], labels: list[int]) -> float:
    """Same rule as models/m0.py: maximise F1 over observed scores; ties go to the higher cutoff."""
    candidates = sorted({0.0, 1.0, *scores})
    results = [confusion(scores, labels, c) for c in candidates]
    best = max((r["f1"] or 0.0) for r in results)
    return max(r["threshold"] for r in results if (r["f1"] or 0.0) == best)


def roc_auc(scores: list[float], labels: list[int]) -> float | None:
    """Mann-Whitney AUC with tie correction; None if only one class is present."""
    pos = [s for s, y in zip(scores, labels) if y == 1]
    neg = [s for s, y in zip(scores, labels) if y == 0]
    if not pos or not neg:
        return None
    wins = sum(1.0 if p > n else 0.5 if p == n else 0.0 for p in pos for n in neg)
    return wins / (len(pos) * len(neg))


def episodes(origins: list[str], labels: list[int]) -> list[list[str]]:
    """Contiguous runs of positive eligible origins; a gap or negative month ends a run."""
    runs: list[list[str]] = []
    previous = None
    for origin, label in sorted(zip(origins, labels)):
        if label != 1:
            previous = None
            continue
        if previous is not None and month_number(origin) == month_number(previous) + 1:
            runs[-1].append(origin)
        else:
            runs.append([origin])
        previous = origin
    return runs


def episode_lead_times(prices: dict[str, float], origins: list[str], labels: list[int], alerts: list[int],
                       horizon: int, rise_pct: float) -> list[dict]:
    """For each stress episode: was it caught, and how many months before the first stress month?

    The first stress month is the earliest month at which the real price is at
    least rise_pct above the price at any positive origin in the episode.
    Lead time = first stress month - first alerted origin within the episode
    (positive means the warning came before the stress month).
    """
    alerted = {o for o, a in zip(origins, alerts) if a == 1}
    out = []
    for run in episodes(origins, labels):
        stress = sorted(m for m in (first_stress_month(prices, o, horizon, rise_pct) for o in run) if m)
        first_stress = stress[0] if stress else None
        hits = [o for o in run if o in alerted]
        lead = (month_number(first_stress) - month_number(hits[0])) if hits and first_stress else None
        out.append({"episode_start": run[0], "episode_end": run[-1], "positive_origins": len(run),
                    "first_stress_month": first_stress, "detected": bool(hits),
                    "first_alert": hits[0] if hits else None, "lead_time_months": lead})
    return out


def summarise(values: list[float]) -> dict[str, float | None]:
    values = sorted(v for v in values if v is not None and not math.isnan(v))
    if not values:
        return {"n": 0, "p10": None, "median": None, "p90": None, "mean": None}

    def q(p: float) -> float:
        pos = (len(values) - 1) * p
        lo = int(pos)
        hi = min(lo + 1, len(values) - 1)
        return values[lo] + (values[hi] - values[lo]) * (pos - lo)

    return {"n": len(values), "p10": q(0.1), "median": q(0.5), "p90": q(0.9), "mean": sum(values) / len(values)}
