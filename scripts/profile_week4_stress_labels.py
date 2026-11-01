"""Profile candidate future real-price stress labels without using holdout labels.

The script evaluates rolling historical-quantile definitions on the common
M0-M4 eligible origins. Candidate outcomes are generated only for the proposed
training and validation periods; 2021 onward is deliberately not labeled.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PRICE_FILE = ROOT / "data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv"
ELIGIBILITY_FILE = ROOT / "data/processed/week4_ukraine_m0_m4_eligibility.csv"
OUTPUT = ROOT / "data/processed/week4_stress_label_sensitivity.csv"
SUMMARY = ROOT / "data/processed/week4_stress_label_sensitivity.json"
HISTORY_MONTHS = 36
QUANTILES = (80, 90, 95)
REAL_PRICE_RISE_PCT = (5, 10, 15)
HORIZONS = (1, 3, 6)
TRAIN_START = "2013-01"
TRAIN_END = "2017-12"
VALIDATION_START = "2018-01"
VALIDATION_END = "2020-12"
TEST_START = "2021-01"  # Holdout labels are never calculated or written.


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def month_number(key: str) -> int:
    year, month = map(int, key.split("-"))
    return year * 12 + month - 1


def month_key(number: int) -> str:
    year, month0 = divmod(number, 12)
    return f"{year:04d}-{month0 + 1:02d}"


def percentile(values: list[float], pct: int) -> float:
    """Linear interpolation at the (n-1)*p percentile position."""
    ordered = sorted(values)
    position = (len(ordered) - 1) * pct / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def contiguous_episodes(positive_months: list[str]) -> int:
    if not positive_months:
        return 0
    ordered = sorted(positive_months)
    episodes = 1
    previous = month_number(ordered[0])
    for key in ordered[1:]:
        current = month_number(key)
        if current != previous + 1:
            episodes += 1
        previous = current
    return episodes


def split_for(origin: str, horizon: int) -> str | None:
    end = month_number(origin) + horizon
    if origin >= TRAIN_START and end <= month_number(TRAIN_END):
        return "train"
    if origin >= VALIDATION_START and end <= month_number(VALIDATION_END):
        return "validation"
    # Test-period outcomes are intentionally not inspected for this profile.
    if origin >= TEST_START:
        return None
    return "purged_or_insufficient_history"


def main() -> None:
    price_rows = read_csv(PRICE_FILE)
    prices = {f"{row['year']}-{int(row['month']):02d}": float(row["real_price_dec_2000_lcu_per_tonne"]) for row in price_rows}
    if len(prices) != len(price_rows):
        raise ValueError("Real-price input must have unique calendar months")

    eligibility = {row["month"]: row for row in read_csv(ELIGIBILITY_FILE)}
    common_origins = sorted(
        key for key, row in eligibility.items()
        if row["M4_features_complete"] == "1"
    )
    ordered_price_months = sorted(prices)
    price_index = {key: i for i, key in enumerate(ordered_price_months)}

    output_rows: list[dict[str, str | int]] = []
    summaries: dict[str, dict] = {}
    for horizon in HORIZONS:
        candidate_rules = [
            ("historical_level", q, f"trailing36m_q{q}_futuremax_{horizon}m")
            for q in QUANTILES
        ] + [
            ("rise_from_origin", rise, f"realrise_{rise}pct_futuremax_{horizon}m")
            for rise in REAL_PRICE_RISE_PCT
        ]
        for family, parameter, rule_id in candidate_rules:
            labels_by_split = {"train": [], "validation": []}
            candidates = 0
            positives = 0
            for origin in common_origins:
                split = split_for(origin, horizon)
                if split not in labels_by_split:
                    continue
                idx = price_index[origin]
                if idx + 1 < HISTORY_MONTHS:
                    continue
                history_keys = ordered_price_months[idx - HISTORY_MONTHS + 1:idx + 1]
                if len(history_keys) != HISTORY_MONTHS or any(
                    month_number(b) != month_number(a) + 1
                    for a, b in zip(history_keys, history_keys[1:])
                ):
                    continue
                future_keys = [month_key(month_number(origin) + offset) for offset in range(1, horizon + 1)]
                if any(key not in prices for key in future_keys):
                    continue
                future_max = max(prices[key] for key in future_keys)
                if family == "historical_level":
                    threshold = percentile([prices[key] for key in history_keys], parameter)
                    label = int(future_max > threshold)
                    rule = f"positive if any real farm-gate price in months t+1..t+{horizon} exceeds the {parameter}th percentile of the 36 consecutive monthly real prices ending at origin t"
                else:
                    rise_pct = (future_max / prices[origin] - 1.0) * 100
                    label = int(rise_pct >= parameter)
                    rule = f"positive if maximum real farm-gate price in months t+1..t+{horizon} is at least {parameter}% above real price at origin t"
                candidates += 1
                positives += label
                labels_by_split[split].append((origin, label))
                output_rows.append({
                    "origin_month": origin,
                    "split": split,
                    "common_stage_set": "M0-M4",
                    "label_rule_id": rule_id,
                    "history_months": HISTORY_MONTHS,
                    "history_percentile": parameter if family == "historical_level" else "",
                    "minimum_real_price_rise_pct": parameter if family == "rise_from_origin" else "",
                    "forecast_horizon_months": horizon,
                    "future_stress_label": label,
                })

            split_summary = {}
            for split, records in labels_by_split.items():
                pos = [origin for origin, label in records if label == 1]
                split_summary[split] = {
                    "eligible_origins": len(records),
                    "positive_origins": len(pos),
                    "positive_origin_rate": round(len(pos) / len(records), 4) if records else None,
                    "contiguous_positive_origin_episodes": contiguous_episodes(pos),
                    "first_origin": records[0][0] if records else None,
                    "last_origin": records[-1][0] if records else None,
                }
            summaries[rule_id] = {
                "rule": rule if candidates else (f"{family} candidate; no eligible labeled origins"),
                "rule_family": family,
                "horizon_months": horizon,
                "history_percentile": parameter if family == "historical_level" else None,
                "minimum_real_price_rise_pct": parameter if family == "rise_from_origin" else None,
                "training_and_validation_only": True,
                "train_validation_candidate_origins": candidates,
                "train_validation_positive_origins": positives,
                "splits": split_summary,
            }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fields = ["origin_month", "split", "common_stage_set", "label_rule_id", "history_months", "history_percentile", "minimum_real_price_rise_pct", "forecast_horizon_months", "future_stress_label"]
    with OUTPUT.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(output_rows)

    metadata = {
        "purpose": "Candidate target-label sensitivity profile for Week 4; this is label feasibility, not model performance.",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "output_file": OUTPUT.relative_to(ROOT).as_posix(),
        "output_sha256": sha256(OUTPUT),
        "inputs": {
            PRICE_FILE.relative_to(ROOT).as_posix(): sha256(PRICE_FILE),
            ELIGIBILITY_FILE.relative_to(ROOT).as_posix(): sha256(ELIGIBILITY_FILE),
        },
        "common_origin_rule": "M4 feature-complete origins; M4 requires all M0-M3 core feature groups, so every reported origin is shared across M0-M4.",
        "candidate_grid": {"history_months": HISTORY_MONTHS, "historical_level_quantiles": QUANTILES, "real_price_rise_percent": REAL_PRICE_RISE_PCT, "horizons_months": HORIZONS},
        "working_periods_review_needed": {
            "training": [TRAIN_START, TRAIN_END],
            "validation": [VALIDATION_START, VALIDATION_END],
            "held_out_test_starts": TEST_START,
        },
        "purge_rule": "Exclude an origin if its full future outcome horizon crosses the end of its candidate train or validation period.",
        "threshold_note": "Rolling real-price quantiles use only the 36 observations available through forecast origin t; percentile uses linear interpolation at (n-1)*p. Outcome checks only months after t. A separate model alert threshold is not selected here.",
        "working_target_review_needed": "The project lead selected realrise_10pct_futuremax_3m as the current working target; it may be revised after team review.",
        "holdout_note": "No labels, price outcomes, or counts are calculated for origins on/after 2021-01. The candidate calendar remains review needed.",
        "episode_count_note": "Positive-label episodes are contiguous runs of positive eligible calendar-month origins; missing or purged months break runs.",
        "source_limit": "The future outcome uses the same FAOSTAT farm-gate price series as M0, with the SSSU CPI chain; temporal separation does not make it source-independent.",
        "results": summaries,
    }
    SUMMARY.write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"summary_file": str(SUMMARY), "output_file": str(OUTPUT), "results": summaries}, indent=2))


if __name__ == "__main__":
    main()
