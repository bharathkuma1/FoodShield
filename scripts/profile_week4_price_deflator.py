"""Build a bounded real-price feasibility screen from shared official inputs.

The output is a candidate farm-gate series for Week 4 source/target profiling.
It is not the selected FoodShield target and is not an IFPA calculation.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PRICE_FILE = ROOT / "data/raw/faostat_ukraine_monthly_wheat_producer_prices_2026-11.csv"
CPI_FILE = ROOT / "data/raw/ukrstat_ukraine_monthly_all_items_cpi_2026-11.csv"
OUTPUT = ROOT / "data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv"
ANCHOR = (2000, 12)
MONTHS = {
    name: number
    for number, name in enumerate(
        ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"),
        start=1,
    )
}
FIELDS = [
    "year",
    "month",
    "nominal_price_lcu_per_tonne",
    "cpi_change_previous_month_percent_index",
    "cpi_index_dec_2000_100",
    "real_price_dec_2000_lcu_per_tonne",
    "producer_price_flag",
    "cpi_updated",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    with CPI_FILE.open(encoding="utf-8-sig", newline="") as stream:
        cpi_rows = list(csv.DictReader(stream))
    cpi_by_month = {}
    for row in cpi_rows:
        year_text, month_text = row["TIME_PERIOD"].split("-M")
        key = (int(year_text), int(month_text))
        if key in cpi_by_month:
            raise ValueError(f"Duplicate CPI month: {key}")
        cpi_by_month[key] = row

    if ANCHOR not in cpi_by_month:
        raise ValueError("CPI series does not include the selected December 2000 anchor")
    available_cpi = [key for key in sorted(cpi_by_month) if key >= ANCHOR]
    cpi_index = {ANCHOR: 100.0}
    previous = ANCHOR
    for key in available_cpi[1:]:
        year, month = key
        prev_year, prev_month = previous
        expected = (prev_year + (prev_month == 12), 1 if prev_month == 12 else prev_month + 1)
        if key != expected:
            raise ValueError(f"CPI gap between {previous} and {key}; cannot chain safely")
        change = float(cpi_by_month[key]["OBS_VALUE"])
        cpi_index[key] = cpi_index[previous] * change / 100.0
        previous = key

    with PRICE_FILE.open(encoding="utf-8-sig", newline="") as stream:
        price_rows = list(csv.DictReader(stream))
    price_by_month = {}
    for row in price_rows:
        key = (int(row["Year"]), MONTHS[row["Months"]])
        if key in price_by_month:
            raise ValueError(f"Duplicate producer-price month: {key}")
        price_by_month[key] = row

    if any(key not in cpi_index for key in price_by_month):
        raise ValueError("A producer-price observation has no chained CPI value")
    ordered = sorted(price_by_month)
    expected_months = []
    year, month = ordered[0]
    while (year, month) <= ordered[-1]:
        expected_months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    if ordered != expected_months:
        raise ValueError("Producer-price series has missing or duplicate months")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        for key in ordered:
            row = price_by_month[key]
            price = float(row["Value"])
            index = cpi_index[key]
            writer.writerow(
                {
                    "year": key[0],
                    "month": f"{key[1]:02d}",
                    "nominal_price_lcu_per_tonne": f"{price:.6f}",
                    "cpi_change_previous_month_percent_index": cpi_by_month[key]["OBS_VALUE"],
                    "cpi_index_dec_2000_100": f"{index:.10f}",
                    "real_price_dec_2000_lcu_per_tonne": f"{price * 100.0 / index:.8f}",
                    "producer_price_flag": row["Flag"],
                    "cpi_updated": cpi_by_month[key].get("UPDATED", ""),
                }
            )

    sidecar = {
        "purpose": "Week 4 candidate price/deflator feasibility screen; not an approved target, label, or IFPA score.",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "output_file": OUTPUT.relative_to(ROOT).as_posix(),
        "output_sha256": sha256(OUTPUT),
        "records": len(ordered),
        "month_range": [f"{ordered[0][0]:04d}-{ordered[0][1]:02d}", f"{ordered[-1][0]:04d}-{ordered[-1][1]:02d}"],
        "missing_months": [],
        "price_source": PRICE_FILE.relative_to(ROOT).as_posix(),
        "price_source_sha256": sha256(PRICE_FILE),
        "price_scope": "FAOSTAT Ukraine wheat Producer Price (LCU/tonne), element 5530; farm-gate producer price, not FPMA wholesale.",
        "cpi_source": CPI_FILE.relative_to(ROOT).as_posix(),
        "cpi_source_sha256": sha256(CPI_FILE),
        "cpi_series": "SSSU all-items monthly CPI change relative to previous month.",
        "deflator_method": "Set December 2000 CPI index to 100. For each later month multiply the previous index by that month's OBS_VALUE / 100. Real price = nominal LCU/tonne * 100 / chained CPI index.",
        "anchor": "2000-12 = 100",
        "interpretation_limit": "General consumer CPI is a broad inflation deflator, not a wheat-specific affordability or supply-stress label. The constant-price scale depends on the anchor; anomaly comparisons are invariant to a positive scale factor. This screen does not resolve the target price concept.",
        "citation": "FAOSTAT Producer Prices, CC BY 4.0; State Statistics Service of Ukraine (SSSU), all-items CPI. Cite both source producers when using the extract.",
    }
    sidecar_path = OUTPUT.with_suffix(OUTPUT.suffix + ".metadata.json")
    sidecar_path.write_text(json.dumps(sidecar, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUTPUT), "records": len(ordered), "month_range": sidecar["month_range"], "output_sha256": sidecar["output_sha256"]}, indent=2))


if __name__ == "__main__":
    main()
