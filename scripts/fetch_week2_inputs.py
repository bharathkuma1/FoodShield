"""Fetch missing Week 2 source snapshots from their configured public sources.

Existing files are never overwritten. FAOSTAT bulk archives are streamed to a
temporary location and filtered to the configured country/item/element rows.
Only the compact filtered extracts are retained.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import tempfile
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
USER_AGENT = "FoodShield-Research/1.0 (academic reproducibility)"
FPMA_API = "https://fpma.fao.org/giews/v4/global/price_module/api/v1/"
FAOSTAT_QCL_URL = (
    "https://bulks-faostat.fao.org/production/"
    "Production_Crops_Livestock_E_All_Data_(Normalized).zip"
)
FAOSTAT_TCL_URL = (
    "https://bulks-faostat.fao.org/production/"
    "Trade_CropsLivestock_E_All_Data_(Normalized).zip"
)
FAOSTAT_CPI_URL = (
    "https://bulks-faostat.fao.org/production/"
    "ConsumerPriceIndices_E_All_Data_(Normalized).zip"
)
FAOSTAT_PP_URL = (
    "https://bulks-faostat.fao.org/production/"
    "Prices_E_All_Data_(Normalized).zip"
)
UKRSTAT_CPI_URL = (
    "https://stat.gov.ua/sdmx/workspaces/default:integration/registry/sdmx/3.0/"
    "data/dataflow/SSSU/DF_PRICE_CHANGE_CONSUMER_GOODS_SERVICE/27.2.0/"
    "INDEX_CONSUMPRICE.PREV_MONTH.UA00000000000000000.0.M"
    "?dimensionAtObservation=TIME_PERIOD"
)
FAOSTAT_FIELDS = [
    "Area Code",
    "Area Code (M49)",
    "Area",
    "Item Code",
    "Item Code (CPC)",
    "Item",
    "Element Code",
    "Element",
    "Year Code",
    "Year",
    "Unit",
    "Value",
    "Flag",
    "Note",
]
CPI_FIELDS = [
    "Area Code",
    "Area Code (M49)",
    "Area",
    "Item Code",
    "Item",
    "Element Code",
    "Element",
    "Months Code",
    "Months",
    "Year Code",
    "Year",
    "Unit",
    "Value",
    "Flag",
    "Note",
]
CPI_ITEMS = {
    "23012": "Consumer Prices, General Indices (2015 = 100)",
    "23013": "Consumer Prices, Food Indices (2015 = 100)",
}
PP_FIELDS = [
    "Area Code",
    "Area Code (M49)",
    "Area",
    "Item Code",
    "Item Code (CPC)",
    "Item",
    "Element Code",
    "Element",
    "Year Code",
    "Year",
    "Months Code",
    "Months",
    "Unit",
    "Value",
    "Flag",
]
MONTH_NUMBER = {
    month: index
    for index, month in enumerate(
        ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"),
        start=1,
    )
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def request_bytes(url: str, timeout: int = 120) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def download_file(url: str, destination: Path, timeout: int = 300) -> tuple[str, bool]:
    """Download atomically if absent; never overwrite a source snapshot."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if not destination.is_file() or destination.stat().st_size == 0:
            raise ValueError(f"Existing source file is empty or invalid: {destination}")
        return sha256_file(destination), False

    temporary = destination.with_name(destination.name + ".download")
    try:
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        digest = hashlib.sha256()
        with urllib.request.urlopen(request, timeout=timeout) as response, temporary.open("wb") as out:
            while chunk := response.read(1024 * 1024):
                out.write(chunk)
                digest.update(chunk)
        if temporary.stat().st_size == 0:
            raise ValueError(f"Downloaded an empty response from {url}")
        temporary.replace(destination)
        return digest.hexdigest(), True
    finally:
        temporary.unlink(missing_ok=True)


def write_json_atomically(path: Path, document: dict[str, Any]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temp.replace(path)


def resolve_input(root: Path, config_value: str) -> Path:
    path = Path(config_value)
    return path if path.is_absolute() else root / path


def fetch_fpma(config: dict[str, Any], root: Path) -> list[dict[str, Any]]:
    uuid = str(config["fpma_series_uuid"])
    slug = config["inputs"]["fpma_price_json"]
    metadata_path = resolve_input(root, config["inputs"]["fpma_metadata_json"])
    price_path = resolve_input(root, slug)
    encoded_uuid = urllib.parse.quote(uuid, safe="-")
    common = f"uuid__in={encoded_uuid}&format=json"
    endpoints = {
        metadata_path: f"{FPMA_API}FpmaSerieDomestic/?{common}",
        price_path: f"{FPMA_API}FpmaSeriePrice/?{common}",
    }
    records = []
    for destination, url in endpoints.items():
        digest, downloaded = download_file(url, destination)
        records.append(
            {
                "path": destination.relative_to(root).as_posix(),
                "url": url,
                "sha256": digest,
                "downloaded_now": downloaded,
                "reuse_note": "FPMA data may include third-party APK-Inform observations; keep local unless reuse rights are confirmed.",
            }
        )
    return records


def fetch_nasa(config: dict[str, Any], root: Path) -> dict[str, Any]:
    nasa = config["nasa_power"]
    params = {
        "parameters": ",".join(nasa["parameters"]),
        "community": nasa["community"],
        "longitude": str(nasa["longitude"]),
        "latitude": str(nasa["latitude"]),
        "start": nasa["start"].replace("-", ""),
        "end": nasa["end"].replace("-", ""),
        "format": "JSON",
        "time-standard": nasa["time_standard"],
    }
    url = "https://power.larc.nasa.gov/api/temporal/daily/point?" + urllib.parse.urlencode(params)
    destination = resolve_input(root, config["inputs"]["nasa_power_json"])
    digest, downloaded = download_file(url, destination)
    sidecar_path = destination.with_name(destination.stem + ".metadata.json")
    metadata = {
        "provider": "NASA POWER Daily API",
        "request_url": url,
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "output_file": destination.relative_to(root).as_posix(),
        "sha256": digest,
        "latitude": nasa["latitude"],
        "longitude": nasa["longitude"],
        "period": [nasa["start"], nasa["end"]],
        "parameters": nasa["parameters"],
        "time_standard": nasa["time_standard"],
        "spatial_limitation": "Configured point sample; not crop-weighted or nationally representative.",
        "downloaded_now": downloaded,
        "citation": "NASA Langley Research Center POWER Project; follow current POWER citation guidance.",
    }
    write_json_atomically(sidecar_path, metadata)
    return {"path": destination.relative_to(root).as_posix(), "url": url, "sha256": digest, "downloaded_now": downloaded}


def download_bulk_to_temp(url: str, work_dir: Path) -> tuple[Path, str]:
    work_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(prefix="foodshield-faostat-", suffix=".zip", dir=work_dir, delete=False)
    temp_path = Path(handle.name)
    handle.close()
    temp_path.unlink(missing_ok=True)
    try:
        digest, _ = download_file(url, temp_path)
        return temp_path, digest
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def extract_faostat(
    *,
    url: str,
    expected_member: str,
    area_m49: str,
    item_code: str,
    elements: list[str],
    output: Path,
    root: Path,
    domain: str,
) -> dict[str, Any]:
    if output.exists():
        if not output.is_file() or output.stat().st_size == 0:
            raise ValueError(f"Existing FAOSTAT extract is empty or invalid: {output}")
        return {
            "path": output.relative_to(root).as_posix(),
            "sha256": sha256_file(output),
            "downloaded_now": False,
        }

    temporary_dir = output.parent / ".source_download_cache"
    archive_path, archive_sha = download_bulk_to_temp(url, temporary_dir)
    temp_output = output.with_name(output.name + ".tmp")
    count = 0
    try:
        with zipfile.ZipFile(archive_path) as archive:
            if expected_member not in archive.namelist():
                raise ValueError(f"Expected {expected_member} not found in {url}")
            with archive.open(expected_member) as raw, temp_output.open("w", encoding="utf-8", newline="") as out:
                import io

                text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                reader = csv.DictReader(text)
                if reader.fieldnames is None or not set(FAOSTAT_FIELDS).issubset(reader.fieldnames):
                    raise ValueError(f"Unexpected FAOSTAT schema in {expected_member}")
                writer = csv.DictWriter(out, fieldnames=FAOSTAT_FIELDS, lineterminator="\n")
                writer.writeheader()
                for row in reader:
                    if (
                        row.get("Area Code (M49)", "").strip() == str(area_m49)
                        and row.get("Item Code", "").strip() == str(item_code)
                        and row.get("Element", "").strip() in elements
                    ):
                        writer.writerow({field: row.get(field, "") for field in FAOSTAT_FIELDS})
                        count += 1
        if count == 0:
            raise ValueError(f"No {domain} rows matched the configured area/item/elements")
        temp_output.replace(output)
    finally:
        archive_path.unlink(missing_ok=True)
        temp_output.unlink(missing_ok=True)
        try:
            temporary_dir.rmdir()
        except OSError:
            pass

    output_sha = sha256_file(output)
    sidecar = {
        "provider": "FAOSTAT",
        "domain": domain,
        "source_archive_url": url,
        "source_archive_sha256": archive_sha,
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "filter": {"Area Code (M49)": str(area_m49), "Item Code": str(item_code), "Element": elements},
        "output_file": output.relative_to(root).as_posix(),
        "records": count,
        "output_sha256": output_sha,
        "fields_preserved": FAOSTAT_FIELDS,
        "license_note": "FAO statistical database terms specify CC BY 4.0 with attribution; check dataset metadata for any third-party exception before redistributing a refresh.",
        "citation": "FAO. 2026. FAOSTAT. Accessed on retrieval date. Licence: CC-BY-4.0, subject to third-party exceptions.",
    }
    write_json_atomically(output.with_name(output.name + ".metadata.json"), sidecar)
    return {"path": output.relative_to(root).as_posix(), "sha256": output_sha, "records": count, "downloaded_now": True}


def extract_faostat_cpi(*, area_m49: str, output: Path, root: Path) -> dict[str, Any]:
    """Fetch and filter official monthly Ukraine General and Food CPI values."""
    if output.exists():
        if not output.is_file() or output.stat().st_size == 0:
            raise ValueError(f"Existing CPI extract is empty or invalid: {output}")
        return {
            "path": output.relative_to(root).as_posix(),
            "sha256": sha256_file(output),
            "downloaded_now": False,
        }

    expected_member = "ConsumerPriceIndices_E_All_Data_(Normalized).csv"
    temporary_dir = output.parent / ".source_download_cache"
    archive_path, archive_sha = download_bulk_to_temp(FAOSTAT_CPI_URL, temporary_dir)
    temp_output = output.with_name(output.name + ".tmp")
    count = 0
    years: list[int] = []
    month_keys: list[tuple[int, int]] = []
    records_by_item: dict[str, int] = {}
    flags_by_item: dict[str, dict[str, int]] = {}
    seen_keys: set[tuple[str, str, str]] = set()
    try:
        with zipfile.ZipFile(archive_path) as archive:
            if expected_member not in archive.namelist():
                raise ValueError(f"Expected {expected_member} not found in {FAOSTAT_CPI_URL}")
            with archive.open(expected_member) as raw, temp_output.open("w", encoding="utf-8", newline="") as out:
                import io

                text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                reader = csv.DictReader(text)
                if reader.fieldnames is None or not set(CPI_FIELDS).issubset(reader.fieldnames):
                    raise ValueError(f"Unexpected FAOSTAT CPI schema in {expected_member}")
                writer = csv.DictWriter(out, fieldnames=CPI_FIELDS, lineterminator="\n")
                writer.writeheader()
                for row in reader:
                    source_m49 = row.get("Area Code (M49)", "").strip().lstrip("'")
                    item_code = row.get("Item Code", "").strip()
                    if (
                        source_m49 == str(area_m49)
                        and item_code in CPI_ITEMS
                        and row.get("Element", "").strip() == "Value"
                    ):
                        key = (item_code, row.get("Year", "").strip(), row.get("Months Code", "").strip())
                        if key in seen_keys:
                            raise ValueError(f"Duplicate FAOSTAT CPI month for key {key}")
                        seen_keys.add(key)
                        writer.writerow({field: row.get(field, "") for field in CPI_FIELDS})
                        count += 1
                        year = int(row["Year"])
                        years.append(year)
                        month_keys.append((year, MONTH_NUMBER[month]))
                        records_by_item[item_code] = records_by_item.get(item_code, 0) + 1
                        flag = row.get("Flag", "").strip()
                        item_flags = flags_by_item.setdefault(item_code, {})
                        item_flags[flag] = item_flags.get(flag, 0) + 1
        if count == 0:
            raise ValueError(f"No FAOSTAT CPI rows matched M49 {area_m49}")
        temp_output.replace(output)
    finally:
        archive_path.unlink(missing_ok=True)
        temp_output.unlink(missing_ok=True)
        try:
            temporary_dir.rmdir()
        except OSError:
            pass

    output_sha = sha256_file(output)
    ordered_months = sorted(month_keys)
    first_year, first_month = ordered_months[0]
    last_year, last_month = ordered_months[-1]
    all_months = {
        (year, month)
        for year in range(first_year, last_year + 1)
        for month in range(1, 13)
        if (year, month) >= (first_year, first_month)
        and (year, month) <= (last_year, last_month)
    }
    month_set = set(ordered_months)
    gaps = sorted(all_months - month_set)
    complete_years = [
        year for year in years
        if all((year, month) in month_set for month in range(1, 13))
    ]
    sidecar = {
        "provider": "FAOSTAT",
        "domain": "CP",
        "source_archive_url": FAOSTAT_CPI_URL,
        "source_archive_sha256": archive_sha,
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "filter": {
            "Area Code (M49)": str(area_m49),
            "Item Code": sorted(CPI_ITEMS),
            "Element": "Value",
        },
        "output_file": output.relative_to(root).as_posix(),
        "records": count,
        "year_range": [min(years), max(years)],
        "month_range": [f"{first_year:04d}-{first_month:02d}", f"{last_year:04d}-{last_month:02d}"],
        "missing_months_between_first_and_last": [f"{year:04d}-{month:02d}" for year, month in gaps],
        "last_complete_calendar_year": max(complete_years) if complete_years else None,
        "records_by_item": records_by_item,
        "flags_by_item": flags_by_item,
        "output_sha256": output_sha,
        "fields_preserved": CPI_FIELDS,
        "license_note": "FAO statistical database terms specify CC BY 4.0 with attribution unless metadata states otherwise; preserve flags because CP values may come from external organizations or be imputed, and check third-party exceptions before redistribution.",
        "citation": "FAO. 2026. FAOSTAT Consumer Price Indices. Accessed on retrieval date. https://www.fao.org/faostat/en/#data/CP Licence: CC-BY-4.0, subject to third-party exceptions.",
    }
    write_json_atomically(output.with_name(output.name + ".metadata.json"), sidecar)
    return {
        "path": output.relative_to(root).as_posix(),
        "sha256": output_sha,
        "records": count,
        "downloaded_now": True,
    }


def extract_faostat_monthly_wheat_producer_prices(
    *, area_m49: str, item_code: str, output: Path, root: Path
) -> dict[str, Any]:
    """Fetch the official monthly wheat producer-price candidate for one country."""
    if output.exists():
        if not output.is_file() or output.stat().st_size == 0:
            raise ValueError(f"Existing producer-price extract is empty or invalid: {output}")
        return {
            "path": output.relative_to(root).as_posix(),
            "sha256": sha256_file(output),
            "downloaded_now": False,
        }

    expected_member = "Prices_E_All_Data_(Normalized).csv"
    temporary_dir = output.parent / ".source_download_cache"
    archive_path, archive_sha = download_bulk_to_temp(FAOSTAT_PP_URL, temporary_dir)
    temp_output = output.with_name(output.name + ".tmp")
    count = 0
    years: list[int] = []
    flags: dict[str, int] = {}
    units: dict[str, int] = {}
    seen_keys: set[tuple[str, str]] = set()
    try:
        with zipfile.ZipFile(archive_path) as archive:
            if expected_member not in archive.namelist():
                raise ValueError(f"Expected {expected_member} not found in {FAOSTAT_PP_URL}")
            with archive.open(expected_member) as raw, temp_output.open("w", encoding="utf-8", newline="") as out:
                import io

                text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                reader = csv.DictReader(text)
                if reader.fieldnames is None or not set(PP_FIELDS).issubset(reader.fieldnames):
                    raise ValueError(f"Unexpected FAOSTAT PP schema in {expected_member}")
                writer = csv.DictWriter(out, fieldnames=PP_FIELDS, lineterminator="\n")
                writer.writeheader()
                for row in reader:
                    month = row.get("Months", "").strip()
                    source_m49 = row.get("Area Code (M49)", "").strip().lstrip("'")
                    if (
                        source_m49 == str(area_m49)
                        and row.get("Item Code", "").strip() == str(item_code)
                        and row.get("Element Code", "").strip() == "5530"
                        and month in {
                            "January", "February", "March", "April", "May", "June",
                            "July", "August", "September", "October", "November", "December",
                        }
                    ):
                        key = (row.get("Year", "").strip(), row.get("Months Code", "").strip())
                        if key in seen_keys:
                            raise ValueError(f"Duplicate FAOSTAT monthly producer-price key {key}")
                        seen_keys.add(key)
                        writer.writerow({field: row.get(field, "") for field in PP_FIELDS})
                        count += 1
                        years.append(int(row["Year"]))
                        flag = row.get("Flag", "").strip()
                        flags[flag] = flags.get(flag, 0) + 1
                        unit = row.get("Unit", "").strip()
                        units[unit] = units.get(unit, 0) + 1
        if count == 0:
            raise ValueError(f"No FAOSTAT monthly PP rows matched M49 {area_m49}, item {item_code}")
        temp_output.replace(output)
    finally:
        archive_path.unlink(missing_ok=True)
        temp_output.unlink(missing_ok=True)
        try:
            temporary_dir.rmdir()
        except OSError:
            pass

    output_sha = sha256_file(output)
    sidecar = {
        "provider": "FAOSTAT",
        "domain": "PP",
        "source_archive_url": FAOSTAT_PP_URL,
        "source_archive_sha256": archive_sha,
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "filter": {
            "Area Code (M49)": str(area_m49),
            "Item Code": str(item_code),
            "Element Code": "5530",
            "Element": "Producer Price (LCU/tonne)",
            "frequency": "monthly; excludes Annual value rows",
        },
        "output_file": output.relative_to(root).as_posix(),
        "records": count,
        "year_range": [min(years), max(years)],
        "flags": flags,
        "units": units,
        "output_sha256": output_sha,
        "fields_preserved": PP_FIELDS,
        "license": "CC-BY-4.0 per FAOSTAT PP dataset catalog; attribution required. All observations in this Ukraine wheat extract are flagged A (official value).",
        "citation": "FAO. 2026. FAOSTAT Producer Prices. Accessed 1 November 2026. https://www.fao.org/faostat/en/#data/PP Licence: CC-BY-4.0.",
        "scope_note": "Farm-gate producer price candidate; it is not the FPMA wholesale price target and requires a project scope decision before substitution.",
    }
    write_json_atomically(output.with_name(output.name + ".metadata.json"), sidecar)
    return {
        "path": output.relative_to(root).as_posix(),
        "sha256": output_sha,
        "records": count,
        "downloaded_now": True,
    }


def extract_ukrstat_monthly_cpi(*, output: Path, root: Path) -> dict[str, Any]:
    """Fetch the official Ukraine all-items monthly CPI change series via SDMX."""
    if output.exists():
        if not output.is_file() or output.stat().st_size == 0:
            raise ValueError(f"Existing SSSU CPI extract is empty or invalid: {output}")
        return {
            "path": output.relative_to(root).as_posix(),
            "sha256": sha256_file(output),
            "downloaded_now": False,
        }

    request = urllib.request.Request(
        UKRSTAT_CPI_URL,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/vnd.sdmx.data+csv;version=2.0.0;labels=id;timeFormat=normalized;keys=both",
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        source_bytes = response.read()
    source_sha = hashlib.sha256(source_bytes).hexdigest()
    text = source_bytes.decode("utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    if reader.fieldnames is None or not {"TIME_PERIOD", "OBS_VALUE"}.issubset(reader.fieldnames):
        raise ValueError("Unexpected SSSU SDMX CSV schema; expected TIME_PERIOD and OBS_VALUE")
    fields = [
        name for name in (
            "FREQ", "INDICATOR", "BASE_PERIOD", "REGION", "GOODS_SERVICES_TYPE",
            "TIME_PERIOD", "OBS_VALUE", "OBS_FLAG", "OBS_STATUS", "UPDATED", "NOTES_OBS",
        ) if name in reader.fieldnames
    ]
    if not {"TIME_PERIOD", "OBS_VALUE"}.issubset(fields):
        raise ValueError("SSSU CSV did not include required observation fields")

    destination_tmp = output.with_name(output.name + ".tmp")
    count = 0
    periods: list[str] = []
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        with destination_tmp.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
            writer.writeheader()
            seen: set[str] = set()
            for row in reader:
                period = (row.get("TIME_PERIOD") or "").strip()
                value = (row.get("OBS_VALUE") or "").strip()
                if not period or not value:
                    continue
                if period in seen:
                    raise ValueError(f"Duplicate SSSU CPI observation for {period}")
                seen.add(period)
                writer.writerow({name: (row.get(name) or "").strip() for name in fields})
                periods.append(period)
                count += 1
        if not count:
            raise ValueError("SSSU returned no non-empty monthly CPI observations")
        destination_tmp.replace(output)
    finally:
        destination_tmp.unlink(missing_ok=True)

    digest = sha256_file(output)
    ordered = sorted(periods)
    year_months = [tuple(map(int, period.split("-M"))) for period in ordered]
    first_year, first_month = year_months[0]
    last_year, last_month = year_months[-1]
    observed = set(year_months)
    expected = {
        (year, month)
        for year in range(first_year, last_year + 1)
        for month in range(1, 13)
        if (year, month) >= (first_year, first_month)
        and (year, month) <= (last_year, last_month)
    }
    missing_months = sorted(expected - observed)
    complete_years = [
        year for year in range(first_year, last_year + 1)
        if all((year, month) in observed for month in range(1, 13))
    ]
    metadata = {
        "provider": "State Statistics Service of Ukraine (SSSU)",
        "dataset": "Changes in prices (tariffs) for consumer goods (services)",
        "dataflow": "DF_PRICE_CHANGE_CONSUMER_GOODS_SERVICE, version 27.2.0",
        "request_url": UKRSTAT_CPI_URL,
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "series": {
            "indicator": "INDEX_CONSUMPRICE",
            "base_period": "PREV_MONTH",
            "region": "UA00000000000000000 (Ukraine)",
            "goods_services_type": "0 (all-items CPI)",
            "frequency": "M (monthly)",
        },
        "records": count,
        "period_range": [ordered[0], ordered[-1]],
        "missing_months_between_first_and_last": [
            f"{year:04d}-M{month:02d}" for year, month in missing_months
        ],
        "last_complete_calendar_year": max(complete_years) if complete_years else None,
        "source_response_sha256": source_sha,
        "output_file": output.relative_to(root).as_posix(),
        "output_sha256": digest,
        "fields_preserved": fields,
        "reuse_basis": "Official state statistical information; Article 27(1) of Ukraine Law 2524-IX provides access and use/distribution/storage rights, and Article 27(3) requires attribution to the producer when publishing or distributing. Cite SSSU; retain series dimensions and caveats.",
        "citation": "State Statistics Service of Ukraine (SSSU), Changes in prices (tariffs) for consumer goods (services), accessed 1 November 2026 via SSSU SDMX API v3. https://stat.gov.ua/en/development-api/sdmx-api-v3",
        "geographic_caveat": "SSSU metadata notes that coverage excludes temporarily occupied and some conflict-affected territories; see dataset notes before interpreting national representativeness.",
        "measurement_caveat": "OBS_VALUE is the monthly CPI change relative to the previous month, not an already chained CPI index level. Construct any price deflator explicitly and document the anchor month.",
    }
    write_json_atomically(output.with_name(output.name + ".metadata.json"), metadata)
    return {"path": output.relative_to(root).as_posix(), "sha256": digest, "records": count, "downloaded_now": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "week2.local.json")
    args = parser.parse_args()
    config_path = args.config if args.config.is_absolute() else ROOT / args.config
    config = json.loads(config_path.read_text(encoding="utf-8"))
    area = config["faostat_area_m49"]
    item = config["commodity_code"]
    inputs = config["inputs"]
    results: dict[str, Any] = {"snapshot": config.get("snapshot_id", "unversioned"), "sources": []}

    results["sources"].extend(fetch_fpma(config, ROOT))
    results["sources"].append(fetch_nasa(config, ROOT))
    results["sources"].append(
        extract_faostat(
            url=FAOSTAT_QCL_URL,
            expected_member="Production_Crops_Livestock_E_All_Data_(Normalized).csv",
            area_m49=area,
            item_code=item,
            elements=["Production", "Yield", "Area harvested"],
            output=resolve_input(ROOT, inputs["qcl_csv"]),
            root=ROOT,
            domain="QCL",
        )
    )
    results["sources"].append(
        extract_faostat_cpi(
            area_m49=area,
            output=resolve_input(ROOT, inputs["cpi_csv"]),
            root=ROOT,
        )
    )
    results["sources"].append(
        extract_ukrstat_monthly_cpi(
            output=resolve_input(ROOT, inputs["ukrstat_cpi_csv"]),
            root=ROOT,
        )
    )
    results["sources"].append(
        extract_faostat_monthly_wheat_producer_prices(
            area_m49=area,
            item_code=item,
            output=resolve_input(ROOT, inputs["producer_prices_csv"]),
            root=ROOT,
        )
    )
    results["sources"].append(
        extract_faostat(
            url=FAOSTAT_TCL_URL,
            expected_member="Trade_CropsLivestock_E_All_Data_(Normalized).csv",
            area_m49=area,
            item_code=item,
            elements=["Import quantity", "Export quantity", "Import value", "Export value"],
            output=resolve_input(ROOT, inputs["tcl_csv"]),
            root=ROOT,
            domain="TCL",
        )
    )
    manifest = {
        "purpose": "Local source retrieval manifest; captures the exact bytes used by this run.",
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "country_code": config["country_code"],
        "faostat_area_m49": str(area),
        "commodity_code": str(item),
        "sources": results["sources"],
        "warning": "Live APIs and bulk archives may change; compare SHA-256 values before comparing results across retrieval dates.",
    }
    manifest_path = ROOT / "data" / "raw" / "retrieval_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"retrieval_manifest": str(manifest_path), "sources": results["sources"]}, indent=2))


if __name__ == "__main__":
    main()
