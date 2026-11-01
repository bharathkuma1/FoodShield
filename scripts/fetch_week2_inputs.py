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
                        years.append(int(row["Year"]))
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
