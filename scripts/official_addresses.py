#!/usr/bin/env python3
"""Convert official LU/CH/LI address registers to OpenAddresses CSV.

Run tests with:
    python3 -m unittest discover -s scripts -p 'test_*.py'
"""

import argparse
import csv
import io
import math
import sys
import zipfile
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO


OUTPUT_COLUMNS = ["LON", "LAT", "NUMBER", "STREET", "CITY", "POSTCODE"]


@contextmanager
def open_source(path: Path) -> Iterator[TextIO]:
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            members = [
                item
                for item in archive.infolist()
                if not item.is_dir() and item.filename.lower().endswith(".csv")
            ]
            if len(members) != 1:
                raise ValueError(f"expected one CSV member in {path}, found {len(members)}")
            with archive.open(members[0]) as raw:
                with io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
                    yield stream
    else:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            yield stream


def lv95_to_wgs84(east: float, north: float) -> tuple[float, float]:
    y = (east - 2600000) / 1e6
    x = (north - 1200000) / 1e6
    lon = (
        2.6779094
        + 4.728982 * y
        + 0.791484 * y * x
        + 0.1306 * y * x * x
        - 0.0436 * y**3
    )
    lat = (
        16.9023892
        + 3.238272 * x
        - 0.270978 * y * y
        - 0.002528 * x * x
        - 0.0447 * y * y * x
        - 0.0140 * x**3
    )
    return lon * 100 / 36, lat * 100 / 36


def coordinates(lon_value: str, lat_value: str) -> tuple[float, float] | None:
    try:
        lon = float(lon_value)
        lat = float(lat_value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(lon) or not math.isfinite(lat):
        return None
    if abs(lon) > 180 or abs(lat) > 90:
        return None
    return lon, lat


def convert_row(source: str, row: dict[str, str]) -> tuple[list[str] | None, str | None]:
    if source == "lu":
        number = (row.get("numero") or "").strip()
        street = (row.get("rue") or "").strip()
        if not number:
            return None, "empty_number"
        if not street:
            return None, "empty_street"
        point = coordinates(row.get("lon_wgs84") or "", row.get("lat_wgs84") or "")
        if point is None:
            return None, "invalid_coordinates"
        lon, lat = point
        city = (row.get("localite") or "").strip()
        postcode = (row.get("code_postal") or "").strip()
    else:
        if (row.get("ADR_STATUS") or "").strip() != "real":
            return None, "non_real"
        number = row.get("ADR_NUMBER") or ""
        if not number.strip():
            return None, "empty_number"
        street = row.get("STN_LABEL") or ""
        if not street.strip():
            return None, "empty_street"
        try:
            east = float(row.get("ADR_EASTING") or "")
            north = float(row.get("ADR_NORTHING") or "")
            lon, lat = lv95_to_wgs84(east, north)
        except ValueError:
            return None, "invalid_coordinates"
        if coordinates(str(lon), str(lat)) is None:
            return None, "invalid_coordinates"
        zip_label = " ".join((row.get("ZIP_LABEL") or "").split())
        zip_parts = zip_label.split(maxsplit=1)
        postcode = zip_parts[0] if zip_parts else ""
        city = zip_parts[1] if len(zip_parts) > 1 else ""

    return [
        f"{lon:.7f}",
        f"{lat:.7f}",
        number,
        street,
        city,
        postcode,
    ], None


def convert(source: str, input_path: Path, output_path: Path) -> dict[str, object]:
    if source not in {"lu", "ch", "li"}:
        raise ValueError(f"unsupported source: {source}")
    required_fields = (
        {"rue", "numero", "localite", "code_postal", "lon_wgs84", "lat_wgs84"}
        if source == "lu"
        else {"ADR_STATUS", "ADR_NUMBER", "STN_LABEL", "ZIP_LABEL", "ADR_EASTING", "ADR_NORTHING"}
    )
    read = 0
    written = 0
    skipped = Counter()
    with open_source(input_path) as stream, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as output:
        reader = csv.DictReader(stream, delimiter=";")
        if reader.fieldnames is None:
            raise ValueError(f"missing CSV header in {input_path}")
        missing = required_fields - set(reader.fieldnames)
        if missing:
            raise ValueError(f"missing required columns in {input_path}: {sorted(missing)}")
        writer = csv.writer(output)
        writer.writerow(OUTPUT_COLUMNS)
        for row in reader:
            read += 1
            converted, reason = convert_row(source, row)
            if reason is not None:
                skipped[reason] += 1
                continue
            writer.writerow(converted)
            written += 1
    return {"read": read, "written": written, "skipped_by_reason": skipped}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=("lu", "ch", "li"))
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    try:
        stats = convert(args.source, args.input, args.output)
    except (OSError, ValueError, csv.Error, zipfile.BadZipFile) as error:
        parser.error(str(error))
    skipped = stats["skipped_by_reason"]
    assert isinstance(skipped, Counter)
    skipped_total = sum(skipped.values())
    reasons = ",".join(f"{key}={skipped[key]}" for key in sorted(skipped)) or "none"
    print(
        f"read={stats['read']} written={stats['written']} skipped={skipped_total} "
        f"skipped_by_reason={reasons}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
