import csv
import math
import tempfile
import unittest
import zipfile
from pathlib import Path

from official_addresses import convert


class OfficialAddressConverterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.output = self.root / "converted.csv"

    def tearDown(self):
        self.temp.cleanup()

    def read_output(self):
        with self.output.open(encoding="utf-8", newline="") as stream:
            return list(csv.DictReader(stream))

    def test_luxembourg_semicolon_csv(self):
        source = self.root / "lu.csv"
        source.write_text(
            "\ufeffrue;numero;localite;code_postal;lon_wgs84;lat_wgs84\n"
            "Kaesfurterstrooss;20;Hupperdange;9755;6.062940933927619;50.094893964700596\n"
            "Rue sans coordonnées;2;Luxembourg;1000;;\n",
            encoding="utf-8",
        )

        stats = convert("lu", source, self.output)

        self.assertEqual(stats["read"], 2)
        self.assertEqual(stats["written"], 1)
        self.assertEqual(stats["skipped_by_reason"], {"invalid_coordinates": 1})
        [row] = self.read_output()
        self.assertEqual(
            row,
            {
                "LON": "6.0629409",
                "LAT": "50.0948940",
                "NUMBER": "20",
                "STREET": "Kaesfurterstrooss",
                "CITY": "Hupperdange",
                "POSTCODE": "9755",
            },
        )

    def test_swiss_lv95_coordinates_and_filters(self):
        source = self.root / "ch.csv"
        source.write_text(
            "\ufeffADR_STATUS;ADR_NUMBER;STN_LABEL;ZIP_LABEL;ADR_EASTING;ADR_NORTHING\n"
            "real;31.1;Grellingerstrasse;4052 Basel;2612677.919;1266615.227\n"
            "planned;7;Teststrasse;4052 Basel;2612677.919;1266615.227\n"
            "real;;Teststrasse;4052 Basel;2612677.919;1266615.227\n",
            encoding="utf-8",
        )

        stats = convert("ch", source, self.output)

        self.assertEqual(stats["read"], 3)
        self.assertEqual(stats["written"], 1)
        self.assertEqual(
            stats["skipped_by_reason"], {"non_real": 1, "empty_number": 1}
        )
        [row] = self.read_output()
        self.assertEqual(row["NUMBER"], "31.1")
        self.assertEqual(row["STREET"], "Grellingerstrasse")
        self.assertEqual(row["CITY"], "Basel")
        self.assertEqual(row["POSTCODE"], "4052")
        lon, lat = float(row["LON"]), float(row["LAT"])
        expected_lon, expected_lat = 7.6070521, 47.5501334
        mean_lat = math.radians((lat + expected_lat) / 2)
        distance = math.hypot(
            (lon - expected_lon) * 111_320 * math.cos(mean_lat),
            (lat - expected_lat) * 110_574,
        )
        self.assertLessEqual(distance, 2)

    def test_liechtenstein_reads_csv_member_from_zip(self):
        source = self.root / "li.zip"
        csv_data = (
            "ADR_STATUS;ADR_NUMBER;STN_LABEL;ZIP_LABEL;ADR_EASTING;ADR_NORTHING\n"
            "real;12a;Landstrasse;9490 Vaduz;2612677.919;1266615.227\n"
            "planned;5;Teststrasse;9490 Vaduz;2612677.919;1266615.227\n"
        )
        with zipfile.ZipFile(source, "w") as archive:
            archive.writestr("addresses_li.csv", csv_data.encode("utf-8-sig"))

        stats = convert("li", source, self.output)

        self.assertEqual(stats["read"], 2)
        self.assertEqual(stats["written"], 1)
        self.assertEqual(stats["skipped_by_reason"], {"non_real": 1})
        [row] = self.read_output()
        self.assertEqual(row["NUMBER"], "12a")
        self.assertEqual(row["CITY"], "Vaduz")
        self.assertEqual(row["POSTCODE"], "9490")


if __name__ == "__main__":
    unittest.main()
