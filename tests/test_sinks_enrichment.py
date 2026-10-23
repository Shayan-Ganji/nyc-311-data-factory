"""
Offline Unit Tests for Storage Sinks and Enrichment:
Validates bitwise determinism across file sinks, SHA-256 integrity,
and population enrichment rate calculations.
"""

from pathlib import Path
import sys
import tempfile
import unittest
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from data_factory.enrichment import PopulationEnricher, map_zip_to_borough
from data_factory.sinks.file_sinks import CsvSink, ParquetSink


class TestSinksAndEnrichmentOffline(unittest.TestCase):
    """Unit tests for deterministic file sinks and population enrichment."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        # Use existing population file in repo
        self.pop_file = Path("data/nyc_modzcta_population.json")
        self.enricher = PopulationEnricher(self.pop_file)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_parquet_sink_bitwise_determinism(self) -> None:
        """Test two separate Parquet writes on identical data yield bitwise identical SHA-256 hashes."""
        df = pd.DataFrame({
            "unique_key": ["200", "100", "300"],
            "complaint_type": ["Street", "Signal", "Pothole"],
            "resolution_time_hours": [12.5, 4.0, 48.2],
        })

        path1 = self.temp_path / "run1.parquet"
        sink1 = ParquetSink(path1, sort_column="unique_key")
        sink1.write_dataframe(df)
        meta1 = sink1.finalize()

        path2 = self.temp_path / "run2.parquet"
        sink2 = ParquetSink(path2, sort_column="unique_key")
        sink2.write_dataframe(df.sample(frac=1.0, random_state=42))  # Shuffle input
        meta2 = sink2.finalize()

        self.assertEqual(meta1["sha256"], meta2["sha256"], "Parquet files must have identical SHA-256 digests!")

    def test_csv_sink_bitwise_determinism(self) -> None:
        """Test two separate CSV writes on identical data yield bitwise identical SHA-256 hashes."""
        df = pd.DataFrame({
            "unique_key": ["B", "A", "C"],
            "value": [2, 1, 3],
        })

        path1 = self.temp_path / "run1.csv"
        sink1 = CsvSink(path1, sort_column="unique_key")
        sink1.write_dataframe(df)
        meta1 = sink1.finalize()

        path2 = self.temp_path / "run2.csv"
        sink2 = CsvSink(path2, sort_column="unique_key")
        sink2.write_dataframe(df.iloc[::-1])  # Reverse input
        meta2 = sink2.finalize()

        self.assertEqual(meta1["sha256"], meta2["sha256"], "CSV files must have identical SHA-256 digests!")

    def test_zip_to_borough_mapping(self) -> None:
        """Test postal code to borough mapping accuracy."""
        self.assertEqual(map_zip_to_borough("10001"), "MANHATTAN")
        self.assertEqual(map_zip_to_borough("11201"), "BROOKLYN")
        self.assertEqual(map_zip_to_borough("10451"), "BRONX")
        self.assertEqual(map_zip_to_borough("10301"), "STATEN ISLAND")
        self.assertEqual(map_zip_to_borough("11354"), "QUEENS")
        self.assertIsNone(map_zip_to_borough("90210"))  # California

    def test_enricher_recovers_unspecified_borough(self) -> None:
        """Test incident_zip recovers 'Unspecified' borough."""
        df = pd.DataFrame({
            "unique_key": ["1", "2"],
            "borough": ["Unspecified", "BROOKLYN"],
            "incident_zip": ["10001", "11201"],
        })
        enriched = self.enricher.enrich_case_level_dataframe(df)
        self.assertEqual(enriched.loc[0, "borough"], "MANHATTAN")
        self.assertEqual(enriched.loc[1, "borough"], "BROOKLYN")

    def test_enricher_builds_wide_summary_table(self) -> None:
        """Test construction of Borough x Complaint Type x Month summary table with rate per 10k."""
        df = pd.DataFrame({
            "unique_key": ["1", "2", "3"],
            "created_date_utc": ["2023-05-01 10:00:00", "2023-05-15 12:00:00", "2023-05-20 14:00:00"],
            "borough": ["MANHATTAN", "MANHATTAN", "BROOKLYN"],
            "complaint_type": ["Street Condition", "Street Condition", "Sidewalk Condition"],
            "status": ["Closed", "Closed", "Pending"],
            "resolution_time_hours": [10.0, 20.0, None],
        })

        summary = self.enricher.build_wide_summary_table(df)

        self.assertEqual(len(summary), 2)
        manhattan_row = summary[summary["borough"] == "MANHATTAN"].iloc[0]
        self.assertEqual(manhattan_row["total_complaints"], 2)
        self.assertEqual(manhattan_row["closed_complaints"], 2)
        self.assertEqual(manhattan_row["median_resolution_hours"], 15.0)
        self.assertGreater(manhattan_row["complaints_per_10k_residents"], 0)


if __name__ == "__main__":
    unittest.main()
