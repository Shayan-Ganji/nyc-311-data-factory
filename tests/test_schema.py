"""
Offline Unit Tests for Schema and Transformations:
Validates schema contract verification, drift detection, timestamp UTC conversion,
ZIP cleaning, and derived resolution metrics.
"""

from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from data_factory.exceptions import SchemaDriftError
from data_factory.schema import (
    check_schema_drift,
    clean_zip_code,
    parse_nyc_timestamp,
    to_canonical_dataframe,
    transform_record_to_canonical,
)


class TestSchemaOffline(unittest.TestCase):
    """Unit tests for schema contracts and canonical transformations."""

    def test_schema_drift_detection_raises_error(self) -> None:
        """Test missing mandatory column (e.g. unique_key missing) raises SchemaDriftError."""
        invalid_batch = [
            {"created_date": "2023-01-01T00:00:00.000", "agency": "DOT", "complaint_type": "Street", "status": "Open"}
        ]
        with self.assertRaises(SchemaDriftError) as ctx:
            check_schema_drift(invalid_batch)
        self.assertIn("unique_key", str(ctx.exception))

    def test_schema_drift_passes_when_compliant(self) -> None:
        """Test compliant raw batch passes schema contract check without error."""
        valid_batch = [
            {
                "unique_key": "123",
                "created_date": "2023-01-01T00:00:00.000",
                "agency": "DOT",
                "complaint_type": "Street Condition",
                "status": "Closed",
            }
        ]
        # Should not raise
        check_schema_drift(valid_batch)

    def test_timestamp_utc_normalization(self) -> None:
        """Test naive NYC Eastern timestamp converts to UTC accounting for 4 or 5 hour offset."""
        # Summer date (EDT: UTC-4)
        summer_ts = "2023-06-15T12:00:00.000"
        dt_utc = parse_nyc_timestamp(summer_ts)
        self.assertIsNotNone(dt_utc)
        self.assertEqual(dt_utc.utcoffset().total_seconds(), 0)
        self.assertEqual(dt_utc.hour, 16)  # 12 EDT = 16 UTC

        # Winter date (EST: UTC-5)
        winter_ts = "2023-01-15T12:00:00.000"
        dt_winter_utc = parse_nyc_timestamp(winter_ts)
        self.assertIsNotNone(dt_winter_utc)
        self.assertEqual(dt_winter_utc.hour, 17)  # 12 EST = 17 UTC

    def test_clean_zip_code_variations(self) -> None:
        """Test clean_zip_code handles numbers, floats, standard strings, and 9-digit formats."""
        self.assertEqual(clean_zip_code(10001), "10001")
        self.assertEqual(clean_zip_code("10001.0"), "10001")
        self.assertEqual(clean_zip_code("10001-4321"), "10001")
        self.assertIsNone(clean_zip_code("UNKNOWN"))
        self.assertIsNone(clean_zip_code(None))

    def test_derived_resolution_time_calculation(self) -> None:
        """Test resolution_time_hours equals exact difference in hours between closed and created."""
        raw = {
            "unique_key": "100",
            "created_date": "2023-05-01T10:00:00.000",
            "closed_date": "2023-05-02T22:30:00.000",  # 36.5 hours later
            "agency": "DOT",
            "complaint_type": "Traffic Signal Condition",
            "status": "Closed",
        }
        canonical = transform_record_to_canonical(raw)
        self.assertEqual(canonical["resolution_time_hours"], 36.5)

    def test_to_canonical_dataframe_types(self) -> None:
        """Test conversion of records into strongly typed pandas DataFrame."""
        records = [
            transform_record_to_canonical({
                "unique_key": "101",
                "created_date": "2023-05-01T10:00:00.000",
                "agency": "DOT",
                "complaint_type": "Street Condition",
                "status": "Closed",
                "latitude": "40.7128",
                "longitude": "-74.0060",
            })
        ]
        df = to_canonical_dataframe(records)
        self.assertEqual(len(df), 1)
        self.assertIsInstance(df["latitude"].iloc[0], float)
        self.assertIsInstance(df["created_date_utc"].iloc[0], pd.Timestamp)


if __name__ == "__main__":
    unittest.main()
