"""
Offline Unit Tests for QuarantineRouter:
Validates detection of urban data traps, temporal paradoxes, envelope construction,
and quarantine rate budget enforcement.
"""

from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from data_factory.config import PipelineConfig
from data_factory.exceptions import QuarantineThresholdExceededError
from data_factory.quarantine import (
    ERR_CLOSED_BEFORE_CREATED,
    ERR_CLOSED_WITHOUT_CLOSED_DATE,
    ERR_INVALID_COORDINATES,
    ERR_MISSING_MANDATORY_FIELD,
    QuarantineRouter,
)
from data_factory.sinks.file_sinks import JsonlSink


class TestQuarantineOffline(unittest.TestCase):
    """Unit tests for Quarantine routing rules and threshold enforcement."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.quarantine_path = Path(self.temp_dir.name) / "test_quarantine.jsonl"
        self.sink = JsonlSink(self.quarantine_path)
        self.config = PipelineConfig(quarantine_threshold=0.20)  # 20% threshold
        self.router = QuarantineRouter(self.config, quarantine_sink=self.sink)

    def tearDown(self) -> None:
        self.sink.finalize()
        self.temp_dir.cleanup()

    def test_quarantine_inverted_date_trap(self) -> None:
        """Test temporal paradox (closed before created) routes to quarantine with ERR_CLOSED_BEFORE_CREATED."""
        bad_record = {
            "unique_key": "TRAP_01",
            "created_date": "2023-04-10T14:00:00.000",
            "closed_date": "2023-04-10T12:00:00.000",  # Inverted!
            "agency": "DOT",
            "complaint_type": "Street Condition",
            "status": "Closed",
        }
        valid, quarantined = self.router.route_batch([bad_record])

        self.assertEqual(len(valid), 0)
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0]["error_code"], ERR_CLOSED_BEFORE_CREATED)
        self.assertEqual(self.router.error_code_counts[ERR_CLOSED_BEFORE_CREATED], 1)

    def test_quarantine_missing_mandatory_field(self) -> None:
        """Test missing unique_key routes to quarantine with ERR_MISSING_MANDATORY_FIELD."""
        missing_key = {
            "created_date": "2023-04-10T14:00:00.000",
            "agency": "DOT",
            "complaint_type": "Street Condition",
            "status": "Open",
        }
        valid, quarantined = self.router.route_batch([missing_key])

        self.assertEqual(len(valid), 0)
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0]["error_code"], ERR_MISSING_MANDATORY_FIELD)

    def test_quarantine_closed_without_closed_date(self) -> None:
        """Test record marked 'Closed' without closed_date is quarantined."""
        closed_no_date = {
            "unique_key": "TRAP_02",
            "created_date": "2023-04-10T14:00:00.000",
            "closed_date": None,
            "agency": "DOT",
            "complaint_type": "Sidewalk Condition",
            "status": "Closed",
        }
        valid, quarantined = self.router.route_batch([closed_no_date])

        self.assertEqual(len(valid), 0)
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0]["error_code"], ERR_CLOSED_WITHOUT_CLOSED_DATE)

    def test_quarantine_invalid_coordinates(self) -> None:
        """Test coordinates outside NYC metropolitan boundary are quarantined."""
        out_of_bounds = {
            "unique_key": "TRAP_03",
            "created_date": "2023-04-10T14:00:00.000",
            "agency": "DOT",
            "complaint_type": "Traffic Signal Condition",
            "status": "Open",
            "latitude": 30.0000,  # Texas, not NYC!
            "longitude": -95.0000,
        }
        valid, quarantined = self.router.route_batch([out_of_bounds])

        self.assertEqual(len(valid), 0)
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0]["error_code"], ERR_INVALID_COORDINATES)

    def test_quarantine_threshold_exceeded_raises_error(self) -> None:
        """Test exceeding configured tolerance threshold raises QuarantineThresholdExceededError (Exit Code 4)."""
        # Route 1 valid and 4 invalid -> 80% quarantine rate vs 20% threshold
        good = {
            "unique_key": "GOOD_01",
            "created_date": "2023-04-10T14:00:00.000",
            "agency": "DOT",
            "complaint_type": "Street Condition",
            "status": "Open",
        }
        bad = {
            "unique_key": "BAD_01",
            "created_date": "2023-04-10T14:00:00.000",
            "closed_date": "2023-04-09T14:00:00.000",
            "agency": "DOT",
            "complaint_type": "Street Condition",
            "status": "Closed",
        }
        self.router.route_batch([good, bad, bad, bad, bad])

        with self.assertRaises(QuarantineThresholdExceededError) as ctx:
            self.router.verify_quarantine_budget()

        self.assertIn("exceeded tolerance threshold", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
