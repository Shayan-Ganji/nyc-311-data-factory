"""
Unit tests for CsvFileSource and Give Me Some Credit ingestion pipeline.
Verifies memory-budgeted CSV streaming, schema contracts, quarantine rules, and manifest generation.
"""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock
import pandas as pd

from data_factory.credit_pipeline import CreditPipelineEngine
from data_factory.credit_schema import (
    ERR_INVALID_AGE,
    ERR_INVALID_TARGET,
    ERR_NEGATIVE_VALUE,
    validate_and_transform_credit_record,
)
from data_factory.sources.csv_source import CsvFileSource


class TestCreditIngestionOffline(unittest.TestCase):
    """Test suite for CSV ingestion source, credit schema, and pipeline mechanics."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_root = Path(self.temp_dir.name)
        self.sample_csv = self.test_root / "sample_credit.csv"

        # Create a deterministic test CSV with 5 valid rows and 1 invalid row (age=0)
        df = pd.DataFrame(
            [
                {
                    "SeriousDlqin2yrs": 0,
                    "RevolvingUtilizationOfUnsecuredLines": 0.55,
                    "age": 45,
                    "NumberOfTime30-59DaysPastDueNotWorse": 1,
                    "DebtRatio": 0.35,
                    "MonthlyIncome": 6000.0,
                    "NumberOfOpenCreditLinesAndLoans": 8,
                    "NumberOfTimes90DaysLate": 0,
                    "NumberRealEstateLoansOrLines": 1,
                    "NumberOfTime60-89DaysPastDueNotWorse": 0,
                    "NumberOfDependents": 2.0,
                },
                {
                    "SeriousDlqin2yrs": 1,
                    "RevolvingUtilizationOfUnsecuredLines": 1.25,
                    "age": 30,
                    "NumberOfTime30-59DaysPastDueNotWorse": 2,
                    "DebtRatio": 0.85,
                    "MonthlyIncome": 3500.0,
                    "NumberOfOpenCreditLinesAndLoans": 4,
                    "NumberOfTimes90DaysLate": 1,
                    "NumberRealEstateLoansOrLines": 0,
                    "NumberOfTime60-89DaysPastDueNotWorse": 1,
                    "NumberOfDependents": 0.0,
                },
                {
                    "SeriousDlqin2yrs": 0,
                    "RevolvingUtilizationOfUnsecuredLines": 0.10,
                    "age": 62,
                    "NumberOfTime30-59DaysPastDueNotWorse": 0,
                    "DebtRatio": 1250.0,  # Absolute debt when income is null
                    "MonthlyIncome": None,
                    "NumberOfOpenCreditLinesAndLoans": 5,
                    "NumberOfTimes90DaysLate": 0,
                    "NumberRealEstateLoansOrLines": 1,
                    "NumberOfTime60-89DaysPastDueNotWorse": 0,
                    "NumberOfDependents": None,
                },
                {
                    "SeriousDlqin2yrs": 0,
                    "RevolvingUtilizationOfUnsecuredLines": 0.75,
                    "age": 0,  # INVALID: Physical/legal impossibility
                    "NumberOfTime30-59DaysPastDueNotWorse": 0,
                    "DebtRatio": 0.20,
                    "MonthlyIncome": 4000.0,
                    "NumberOfOpenCreditLinesAndLoans": 3,
                    "NumberOfTimes90DaysLate": 0,
                    "NumberRealEstateLoansOrLines": 0,
                    "NumberOfTime60-89DaysPastDueNotWorse": 0,
                    "NumberOfDependents": 1.0,
                },
            ]
        )
        df.to_csv(self.sample_csv, index=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_csv_source_streaming_and_metadata(self) -> None:
        """Test CsvFileSource streams batches correctly and computes SHA-256."""
        source = CsvFileSource(self.sample_csv, chunk_size=2)
        meta = source.get_metadata()

        self.assertEqual(meta["source_type"], "csv_file")
        self.assertEqual(meta["file_name"], "sample_credit.csv")
        self.assertIn("sha256", meta)
        self.assertEqual(len(meta["sha256"]), 64)

        batches = list(source.stream_batches(batch_size=2))
        self.assertEqual(len(batches), 2)
        self.assertEqual(len(batches[0]), 2)
        self.assertEqual(len(batches[1]), 2)

    def test_credit_schema_quarantine_rules(self) -> None:
        """Test validation catches age=0, invalid target, and negative values."""
        # Valid record
        valid_raw = {
            "SeriousDlqin2yrs": 0,
            "RevolvingUtilizationOfUnsecuredLines": 0.2,
            "age": 35,
            "NumberOfTime30-59DaysPastDueNotWorse": 0,
            "DebtRatio": 0.3,
            "MonthlyIncome": 5000,
            "NumberOfOpenCreditLinesAndLoans": 5,
            "NumberOfTimes90DaysLate": 0,
            "NumberRealEstateLoansOrLines": 1,
            "NumberOfTime60-89DaysPastDueNotWorse": 0,
            "NumberOfDependents": 1,
        }
        is_val, err_code, _, canonical = validate_and_transform_credit_record(valid_raw)
        self.assertTrue(is_val)
        self.assertIsNone(err_code)
        self.assertEqual(canonical["age"], 35)
        self.assertEqual(canonical["serious_dlq_in_2yrs"], 0)

        # Age = 0 -> Quarantine
        invalid_age = dict(valid_raw, age=0)
        is_val, err_code, _, _ = validate_and_transform_credit_record(invalid_age)
        self.assertFalse(is_val)
        self.assertEqual(err_code, ERR_INVALID_AGE)

        # Invalid target -> Quarantine
        invalid_target = dict(valid_raw, SeriousDlqin2yrs=5)
        is_val, err_code, _, _ = validate_and_transform_credit_record(invalid_target)
        self.assertFalse(is_val)
        self.assertEqual(err_code, ERR_INVALID_TARGET)

        # Negative count -> Quarantine
        negative_val = dict(valid_raw, NumberOfOpenCreditLinesAndLoans=-2)
        is_val, err_code, _, _ = validate_and_transform_credit_record(negative_val)
        self.assertFalse(is_val)
        self.assertEqual(err_code, ERR_NEGATIVE_VALUE)

    def test_credit_pipeline_execution_and_manifest(self) -> None:
        """Test CreditPipelineEngine runs end-to-end, isolates age=0, and creates manifest."""
        source = CsvFileSource(self.sample_csv, chunk_size=2)
        quarantine_file = self.test_root / "test_quarantine.jsonl"
        manifest_file = self.test_root / "test_manifest.json"

        # Mock sink to avoid external DB dependency in offline unit tests
        mock_sink = MagicMock()
        mock_sink.finalize.return_value = {
            "format": "postgresql",
            "schema": "credit",
            "table": "test_applications",
            "record_count": 3,
        }

        pipeline = CreditPipelineEngine(
            source=source,
            sink=mock_sink,
            quarantine_file=quarantine_file,
            manifest_file=manifest_file,
        )

        manifest = pipeline.run()

        # 4 records total: 3 valid, 1 quarantined (age=0)
        self.assertEqual(manifest["records_summary"]["total_processed"], 4)
        self.assertEqual(manifest["records_summary"]["total_valid"], 3)
        self.assertEqual(manifest["records_summary"]["total_quarantined"], 1)
        self.assertEqual(
            manifest["quarantine"]["error_code_breakdown"],
            {ERR_INVALID_AGE: 1},
        )
        self.assertTrue(manifest_file.exists())
        self.assertTrue(quarantine_file.exists())


if __name__ == "__main__":
    unittest.main()
