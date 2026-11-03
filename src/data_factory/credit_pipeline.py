"""
Pipeline Orchestrator for Give Me Some Credit Ingestion.
Coordinates CsvFileSource streaming, schema contract validation, quarantine routing,
PostgresSink loading, and cryptographic run manifest generation.
"""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from typing import Any
import uuid
import pandas as pd

from .credit_schema import (
    check_credit_schema_drift,
    validate_and_transform_credit_record,
)
from .sinks.file_sinks import JsonlSink
from .sinks.postgres import PostgresSink
from .sources.csv_source import CsvFileSource

logger = logging.getLogger(__name__)


class CreditPipelineEngine:
    """
    Decoupled pipeline engine orchestrating CSV file streaming,
    quarantine isolation, and relational PostgreSQL bulk ingestion.
    """

    def __init__(
        self,
        source: CsvFileSource,
        sink: PostgresSink,
        quarantine_file: Path | str = "data/curated/credit_quarantine.jsonl",
        manifest_file: Path | str = "data/curated/credit_run_manifest.json",
    ) -> None:
        self.source = source
        self.sink = sink
        self.quarantine_file = Path(quarantine_file)
        self.manifest_file = Path(manifest_file)
        self.quarantine_file.parent.mkdir(parents=True, exist_ok=True)
        self.manifest_file.parent.mkdir(parents=True, exist_ok=True)

    def run(self) -> dict[str, Any]:
        """
        Execute streaming ingestion from CSV to PostgreSQL with quarantine filtering.
        Returns the execution manifest.
        """
        start_time = time.time()
        start_iso = datetime.now(timezone.utc).isoformat()
        logger.info(f"Initiating Credit Pipeline run at {start_iso}...")

        total_processed = 0
        total_valid = 0
        total_quarantined = 0
        error_code_counts: dict[str, int] = {}
        quarantine_sink = JsonlSink(self.quarantine_file)

        for batch_idx, raw_batch in enumerate(self.source.stream_batches()):
            if not raw_batch:
                continue

            if batch_idx == 0:
                check_credit_schema_drift(raw_batch)

            valid_batch: list[dict[str, Any]] = []

            for record in raw_batch:
                total_processed += 1
                is_valid, err_code, err_msg, canonical = validate_and_transform_credit_record(record)

                if is_valid and canonical is not None:
                    valid_batch.append(canonical)
                    total_valid += 1
                else:
                    total_quarantined += 1
                    code = err_code or "ERR_UNKNOWN"
                    error_code_counts[code] = error_code_counts.get(code, 0) + 1
                    quarantine_entry = {
                        "quarantined_at_utc": datetime.now(timezone.utc).isoformat(),
                        "error_code": code,
                        "error_description": err_msg,
                        "raw_payload": record,
                    }
                    quarantine_sink.write_batch([quarantine_entry])

            # Write valid batch to PostgreSQL sink
            if valid_batch:
                df_batch = pd.DataFrame(valid_batch)
                self.sink.write_dataframe(df_batch)

            logger.info(
                f"Batch {batch_idx + 1}: Processed {len(raw_batch)} | Valid: {len(valid_batch)} | Quarantined: {total_quarantined}"
            )

        quarantine_meta = quarantine_sink.finalize()
        sink_meta = self.sink.finalize()

        end_time = time.time()
        end_iso = datetime.now(timezone.utc).isoformat()
        duration_sec = round(end_time - start_time, 2)

        manifest = {
            "run_id": str(uuid.uuid4()),
            "pipeline_name": "give_me_some_credit_factory",
            "started_at_utc": start_iso,
            "completed_at_utc": end_iso,
            "duration_seconds": duration_sec,
            "source": self.source.get_metadata(),
            "destination": sink_meta,
            "quarantine": {
                "file_path": str(self.quarantine_file.resolve()),
                "total_quarantined": total_quarantined,
                "error_code_breakdown": error_code_counts,
                "sha256": quarantine_meta.get("sha256"),
            },
            "records_summary": {
                "total_processed": total_processed,
                "total_valid": total_valid,
                "total_quarantined": total_quarantined,
                "quarantine_rate": round(total_quarantined / max(1, total_processed), 6),
            },
        }

        with open(self.manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        logger.info(
            f"Credit Pipeline completed in {duration_sec}s: {total_valid} valid rows loaded to {sink_meta.get('schema')}.{sink_meta.get('table')}."
        )

        return manifest
