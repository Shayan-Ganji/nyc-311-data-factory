"""
Main Pipeline Orchestrator for NYC 311 Data Factory.
Manages the end-to-end execution flow:
Ingestion -> Validation & Quarantine -> Normalization -> Population Enrichment -> Storage Sinks -> Manifest Hashing.
"""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from typing import Any
import pandas as pd

from .client import SodaClient
from .config import DEFAULT_CONFIG, PipelineConfig
from .enrichment import PopulationEnricher
from .exceptions import StorageError
from .quarantine import QuarantineRouter
from .schema import check_schema_drift, to_canonical_dataframe
from .sinks.file_sinks import CsvSink, JsonlSink, ParquetSink, compute_file_sha256

logger = logging.getLogger(__name__)


class PipelineEngine:
    """
    Decoupled orchestrator coordinating ingestion, transformation, quarantine, and persistence.
    Guarantees deterministic execution and cryptographic artifact verification.
    """

    def __init__(
        self,
        config: PipelineConfig | None = None,
        client: SodaClient | None = None,
        quarantine_router: QuarantineRouter | None = None,
        enricher: PopulationEnricher | None = None,
    ) -> None:
        self.config = config or DEFAULT_CONFIG
        self.config.ensure_directories()

        self.client = client or SodaClient(self.config)
        self.quarantine_router = quarantine_router or QuarantineRouter(self.config)
        self.enricher = enricher or PopulationEnricher(self.config.reference_population_file)

    def load_checkpoint(self) -> dict[str, Any] | None:
        """Read saved cursor state from checkpoint file if it exists."""
        if not self.config.checkpoint_file.exists():
            return None
        try:
            with open(self.config.checkpoint_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
            return None

    def save_checkpoint(self, last_created: str, last_key: str, count: int) -> None:
        """Atomically persist cursor position for resumption capability."""
        state = {
            "last_created_date": last_created,
            "last_unique_key": last_key,
            "total_ingested_so_far": count,
            "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        temp_file = self.config.checkpoint_file.with_suffix(".tmp")
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        temp_file.replace(self.config.checkpoint_file)

    def run(self, resume: bool = False, max_records: int | None = None) -> dict[str, Any]:
        """
        Execute full ingestion, curation, and export workflow.
        Returns the structured run manifest with cryptographic file hashes.
        """
        start_time = time.time()
        start_iso = datetime.now(timezone.utc).isoformat()
        logger.info(f"Initiating NYC 311 Data Factory run at {start_iso}...")

        resume_created: str | None = None
        resume_key: str | None = None
        if resume:
            ckpt = self.load_checkpoint()
            if ckpt:
                resume_created = ckpt.get("last_created_date")
                resume_key = ckpt.get("last_unique_key")
                logger.info(f"Resuming ingestion from cursor: {resume_created}, key: {resume_key}")

        all_valid_canonical_records: list[dict[str, Any]] = []
        total_raw_ingested = 0

        # Stream batches from SODA API
        batch_generator = self.client.stream_batches(
            resume_created_date=resume_created,
            resume_unique_key=resume_key,
        )

        for batch_idx, raw_batch in enumerate(batch_generator):
            if not raw_batch:
                continue

            # Check schema contract on first batch
            if batch_idx == 0:
                check_schema_drift(raw_batch)

            total_raw_ingested += len(raw_batch)

            # Route through quarantine filter
            valid_canonical, quarantined = self.quarantine_router.route_batch(raw_batch)
            all_valid_canonical_records.extend(valid_canonical)

            # Checkpoint progress
            last_record = raw_batch[-1]
            self.save_checkpoint(
                last_created=str(last_record.get("created_date")),
                last_key=str(last_record.get("unique_key")),
                count=total_raw_ingested,
            )

            logger.info(
                f"Batch {batch_idx + 1} processed: {len(raw_batch)} raw | "
                f"{len(valid_canonical)} valid | {len(quarantined)} quarantined | "
                f"Total valid so far: {len(all_valid_canonical_records)}"
            )

            if max_records and total_raw_ingested >= max_records:
                logger.info(f"Reached requested max_records limit: {max_records}")
                break

        # Finalize Quarantine Sink and check tolerance
        quarantine_stats = self.quarantine_router.finalize()
        self.quarantine_router.verify_quarantine_budget()

        # Build Case-Level DataFrame
        case_df = to_canonical_dataframe(all_valid_canonical_records)

        # Enrich Boroughs using MODZCTA reference table
        case_df = self.enricher.enrich_case_level_dataframe(case_df)

        # Persist Case-Level Dataset (Parquet & CSV)
        case_parquet_sink = ParquetSink(self.config.curated_case_level_parquet)
        case_parquet_sink.write_dataframe(case_df)
        case_parquet_meta = case_parquet_sink.finalize()

        case_csv_sink = CsvSink(self.config.curated_case_level_csv)
        case_csv_sink.write_dataframe(case_df)
        case_csv_meta = case_csv_sink.finalize()

        # Build and Persist Wide Summary Table (Borough x Complaint Type x Month)
        summary_df = self.enricher.build_wide_summary_table(case_df)

        summary_parquet_sink = ParquetSink(self.config.curated_summary_parquet, sort_column="borough")
        summary_parquet_sink.write_dataframe(summary_df)
        summary_parquet_meta = summary_parquet_sink.finalize()

        summary_csv_sink = CsvSink(self.config.curated_summary_csv, sort_column="borough")
        summary_csv_sink.write_dataframe(summary_df)
        summary_csv_meta = summary_csv_sink.finalize()

        # Clean up checkpoint on successful completion
        if self.config.checkpoint_file.exists():
            try:
                self.config.checkpoint_file.unlink()
            except Exception:
                pass

        execution_duration = round(time.time() - start_time, 2)
        end_iso = datetime.now(timezone.utc).isoformat()

        # Assemble Master Execution Manifest
        manifest: dict[str, Any] = {
            "manifest_version": "1.0.0",
            "execution_metadata": {
                "started_at_utc": start_iso,
                "completed_at_utc": end_iso,
                "duration_seconds": execution_duration,
                "target_agency": self.config.target_agency,
                "start_date": self.config.start_date,
                "end_date": self.config.end_date,
            },
            "record_counts": {
                "total_raw_ingested": total_raw_ingested,
                "total_valid_curated": len(case_df),
                "total_quarantined": quarantine_stats["total_quarantined"],
                "quarantine_rate": quarantine_stats["quarantine_rate"],
                "summary_table_rows": len(summary_df),
            },
            "quarantine_breakdown": quarantine_stats["error_breakdown"],
            "artifact_checksums": {
                "case_level_parquet": case_parquet_meta,
                "case_level_csv": case_csv_meta,
                "summary_table_parquet": summary_parquet_meta,
                "summary_table_csv": summary_csv_meta,
                "quarantine_log": quarantine_stats["quarantine_sink_metadata"],
            },
        }

        # Write manifest to disk
        with open(self.config.manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        logger.info(f"Pipeline run completed successfully in {execution_duration}s. Manifest written to {self.config.manifest_file}")
        return manifest
